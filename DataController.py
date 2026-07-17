# Copyright 2020, Charles Powell

import websockets
import json
import logging
import asyncio
import dpath.util
from socket import gaierror
from asyncio_mqtt import Client, MqttError
from contextlib import AsyncExitStack
from typing import Dict

try:
    # Brokerless IPv6-multicast pub/sub. Optional so SenseLink still imports
    # without it when no mpubsub source is configured; the MPubSubController
    # raises a clear error if one is.
    from mpubsub import MpubsubClient
except ImportError:
    MpubsubClient = None

# Independently set WS logger
wslogger = logging.getLogger('websockets')
wslogger.setLevel(logging.WARNING)

MQTT_LOGGER = logging.getLogger('mqtt')
MQTT_LOGGER.setLevel(logging.WARNING)


def safekey(d, keypath, default=None):
    try:
        val = dpath.util.get(d, keypath)
        return val
    except KeyError:
        return default


class HASSController:
    ws = None
    event_rq_id = 1
    bulk_rq_id = 2
    data_sources = []

    def __init__(self, url, auth_token):
        self.url = url
        self.auth_token = auth_token

    def connect(self):
        # Create task
        asyncio.create_task(self.client_handler())

    async def client_handler(self):
        logging.info(f"Starting websocket client to URL: {self.url}")
        try:
            async with websockets.connect(self.url) as websocket:
                self.ws = websocket
                # Wait for incoming message from server
                while True:
                    try:
                        message = await websocket.recv()
                        logging.debug(f"Received message: {message}")
                        await self.on_message(websocket, message)
                    except websockets.exceptions.ConnectionClosed as err:
                        logging.error(f"Lost connection to websocket server ({err})")
                        logging.info(f"Reconnecting in 10...")
                        await asyncio.sleep(10)
                        asyncio.create_task(self.client_handler())
                        return False
        except (websockets.exceptions.WebSocketException, gaierror) as err:
            logging.error(f"Unable to connect to server at {self.url} ({type(err)}:{err})")
            logging.info(f"Attempting to reconnect in 10...")
            await asyncio.sleep(10)
            asyncio.create_task(self.client_handler())

    async def on_message(self, ws, message):
        # Authentication with HASS Websockets
        message = json.loads(message)

        if 'type' in message and message['type'] == 'auth_required':
            logging.info("Authentication requested")
            auth_response = {'type': 'auth', 'access_token': self.auth_token}
            await ws.send(json.dumps(auth_response))

        elif 'type' in message and message['type'] == "auth_invalid":
            logging.error("Authentication failed")

        elif 'type' in message and message['type'] == "auth_ok":
            logging.info("Authentication successful")
            # Authentication successful
            # Send subscription command
            events_command = {
                "id": self.event_rq_id,
                "type": "subscribe_events",
                "event_type": "state_changed"
            }
            await ws.send(json.dumps(events_command))
            logging.info("Event update request sent")

            # Request full status update to get current value
            events_command = {
                "id": self.bulk_rq_id,
                "type": "get_states",
            }
            await ws.send(json.dumps(events_command))
            logging.info("All states request sent")

        elif 'type' in message and message['id'] == self.event_rq_id:
            # Look for state_changed events
            logging.debug("Potential event update received")
            # Check for data
            if not safekey(message, 'event/data'):
                return
            # Notify attached data sources
            for ds in self.data_sources:
                ds.parse_incremental_update(message['event']['data'])

        elif 'type' in message and message['id'] == self.bulk_rq_id:
            # Look for state_changed events
            logging.info("Bulk update received")
            if message.get('result') is None:
                return
            # Extract data
            bulk_update = message.get('result')
            logging.debug(f"Entity update received: {bulk_update}")
            # Loop through statuses
            for status in bulk_update:
                # Notify attached data sources
                for ds in self.data_sources:
                    ds.parse_bulk_update(status)
        else:
            logging.debug(f"Unknown/unhandled message received: {message}")


class MQTTListener:
    def __init__(self, topic, hndls=None):
        self.topic = topic
        self.handlers = []
        self.handlers.extend(hndls)


class MQTTController:
    client = None
    topics: Dict[str, MQTTListener] = None
    tasks = set()

    def __init__(self, host, port=1883, username=None, password=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        self.data_sources = []
        self.topics = {}

    def connect(self):
        # Create task
        asyncio.create_task(self.client_handler())

    async def client_handler(self):
        logging.info(f"Starting MQTT client to URL: {self.host}")
        reconnect_interval = 5  # [seconds]
        while True:
            try:
                await self.listen()
            except MqttError as error:
                logging.error(f'Disconnected from MQTT broker, reconnecting in {reconnect_interval}... ({error}')
            finally:
                await asyncio.sleep(reconnect_interval)

    async def listen(self):
        async with AsyncExitStack() as stack:
            # Track tasks
            stack.push_async_callback(self.cancel_tasks)

            # Connect to the MQTT broker
            client = Client(self.host, self.port, username=self.username, password=self.password)
            await stack.enter_async_context(client)

            logging.info(f'MQTT client connected')
            # Add tasks for each data source handler
            for ds in self.data_sources:
                # Get handlers from data source
                ds_listeners = ds.listeners()
                # Iterate through data source listeners and convert to
                # 'prime' listeners for each topic
                for listener in ds_listeners:
                    topic = listener.topic
                    funcs = listener.handlers
                    if topic in self.topics:
                        # Add these handlers to existing top level topic handler
                        logging.debug(f'Adding handlers for existing prime Listener: {topic}')
                        ext_topic = self.topics[topic]
                        ext_topic.handlers.extend(funcs)
                    else:
                        # Add this instance as a new top level handler
                        logging.debug(f'Creating new prime Listener for topic: {topic}')
                        self.topics[topic] = MQTTListener(topic, funcs)

            # Add handlers for each topic as a filtered topic
            for topic, listener in self.topics.items():
                manager = client.filtered_messages(topic)
                messages = await stack.enter_async_context(manager)
                task = asyncio.create_task(self.parse_messages(messages))
                self.tasks.add(task)

            # Subscribe to all topics
            # Assume QoS 0 for now
            all_topics = [(t, 0) for t in self.topics.keys()]
            logging.info(f'Subscribing to MQTT {len(all_topics)} topic(s)')
            logging.debug(f'Topics: {all_topics}')
            try:
                await client.subscribe(all_topics)
            except ValueError as err:
                logging.error(f'MQTT Subscribe error: {err}')

            # Gather all tasks
            await asyncio.gather(*self.tasks)
            logging.info(f'Listening for MQTT updates')

    async def parse_messages(self, messages):
        async for message in messages:
            topic = message.topic
            # Get handlers and iterate through
            listener = self.topics[topic]
            for func in listener.handlers:
                # Decode to UTF-8
                await func(message.payload.decode())

    async def cancel_tasks(self):
        for task in self.tasks:
            if task.done():
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


class MPubSubController:
    """Feeds data sources from mpubsub topics.

    The mpubsub analogue of MQTTController: it gathers each data source's
    topic listeners, joins the corresponding IPv6 multicast groups via the
    `mpubsub` library, and hands each received payload to the matching
    handlers -- the very same handlers the MQTT source uses, since a payload
    is just a decoded string either way.

    mpubsub is brokerless (peers on the segment hear each other directly), so
    there is no connection to lose and no reconnect loop.
    """

    def __init__(self, port=18512, scope="link-local", interface=None,
                 key=None, replay_window=0):
        if MpubsubClient is None:
            raise ImportError(
                "The 'mpubsub' package is required for mpubsub sources. Install it with:\n"
                "  pip install 'mpubsub @ git+https://github.com/pgenera/esphome-mpubsub.git#subdirectory=python'")
        self.port = port
        self.scope = scope
        self.interface = interface
        self.key = key
        self.replay_window = replay_window

        self.data_sources = []
        self.client = None
        # topic -> MQTTListener (a generic topic+handlers pair, reused here)
        self.topics = {}

    def connect(self):
        # Create task
        asyncio.create_task(self.client_handler())

    async def client_handler(self):
        logging.info(f"Starting mpubsub client on port {self.port} (scope: {self.scope})")
        self.client = MpubsubClient(
            port=self.port, scope=self.scope, interface=self.interface,
            key=self.key, replay_window=self.replay_window)
        try:
            await self.client.start()
        except OSError as err:
            logging.error(f"Unable to start mpubsub client: {err}")
            return

        # Gather listeners from each data source, aggregating handlers per topic
        for ds in self.data_sources:
            for listener in ds.listeners():
                if listener.topic in self.topics:
                    logging.debug(f"Adding handlers for existing mpubsub topic: {listener.topic}")
                    self.topics[listener.topic].handlers.extend(listener.handlers)
                else:
                    logging.debug(f"New mpubsub listener for topic: {listener.topic}")
                    self.topics[listener.topic] = MQTTListener(listener.topic, listener.handlers)

        # Subscribe to each topic. mpubsub topics are exact strings (no
        # wildcards): the wire carries only a CRC32, so there is nothing to
        # match a pattern against.
        for topic, listener in self.topics.items():
            try:
                await self.client.subscribe(topic, self._dispatcher(listener))
            except ValueError as err:
                logging.error(f"mpubsub subscribe error for '{topic}': {err}")
        logging.info(f"Subscribed to {len(self.topics)} mpubsub topic(s)")

    @staticmethod
    def _dispatcher(listener):
        # One callback per topic that fans out to all its handlers. The
        # payload arrives already decoded to a string (mpubsub's default
        # utf-8), exactly what the MQTT handlers expect.
        async def dispatch(message):
            for func in listener.handlers:
                await func(message.payload)
        return dispatch


if __name__ == "__main__":
    pass
