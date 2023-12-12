#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import json
from collections import defaultdict
from logging import getLogger
from typing import Any, Iterable, Mapping

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type
from airbyte_protocol.models import DestinationSyncMode

from opensearchpy import OpenSearch

logger = getLogger('airbyte')


class DestinationOpensearch(Destination):
    def __init__(self, *args, **kwargs):
        self.buffer: None | defaultdict  = None
        self.client: None | OpenSearch = None

        super(DestinationOpensearch, self).__init__(*args, **kwargs)

    def _flush_buffer(self):
        for stream_name, records in self.buffer.items():
            formatted_data = "\n".join(["""{ "index": {} }\n """ + record for record in self.buffer[stream_name]]) + '\n\n'
            self.client.bulk(
                body=formatted_data,
                index=f"airbyte_raw_{stream_name.lower()}",
                headers={"Accept-Encoding": "identity"}
            )
            logger.info(f"Wrote {len(self.buffer[stream_name])} records for {stream_name=}")

        self.buffer = defaultdict(list)

    def write(
            self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:

        """
        Reads the input stream of messages, config, and catalog to write data to the destination.

        This method returns an iterable (typically a generator of AirbyteMessages via yield) containing state messages received
        in the input message stream. Outputting a state message means that every AirbyteRecordMessage which came before it has been
        successfully persisted to the destination. This is used to ensure fault tolerance in the case that a sync fails before fully completing,
        then the source is given the last state message output from this method as the starting point of the next sync.

        :param config: dict of JSON configuration matching the configuration declared in spec.json
        :param configured_catalog: The Configured Catalog describing the schema of the data being received and how it should be persisted in the
                                    destination
        :param input_messages: The stream of input messages received from the source
        :return: Iterable of AirbyteStateMessages wrapped in AirbyteMessage structs
        """

        streams = {s.stream.name for s in configured_catalog.streams}
        logger.info(f"Starting write to OpenSearch with {len(streams)} streams")

        self.client = OpenSearch(
            hosts=[{'host': config['host'], 'port': config['port']}],
            http_compress=False,
            http_auth=(config['username'], config['password']),
            use_ssl=True,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )
        for configured_stream in configured_catalog.streams:
            name = configured_stream.stream.name
            index_name = f"airbyte_raw_{name.lower()}"
            if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                if self.client.indices.exists(index_name):
                    # delete the pre-existing index
                    logger.info(f"Dropping index for overwrite: {index_name}")
                    self.client.indices.delete(index_name)
                self.client.indices.create(index_name)

        self.buffer = defaultdict(list)
        for message in input_messages:
            if message.type == Type.RECORD:
                data = message.record.data
                stream = message.record.stream
                if stream not in streams:
                    logger.debug(f"Stream {stream} was not present in configured streams, skipping")
                    continue
                # add to buffer
                self.buffer[stream].append(json.dumps(data))
                if sum((len(records) for records in self.buffer.values())) >= 5000:
                    self._flush_buffer()

            elif message.type == Type.STATE:
                logger.info(f"flushing buffer for state: {message}")
                self._flush_buffer()
                yield message

        # flush any remaining messages
        self._flush_buffer()

        self.client.close()

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the destination with the needed permissions
            e.g: if a provided API token or password can be used to connect and write to the destination.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this destination, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            client = OpenSearch(
                hosts=[{'host': config['host'], 'port': config['port']}],
                http_compress=True,
                http_auth=(config['username'], config['password']),
                use_ssl=True,
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False,
            )
            response = client.info()
            logger.info(f"Info response: {response}")
            client.close()
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
