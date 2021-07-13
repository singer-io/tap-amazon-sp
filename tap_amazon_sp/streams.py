import csv
import datetime
import enum
import time
from typing import Any, Iterator

import requests
import singer
from singer import Transformer, metrics
from sp_api.base.marketplaces import Marketplaces

from tap_amazon_sp.client import Client

LOGGER = singer.get_logger()

class BaseStream:
    """
    A base class representing singer streams.

    :param client: The API client used extract records from the external source
    """
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    params = {}
    parent = None

    def get_records(self, config: dict, stream_schema: dict, is_parent: bool = False) -> list:
        """
        Returns a list of records for that stream.

        :param config: The tap config file
        :param stream_schema: The schema for the stream
        :param is_parent: If true, may change the type of data
            that is returned for a child stream to consume
        :return: list of records
        """
        raise NotImplementedError("Child classes of BaseStream require implementation")

    def set_parameters(self, params: dict) -> None:
        """
        Sets or updates the `params` attribute of a class.

        :param params: Dictionary of parameters to set or update the class with
        """
        self.params = params

    def get_parent_data(self, config: dict = None) -> list:
        """
        Returns a list of records from the parent stream.

        :param config: The tap config file
        :return: A list of records
        """
        parent = self.parent(self.client)
        return parent.get_records(config, is_parent=True)

    def get_credentials(self, config: dict) -> dict:
        """
        Constructs the credentials object for authenticating with the API.

        :param config: The tap config file
        :return: A dictionary with secrets
        """
        return {
            'refresh_token': config['refresh_token'],
            'lwa_app_id': config['client_id'],
            'lwa_client_secret': config['client_secret'],
            'aws_access_key': config['aws_access_key'],
            'aws_secret_key': config['aws_secret_key'],
            'role_arn': config['role_arn'],
        }

    def get_marketplace(self, config: dict) -> enum:
        """
        Retrieves marketplace enum. Defaults to US if no marketplace provided.

        :param config: The tap config file
        :return: An enum for the marketplace to be used with the API
        """
        marketplace = config.get('marketplace')
        if marketplace:
            marketplace.upper()
            if hasattr(Marketplaces, marketplace):
                return getattr(Marketplaces, marketplace)

            # If marketplace not part of enum, log message and throw error
            valid_marketplaces = set()
            for mp in dir(Marketplaces):
                if not mp.startswith("__"):
                    valid_marketplaces.add(mp)
            LOGGER.critical(f"provided marketplace '{marketplace}' is not "
                            f"in Marketplaces set: {valid_marketplaces}")

            raise Exception

        return Marketplaces.US

    def check_and_update_missing_fields(self, data, schema):
        fields = list(schema['properties'].keys())

        if not all(field in data for field in fields):
            missing_fields = set(fields) - data.keys()
            for field in missing_fields:
                data[field] = None

        yield data


class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'
    batched = False

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict, config: dict, transformer: Transformer) -> dict:
        """
        The sync logic for an incremental stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        start_time = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        max_record_value = start_time

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(config, stream_schema):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                record_replication_value = singer.utils.strptime_to_utc(transformed_record[self.replication_key])
                if record_replication_value >= singer.utils.strptime_to_utc(max_record_value):
                    singer.write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    max_record_value = record_replication_value.isoformat()

        state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_record_value)
        singer.write_state(state)
        return state


class FullTableStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    FULL_TABLE replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'FULL_TABLE'

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict, config: dict, transformer: Transformer) -> dict:
        """
        The sync logic for an full table stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(config):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record)
                counter.increment()

        singer.write_state(state)
        return state


class Orders(IncrementalStream):
    """
    Gets records for a sample stream.
    """
    tap_stream_id = 'orders'
    key_properties = ['AmazonOrderId']
    replication_key = 'LastUpdateDate'
    valid_replication_keys = ['LastUpdateDate']

    def get_records(self, config, stream_schema, is_parent=False):
        from sp_api.api import Orders

        credentials = self.get_credentials(config)
        marketplace = self.get_marketplace(config)

        # TODO: hardcoding bookmark for now
        last_updated_after = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat()
        client = Orders(credentials=credentials, marketplace=marketplace)
        paginate = True
        next_token = None

        with metrics.http_request_timer('/orders/v0/orders') as timer:
            while paginate:
                response = client.get_orders(LastUpdatedAfter=last_updated_after, NextToken=next_token)
                timer.tags[metrics.Tag.http_status_code] = 200
                next_token = response.next_token
                paginate = True if next_token else False

                # transformed = (self.check_and_update_missing_fields(data, stream_schema)
                #                for data in response.payload.get('Orders'))

                yield from response.payload.get('Orders')


STREAMS = {
    'orders': Orders,
}
