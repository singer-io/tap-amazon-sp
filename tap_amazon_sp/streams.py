import datetime
import enum
from functools import lru_cache

from generate_schema import create_date_interval
import time

import backoff
import singer
from singer import Transformer, metrics
from sp_api.api import Orders, Sales
from sp_api.base.exceptions import SellingApiRequestThrottledException
from sp_api.base.marketplaces import Marketplaces
from sp_api.base.sales_enum import Granularity

from tap_amazon_sp.helpers import (calculate_sleep_time, flatten_order_items, format_date,
                                   log_backoff)

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

    def __init__(self, config: dict) -> None:
        self.config = config

    def get_records(self, start_date: str, state: dict, is_parent: bool = False) -> list:
        """
        Returns a list of records for that stream.

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

    def get_parent_data(self, start_date: str = None) -> list:
        """
        Returns a list of records from the parent stream.

        :return: A list of records
        """
        parent = self.parent(self.config)
        return parent.get_records(start_date, is_parent=True)

    def get_credentials(self) -> dict:
        """
        Constructs the credentials object for authenticating with the API.

        :return: A dictionary with secrets
        """
        return {
            'refresh_token': self.config['refresh_token'],
            'lwa_app_id': self.config['client_id'],
            'lwa_client_secret': self.config['client_secret'],
            'aws_access_key': self.config['aws_access_key'],
            'aws_secret_key': self.config['aws_secret_key'],
            'role_arn': self.config['role_arn'],
        }

    def get_marketplace(self) -> enum:
        """
        Retrieves marketplace enum. Defaults to US if no marketplace provided.

        :return: An enum for the marketplace to be used with the API
        """
        marketplace = self.config.get('marketplace')
        if marketplace:
            marketplace = marketplace.upper()
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

    def get_granularity(self) -> enum:
        """
        Retrieves granularity enum. Defaults to DAY if no granularity provided.

        :return: An enum for the granularity to be used with the API
        """
        granularity = self.config.get('sales_data_granularity')
        if granularity:
            granularity = granularity.upper()
            if hasattr(Granularity, granularity):
                return getattr(Granularity, granularity)

            # If granularity not part of enum, log message and throw error
            valid_granularities = set()
            for mp in dir(Granularity):
                if not mp.startswith("__"):
                    valid_granularities.add(mp)
            LOGGER.critical(f"provided marketplace '{granularity}' is not "
                            f"in Granularity set: {valid_granularities}")

            raise Exception

        return Granularity.DAY

    def check_and_update_missing_fields(self, data, schema):
        fields = list(schema['properties'].keys())
        properties = schema['properties']

        if not all(field in data for field in fields):
            missing_fields = set(fields) - data.keys()
            for field in missing_fields:
                data[field] = self._get_empty_field_type(field, properties)

        yield data

    # TODO: make this more robust
    def _get_empty_field_type(self, field, properties):
        mapping = {
            'string': '',
            'boolean': False,
            'array': [],
            'integer': 0,
            'float': 0,
            'object': {}
        }

        types = properties[field]

        if "null" in types:
            types.remove("null")

        property_type = types[0]

        return mapping[property_type]


class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'
    batched = False

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict, transformer: Transformer) -> dict:
        """
        The sync logic for an incremental stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        start_date = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, self.config['start_date'])
        max_record_value = start_date

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(start_date):
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

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict, transformer: Transformer) -> dict:
        """
        The sync logic for an full table stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record)
                counter.increment()

        singer.write_state(state)
        return state


class OrdersStream(IncrementalStream):
    """
    Gets records for a sample stream.
    """
    tap_stream_id = 'orders'
    key_properties = ['AmazonOrderId']
    replication_key = 'LastUpdateDate'
    valid_replication_keys = ['LastUpdateDate']

    @lru_cache
    def get_orders(self, client, start_date, next_token):
        return client.get_orders(LastUpdatedAfter=start_date,
                                 NextToken=next_token)

    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=3,
                          on_backoff=log_backoff)
    def get_records(self, start_date, is_parent=False):

        credentials = self.get_credentials()
        marketplace = self.get_marketplace()
        start_date = format_date(start_date)

        client = Orders(credentials=credentials, marketplace=marketplace)
        paginate = True
        next_token = None

        with metrics.http_request_timer('/orders/v0/orders') as timer:
            while paginate:
                try:
                    response = self.get_orders(client, start_date, next_token)
                    timer.tags[metrics.Tag.http_status_code] = 200
                except SellingApiRequestThrottledException as e:
                    timer.tags[metrics.Tag.http_status_code] = 429

                    raise e

                next_token = response.next_token
                paginate = True if next_token else False

                if is_parent:
                    yield from ((item['AmazonOrderId'], item['LastUpdateDate'])
                                for item in response.payload['Orders'])
                    continue

                yield from response.payload['Orders']


class OrderItems(IncrementalStream):
    tap_stream_id = 'order_items'
    key_properties = ['AmazonOrderId', 'OrderItemId']
    replication_key = 'OrderLastUpdateDate'
    valid_replication_keys = ['OrderLastUpdateDate']
    parent = OrdersStream

    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=3,
                          on_backoff=log_backoff)
    def get_order_items(self, client: Orders, order_id: str):
        return client.get_order_items(order_id=order_id).payload

    def get_records(self, start_date: str) -> list:

        credentials = self.get_credentials()
        marketplace = self.get_marketplace()
        start_date = format_date(start_date)

        client = Orders(credentials=credentials, marketplace=marketplace)
        for order_id, date in self.get_parent_data(start_date):
            with metrics.http_request_timer(f'/orders/v0/orders/{order_id}/orderItems') as timer:
                response = self.get_order_items(client, order_id)
                timer.tags[metrics.Tag.http_status_code] = 200

                # Endpoint allows for 1 request per second
                time.sleep(1)

                order_items = flatten_order_items(response, date)

                yield from order_items


class SalesStream(IncrementalStream):
    tap_stream_id = 'sales'
    key_properties = ['interval']
    replication_key = 'retrieved'
    valid_replication_keys = ['retrieved']

    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=3,
                          on_backoff=log_backoff)
    def get_records(self, start_date, is_parent=False):

        credentials = self.get_credentials()
        marketplace = self.get_marketplace()
        granularity = self.get_granularity()
        start_date = format_date(start_date)
        start_date_dt = singer.utils.strptime_to_utc(start_date)
        end_date_dt = datetime.datetime.utcnow()
        end_date = end_date_dt.isoformat()

        client = Sales(credentials=credentials, marketplace=marketplace)
        paginate = True
        next_token = None
        interval = create_date_interval(start_date_dt, end_date_dt)

        with metrics.http_request_timer('/sales/v1/orderMetrics') as timer:
            while paginate:
                try:
                    response = client.get_order_metrics(interval=interval, granularity=granularity)
                    timer.tags[metrics.Tag.http_status_code] = 200
                except SellingApiRequestThrottledException as e:
                    timer.tags[metrics.Tag.http_status_code] = 429

                    raise e

                next_token = response.next_token
                paginate = True if next_token else False

                for record in response.payload:
                    record.update({'retrieved': end_date})

                yield from response.payload


STREAMS = {
    'orders': OrdersStream,
    'order_items': OrderItems,
    'sales': SalesStream,
}
