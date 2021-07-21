import datetime
import enum
import time

import backoff
import singer
from singer import Transformer, metrics
from sp_api.api import Orders
from sp_api.base.exceptions import SellingApiRequestThrottledException
from sp_api.base.marketplaces import Marketplaces

LOGGER = singer.get_logger()


def log_backoff(details):
    '''
    Logs a backoff retry message
    '''
    LOGGER.warning(f'Error receiving data from Amazon SP API. Sleeping {details["wait"]:.1f} seconds before trying again')


def calculate_sleep_time(headers: dict):
    """
    Checks the rate limit headers and returns number of seconds
    to sleep in between calls.
    """
    rate_limit = headers.get('x-amzn-RateLimit-Limit', '100')
    try:
        rate_limit_value = float(rate_limit)
    except ValueError:
        rate_limit_value = 100

    # Set minimum rate limit value
    # rate_limit_value = max(0.5, rate_limit_value)

    LOGGER.info(f"x-amzn-RateLimit-Limit: {headers.get('x-amzn-RateLimit-Limit')}")

    return 1 / rate_limit_value


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

    def get_records(self, is_parent: bool = False) -> list:
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

    def get_parent_data(self) -> list:
        """
        Returns a list of records from the parent stream.

        :return: A list of records
        """
        parent = self.parent(self.config)
        return parent.get_records(is_parent=True)

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
        start_time = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, self.config['start_date'])
        max_record_value = start_time

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
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

    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=3,
                          on_backoff=log_backoff)
    def get_records(self, is_parent=False):

        credentials = self.get_credentials()
        marketplace = self.get_marketplace()

        # TODO: hardcoding bookmark for now
        last_updated_after = (datetime.datetime.utcnow() - datetime.timedelta(days=2)).isoformat()
        client = Orders(credentials=credentials, marketplace=marketplace)
        paginate = True
        next_token = None

        with metrics.http_request_timer('/orders/v0/orders') as timer:
            while paginate:
                try:
                    response = client.get_orders(LastUpdatedAfter=last_updated_after, NextToken=next_token)
                    timer.tags[metrics.Tag.http_status_code] = 200
                except SellingApiRequestThrottledException as e:
                    timer.tags[metrics.Tag.http_status_code] = 429

                    raise e

                next_token = response.next_token
                paginate = True if next_token else False

                if is_parent:
                    yield from (item['AmazonOrderId'] for item
                                in response.payload['Orders'])
                    continue

                # transformed = (self.check_and_update_missing_fields(data, stream_schema)
                #                for data in response.payload.get('Orders'))

                # yield from transformed
                yield from response.payload


# TODO: check if running Orders stream before OrderItems stream will affect the results
class OrderItems(FullTableStream):
    tap_stream_id = 'order_items'
    key_properties = ['AmazonOrderId', 'OrderItemId']
    parent = OrdersStream

    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=3,
                          on_backoff=log_backoff)
    def get_order_items(self, client: Orders, order_id: str):
        return client.get_order_items(order_id=order_id).payload

    def get_records(self, is_parent: bool = False) -> list:

        credentials = self.get_credentials()
        marketplace = self.get_marketplace()

        client = Orders(credentials=credentials, marketplace=marketplace)
        for order_id in self.get_parent_data():
            with metrics.http_request_timer(f'/orders/v0/orders/{order_id}/orderItems') as timer:
                response = self.get_order_items(client, order_id)
                timer.tags[metrics.Tag.http_status_code] = 200

                # Endpoint allows for 1 request per second
                time.sleep(1)

                yield response


# TODO: check if running Orders stream before Order stream will affect the resultsclass OrderItems(FullTableStream):
class OrderStream(FullTableStream):
    tap_stream_id = 'order'
    key_properties = ['AmazonOrderId']
    parent = OrdersStream

    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=3,
                          on_backoff=log_backoff)
    def get_order(self, client: Orders, order_id: str):
        return client.get_order(order_id=order_id)

    def get_records(self, is_parent: bool = False) -> list:

        credentials = self.get_credentials()
        marketplace = self.get_marketplace()

        client = Orders(credentials=credentials, marketplace=marketplace)
        for order_id in self.get_parent_data():
            with metrics.http_request_timer(f'/orders/v0/orders/{order_id}') as timer:
                response = self.get_order(client, order_id)
                timer.tags[metrics.Tag.http_status_code] = 200

                # Check headers for rate limit and sleep in between calls
                sleep_time = calculate_sleep_time(response.headers)
                LOGGER.info(f"sleeping for {round(sleep_time, 2)} seconds")
                time.sleep(sleep_time)

                yield response.payload


STREAMS = {
    'order': OrderStream,
    'orders': OrdersStream,
    'order_items': OrderItems,
}
