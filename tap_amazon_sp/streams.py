import datetime
import enum
import time
from functools import lru_cache
from typing import List

import backoff
import singer
from singer import Transformer, metrics
from sp_api.api import Orders, Sales, VendorOrders
from sp_api.base.exceptions import SellingApiRequestThrottledException
from sp_api.base.marketplaces import Marketplaces
from sp_api.base.sales_enum import Granularity

from tap_amazon_sp.helpers import (create_date_interval, flatten_order_items,
                                   format_date, log_backoff, calculate_sleep_time)

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

    def get_records(self, start_date: str, end_date: str, marketplace: str, is_parent: bool = False) -> list:
        """
        Returns a list of records for that stream.

        :param start_date: The start date
        :param marketplace: The Amazon SP marketplace
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

    def get_parent_data(self, start_date: str = None, end_date: str = None, marketplace: Marketplaces = Marketplaces.US) -> list:
        """
        Returns a list of records from the parent stream.

        :return: A list of records
        """
        parent = self.parent(self.config)
        return parent.get_records(start_date, end_date, marketplace, is_parent=True)

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

    def get_marketplaces(self) -> List[Marketplaces]:
        """
        Retrieves marketplace enum list. Defaults to US if no marketplace provided.

        :return: A list of marketplace enums for the marketplace(s) to be used with the API
        """
        marketplaces = self.config.get('marketplaces')
        cleaned_marketplaces = []
        if marketplaces:
            marketplaces_arr = marketplaces.strip().split(" ")
            for marketplace in marketplaces_arr:
                try:
                    marketplace = marketplace.upper()
                    if hasattr(Marketplaces, marketplace):
                        cleaned_marketplaces.append(getattr(Marketplaces, marketplace))
                    else:
                        raise Exception(f"Invalid marketplace {marketplace} provided")
                except Exception as e:
                    # If marketplace not part of enum, log message and throw error
                    valid_marketplaces = set()
                    for mp in dir(Marketplaces):
                        if not mp.startswith("__"):
                            valid_marketplaces.add(mp)
                    # pylint: disable=logging-fstring-interpolation
                    LOGGER.critical(f"provided marketplace '{marketplace}' is not "
                                    f"in Marketplaces set: {valid_marketplaces}")

                    raise e

            return cleaned_marketplaces

        return [Marketplaces.US]

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
            # pylint: disable=logging-fstring-interpolation
            LOGGER.critical(f"provided granularity '{granularity}' is not "
                            f"in Granularity set: {valid_granularities}")

            raise Exception

        return Granularity.DAY


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
        marketplaces = self.get_marketplaces()
        for marketplace in marketplaces:
            start_date = singer.get_bookmark(state, self.tap_stream_id, marketplace.name, self.config['start_date'])
            end_date = self.config['end_date']

            # Value in state will be in the form {replication_key: value}
            if isinstance(start_date, dict):
                start_date = start_date.get(self.replication_key)
            max_record_value = start_date

            with metrics.record_counter(self.tap_stream_id) as counter:
                for record in self.get_records(start_date, end_date, marketplace):
                    transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                    record_replication_value = singer.utils.strptime_to_utc(transformed_record[self.replication_key])
                    if record_replication_value >= singer.utils.strptime_to_utc(max_record_value):
                        singer.write_record(self.tap_stream_id, transformed_record)
                        counter.increment()
                        max_record_value = record_replication_value.isoformat()

                        state = singer.write_bookmark(state, self.tap_stream_id, marketplace.name, {self.replication_key: max_record_value})
                        singer.write_state(state)

            state = singer.write_bookmark(state, self.tap_stream_id, marketplace.name, {self.replication_key: max_record_value})
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
        marketplaces = self.get_marketplaces()
        for marketplace in marketplaces:
            start_date = self.config['start_date']
            end_date = self.config['end_date']
            with metrics.record_counter(self.tap_stream_id) as counter:
                for record in self.get_records(start_date, end_date, marketplace):
                    transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                    singer.write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
        singer.write_state(state)
        return state

class OrdersStreamFullTable(FullTableStream):
    """
    Gets records for orders stream.
    """
    tap_stream_id = 'orders'
    key_properties = ['AmazonOrderId']
    replication_key = 'PurchaseDate'
    valid_replication_keys = ['PurchaseDate']

    @staticmethod
    @lru_cache
    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=5,
                          base=3,
                          factor=20,
                          on_backoff=log_backoff)
    def get_orders(client, start_date, next_token, timer, end_date=None):

        try:
            if not end_date:
                response = client.get_orders(CreatedAfter=start_date,
                                 NextToken=next_token)
            else:
                response = client.get_orders(CreatedAfter=start_date, CreatedBefore=end_date,
                                 NextToken=next_token)
            timer.tags[metrics.Tag.http_status_code] = 200
            return response
        except SellingApiRequestThrottledException as e:
            timer.tags[metrics.Tag.http_status_code] = 429

            raise e

    def get_records(self, start_date, end_date, marketplace, is_parent=False):

        credentials = self.get_credentials()
        start_date = format_date(start_date)
        if end_date:
            end_date = format_date(end_date)

        LOGGER.info(f"Getting records for marketplace: {marketplace.name}")

        client = Orders(credentials=credentials, marketplace=marketplace)
        paginate = True
        next_token = None

        with metrics.http_request_timer('/orders/v0/orders') as timer:
            while paginate:
                response = self.get_orders(client, start_date, next_token, timer, end_date=end_date)

                next_token = response.next_token
                paginate = True if next_token else False

                if is_parent:
                    yield from ((item['AmazonOrderId'], item['LastUpdateDate'])
                                for item in response.payload['Orders'])
                    continue

                yield from response.payload['Orders']

class OrderItemsFullTable(FullTableStream):
    """
    Gets records for order items stream.
    """
    tap_stream_id = 'order_items'
    key_properties = ['AmazonOrderId', 'OrderItemId']
    replication_key = 'OrderLastUpdateDate'
    valid_replication_keys = ['OrderLastUpdateDate']
    parent = OrdersStreamFullTable

    @staticmethod
    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=5,
                          base=3,
                          factor=10,
                          on_backoff=log_backoff)
    def get_order_items(client: Orders, order_id: str):
        response = client.get_order_items(order_id=order_id)
        return response.payload, response.headers

    def get_records(self, start_date: str, end_date: str, marketplace) -> list:

        credentials = self.get_credentials()
        start_date = format_date(start_date)
        if end_date:
            end_date = format_date(end_date)

        LOGGER.info(f"Getting records for marketplace: {marketplace.name}")

        client = Orders(credentials=credentials, marketplace=marketplace)
        for order_id, date in self.get_parent_data(start_date, end_date, marketplace):
            with metrics.http_request_timer(f'/orders/v0/orders/{order_id}/orderItems') as timer:
                response, headers = self.get_order_items(client, order_id)
                timer.tags[metrics.Tag.http_status_code] = 200

                sleep_time = calculate_sleep_time(headers)
                time.sleep(sleep_time)

                order_items = flatten_order_items(response, date)

                yield from order_items

class OrderBuyerInfoFullTable(FullTableStream):
    """
    Gets records for order buyer info stream.
    """
    tap_stream_id = 'order_buyer_info'
    # key_properties = ['AmazonOrderId']
    replication_key = 'OrderLastUpdateDate'
    valid_replication_keys = ['OrderLastUpdateDate']
    parent = OrdersStreamFullTable

    @staticmethod
    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=5,
                          base=3,
                          factor=10,
                          on_backoff=log_backoff)
    def get_order_buyer_info(client: Orders, order_id: str):
        response = client.get_order_buyer_info(order_id=order_id)
        return response.payload, response.headers

    def get_records(self, start_date: str, end_date: str, marketplace) -> list:

        credentials = self.get_credentials()
        start_date = format_date(start_date)
        if end_date:
            end_date = format_date(end_date)

        LOGGER.info(f"Getting records for marketplace: {marketplace.name}")

        client = Orders(credentials=credentials, marketplace=marketplace)
        for order_id, date in self.get_parent_data(start_date, end_date, marketplace):
            with metrics.http_request_timer(f'/orders/v0/orders/{order_id}/buyerInfo') as timer:
                response, headers = self.get_order_buyer_info(client, order_id)
                timer.tags[metrics.Tag.http_status_code] = 200

                # Adding dynamic sleep as per rate limit from Amazon
                sleep_time = calculate_sleep_time(headers)
                time.sleep(sleep_time)

                response['OrderLastUpdateDate'] = date

                yield from [response]

class OrderAddressFullTable(FullTableStream):
    """
    Gets records for order buyer info stream.
    """
    tap_stream_id = 'order_address'
    # key_properties = ['AmazonOrderId']
    replication_key = 'OrderLastUpdateDate'
    valid_replication_keys = ['OrderLastUpdateDate']
    parent = OrdersStreamFullTable

    @staticmethod
    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=5,
                          base=3,
                          factor=10,
                          on_backoff=log_backoff)
    def get_order_address(client: Orders, order_id: str):
        response = client.get_order_address(order_id=order_id)
        return response.payload, response.headers

    def get_records(self, start_date: str, end_date: str, marketplace) -> list:

        credentials = self.get_credentials()
        start_date = format_date(start_date)
        if end_date:
            end_date = format_date(end_date)

        LOGGER.info(f"Getting records for marketplace: {marketplace.name}")

        client = Orders(credentials=credentials, marketplace=marketplace)
        for order_id, date in self.get_parent_data(start_date, end_date, marketplace):
            with metrics.http_request_timer(f'/orders/v0/orders/{order_id}/address') as timer:
                response, headers = self.get_order_address(client, order_id)
                timer.tags[metrics.Tag.http_status_code] = 200
                
                # Adding dynamic sleep as per rate limit from Amazon
                sleep_time = calculate_sleep_time(headers)
                time.sleep(sleep_time)

                response['OrderLastUpdateDate'] = date

                yield from [response]
class VendorPurchaseOrders(FullTableStream):
    """
    Gets records for vendor purchase order stream.
    The date range to search must not be more than 7 days
    """
    tap_stream_id = 'vendor_purchase_orders'
    # key_properties = ['AmazonOrderId']
    replication_key = 'purchaseOrderNumber'
    valid_replication_keys = ['purchaseOrderNumber']

    @staticmethod
    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=5,
                          base=3,
                          factor=10,
                          on_backoff=log_backoff)
    def get_vendor_purchase_orders(client: VendorOrders, start_date, next_token, timer, end_date):
        try:
            response = client.get_purchase_orders(changedAfter=start_date, changedBefore=end_date, nextToken=next_token)
            timer.tags[metrics.Tag.http_status_code] = 200
            return response
        except SellingApiRequestThrottledException as e:
            timer.tags[metrics.Tag.http_status_code] = 429
            raise e

    def get_records(self, start_date: str, end_date: str, marketplace) -> list:

        credentials = self.get_credentials()
        start_date = format_date(start_date)
        if end_date:
            end_date = format_date(end_date)

        LOGGER.info(f"Getting vendor_purchase_orders records for marketplace: {marketplace.name}")

        client = VendorOrders(credentials=credentials, marketplace=marketplace)
        paginate = True
        next_token = None

        with metrics.http_request_timer('/vendor/orders/v1/purchaseOrders') as timer:
            while paginate:
                response = self.get_vendor_purchase_orders(client, start_date, next_token, timer, end_date=end_date)

                next_token = response.next_token
                paginate = True if next_token else False

                yield from response.payload['orders']
class OrdersStream(IncrementalStream):
    """
    Gets records for orders stream.
    """
    tap_stream_id = 'orders'
    key_properties = ['AmazonOrderId']
    replication_key = 'LastUpdateDate'
    valid_replication_keys = ['LastUpdateDate']

    @staticmethod
    @lru_cache
    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=5,
                          base=3,
                          factor=20,
                          on_backoff=log_backoff)
    def get_orders(client, start_date, next_token, timer, end_date=None):

        try:
            if not end_date:
                response = client.get_orders(LastUpdatedAfter=start_date,
                                 NextToken=next_token)
            else:
                response = client.get_orders(LastUpdatedAfter=start_date, LastUpdatedBefore=end_date,
                                 NextToken=next_token)
            timer.tags[metrics.Tag.http_status_code] = 200
            return response
        except SellingApiRequestThrottledException as e:
            timer.tags[metrics.Tag.http_status_code] = 429

            raise e

    def get_records(self, start_date, end_date, marketplace, is_parent=False):

        credentials = self.get_credentials()
        start_date = format_date(start_date)
        if end_date:
            end_date = format_date(end_date)

        LOGGER.info(f"Getting records for marketplace: {marketplace.name}")

        client = Orders(credentials=credentials, marketplace=marketplace)
        paginate = True
        next_token = None

        with metrics.http_request_timer('/orders/v0/orders') as timer:
            while paginate:
                response = self.get_orders(client, start_date, next_token, timer, end_date=end_date)

                next_token = response.next_token
                paginate = True if next_token else False

                if is_parent:
                    yield from ((item['AmazonOrderId'], item['LastUpdateDate'])
                                for item in response.payload['Orders'])
                    continue

                yield from response.payload['Orders']


class OrderItems(IncrementalStream):
    """
    Gets records for order items stream.
    """
    tap_stream_id = 'order_items'
    key_properties = ['AmazonOrderId', 'OrderItemId']
    replication_key = 'OrderLastUpdateDate'
    valid_replication_keys = ['OrderLastUpdateDate']
    parent = OrdersStream

    @staticmethod
    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=5,
                          base=3,
                          factor=10,
                          on_backoff=log_backoff)
    def get_order_items(client: Orders, order_id: str):
        response = client.get_order_items(order_id=order_id)
        return response.payload, response.headers

    def get_records(self, start_date: str, end_date: str, marketplace) -> list:

        credentials = self.get_credentials()
        start_date = format_date(start_date)
        if end_date:
            end_date = format_date(end_date)

        LOGGER.info(f"Getting records for marketplace: {marketplace.name}")

        client = Orders(credentials=credentials, marketplace=marketplace)
        for order_id, date in self.get_parent_data(start_date, end_date, marketplace):
            with metrics.http_request_timer(f'/orders/v0/orders/{order_id}/orderItems') as timer:
                response, headers = self.get_order_items(client, order_id)
                timer.tags[metrics.Tag.http_status_code] = 200

                sleep_time = calculate_sleep_time(headers)
                time.sleep(sleep_time)

                order_items = flatten_order_items(response, date)

                yield from order_items

class OrderBuyerInfo(IncrementalStream):
    """
    Gets records for order buyer info stream.
    """
    tap_stream_id = 'order_buyer_info'
    # key_properties = ['AmazonOrderId']
    replication_key = 'OrderLastUpdateDate'
    valid_replication_keys = ['OrderLastUpdateDate']
    parent = OrdersStream

    @staticmethod
    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=5,
                          base=3,
                          factor=10,
                          on_backoff=log_backoff)
    def get_order_buyer_info(client: Orders, order_id: str):
        response = client.get_order_buyer_info(order_id=order_id)
        return response.payload, response.headers

    def get_records(self, start_date: str, end_date: str, marketplace) -> list:

        credentials = self.get_credentials()
        start_date = format_date(start_date)
        if end_date:
            end_date = format_date(end_date)

        LOGGER.info(f"Getting records for marketplace: {marketplace.name}")

        client = Orders(credentials=credentials, marketplace=marketplace)
        for order_id, date in self.get_parent_data(start_date, end_date, marketplace):
            with metrics.http_request_timer(f'/orders/v0/orders/{order_id}/buyerInfo') as timer:
                response, headers = self.get_order_buyer_info(client, order_id)
                timer.tags[metrics.Tag.http_status_code] = 200

                # Adding dynamic sleep as per rate limit from Amazon
                sleep_time = calculate_sleep_time(headers)
                time.sleep(sleep_time)

                response['OrderLastUpdateDate'] = date

                yield from [response]

class OrderAddress(IncrementalStream):
    """
    Gets records for order buyer info stream.
    """
    tap_stream_id = 'order_address'
    # key_properties = ['AmazonOrderId']
    replication_key = 'OrderLastUpdateDate'
    valid_replication_keys = ['OrderLastUpdateDate']
    parent = OrdersStream

    @staticmethod
    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=5,
                          base=3,
                          factor=10,
                          on_backoff=log_backoff)
    def get_order_address(client: Orders, order_id: str):
        response = client.get_order_address(order_id=order_id)
        return response.payload, response.headers

    def get_records(self, start_date: str, end_date: str, marketplace) -> list:

        credentials = self.get_credentials()
        start_date = format_date(start_date)
        if end_date:
            end_date = format_date(end_date)

        LOGGER.info(f"Getting records for marketplace: {marketplace.name}")

        client = Orders(credentials=credentials, marketplace=marketplace)
        for order_id, date in self.get_parent_data(start_date, end_date, marketplace):
            with metrics.http_request_timer(f'/orders/v0/orders/{order_id}/address') as timer:
                response, headers = self.get_order_address(client, order_id)
                timer.tags[metrics.Tag.http_status_code] = 200
                
                # Adding dynamic sleep as per rate limit from Amazon
                sleep_time = calculate_sleep_time(headers)
                time.sleep(sleep_time)

                response['OrderLastUpdateDate'] = date

                yield from [response]
                
class SalesStream(IncrementalStream):
    """
    Gets records for sales stream.
    """
    tap_stream_id = 'sales'
    key_properties = ['interval']
    replication_key = 'retrieved'
    valid_replication_keys = ['retrieved']

    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          max_tries=3,
                          on_backoff=log_backoff)
    def get_sales_data(self, client, interval, granularity, timer):
        try:
            response = client.get_order_metrics(interval=interval, granularity=granularity)
            timer.tags[metrics.Tag.http_status_code] = 200
            return response
        except SellingApiRequestThrottledException as e:
            timer.tags[metrics.Tag.http_status_code] = 429

            raise e

    def get_records(self, start_date, marketplace, is_parent=False):

        credentials = self.get_credentials()
        granularity = self.get_granularity()
        start_date = format_date(start_date)
        start_date_dt = singer.utils.strptime_to_utc(start_date)
        end_date_dt = datetime.datetime.utcnow()
        end_date = end_date_dt.isoformat()

        LOGGER.info(f"Getting records for marketplace: {marketplace}")

        client = Sales(credentials=credentials, marketplace=marketplace)
        paginate = True
        next_token = None
        interval = create_date_interval(start_date_dt, end_date_dt)

        with metrics.http_request_timer('/sales/v1/orderMetrics') as timer:
            while paginate:
                response = self.get_sales_data(client, interval, granularity, timer)

                next_token = response.next_token
                paginate = True if next_token else False

                for record in response.payload:
                    record.update({'retrieved': end_date})

                yield from response.payload


STREAMS = {
    'orders': OrdersStream,
    'order_items': OrderItems,
    'order_buyer_info': OrderBuyerInfo,
    'order_address': OrderAddress,
    'sales': SalesStream,
}

def StreamClassSelector(config, stream):
    INCREMENTAL_STREAMS = STREAMS

    FULL_TABLE_STREAMS = {
        'orders': OrdersStreamFullTable,
        'order_items': OrderItemsFullTable,
        'order_buyer_info': OrderBuyerInfoFullTable,
        'order_address': OrderAddressFullTable,
        'vendor_purchase_orders': VendorPurchaseOrders,
    }

    if stream.replication_method == 'FULL_TABLE':
        return FULL_TABLE_STREAMS[stream.tap_stream_id](config)
    elif stream.replication_method == 'INCREMENTAL':
        return INCREMENTAL_STREAMS[stream.tap_stream_id](config)
