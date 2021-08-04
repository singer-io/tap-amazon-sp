import datetime
from typing import List, Tuple

import singer

LOGGER = singer.get_logger()

def log_backoff(details):
    """
    Logs a backoff retry message
    """
    # pylint: disable=logging-fstring-interpolation
    LOGGER.warning('Error receiving data from Amazon SP API. '
                  f'Sleeping {details["wait"]:.1f} seconds before trying again')


def calculate_sleep_time(headers: dict) -> float:
    """
    Checks the rate limit headers and returns number of seconds
    to sleep in between calls.
    """
    rate_limit = headers.get('x-amzn-RateLimit-Limit', '100')
    try:
        rate_limit_value = float(rate_limit)
    except ValueError:
        rate_limit_value = 100

    # Ensure value is not 0 to avoid ZeroDivisionError
    rate_limit_value = 0.1 if rate_limit_value == 0 else rate_limit_value

    # pylint: disable=logging-fstring-interpolation
    LOGGER.info(f"x-amzn-RateLimit-Limit: "
                f"{headers.get('x-amzn-RateLimit-Limit')}")

    return 1 / rate_limit_value


def format_date(start_date: str) -> str:
    """
    Strips out time zone info to comply with API format
    """
    return singer.utils.strptime_to_utc(start_date) \
                .replace(tzinfo=None).isoformat()


def create_date_interval(start_date: datetime.datetime,
                         end_date: datetime.datetime,
                         hours=1) -> Tuple[datetime.datetime, datetime.datetime]:
    date = (start_date - datetime.timedelta(hours=hours))
    return (_prepare_datetime(date), _prepare_datetime(end_date))


def _prepare_datetime(datetimeobj: datetime.datetime) -> str:
    return datetimeobj.astimezone().replace(microsecond=0).isoformat()


def flatten_order_items(response: dict, date_to_add: str) -> List[dict]:
    """
    Flatten a dictionary of nested items.
    """
    order_items = []
    amazon_order_id = response.get("AmazonOrderId")

    for order_item in response['OrderItems']:
        order_item['AmazonOrderId'] = amazon_order_id
        order_item['OrderLastUpdateDate'] = date_to_add
        order_items.append(order_item)

    return order_items
