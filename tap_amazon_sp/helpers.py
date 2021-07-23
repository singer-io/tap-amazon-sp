import singer

LOGGER = singer.get_logger()

def log_backoff(details):
    """
    Logs a backoff retry message
    """
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

    LOGGER.info(f"x-amzn-RateLimit-Limit: "
                f"{headers.get('x-amzn-RateLimit-Limit')}")

    return 1 / rate_limit_value


def format_date(start_date: str) -> str:
    """
    Strips out time zone info to comply with API format
    """
    return singer.utils.strptime_to_utc(start_date) \
                .replace(tzinfo=None).isoformat()
