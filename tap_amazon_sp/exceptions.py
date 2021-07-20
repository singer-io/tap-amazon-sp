from requests.exceptions import ConnectionError
from singer import get_logger
from sp_api.base.exceptions import SellingApiRequestThrottledException

LOGGER = get_logger()


class AmazonSPRateLimitError(SellingApiRequestThrottledException):
    pass


class AmazonSPConnectionError(ConnectionError):
    pass
