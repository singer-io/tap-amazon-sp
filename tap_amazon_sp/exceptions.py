from sp_api.base.exceptions import SellingApiRequestThrottledException
from requests.exceptions import ConnectionError


class AmazonSPRateLimitError(SellingApiRequestThrottledException):
    pass


class AmazonSPConnectionError(ConnectionError):
    pass
