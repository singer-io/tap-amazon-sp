from unittest import TestCase, mock

from sp_api.base.exceptions import SellingApiRequestThrottledException

import tap_amazon_sp


CONFIG = {
  "refresh_token": "Atzr|abc123",
  "client_id": "amzn123",
  "client_secret": "abcde",
  "aws_access_key": "ABCDE",
  "aws_secret_key": "abc123",
  "role_arn": "arn:aws:iam::123456:role/some_role",
  "start_date": "2021-08-03T16:41:14+00:00",
  "marketplaces": "GB US",
  "sales_data_granularity": "HOUR"
}

@mock.patch('tap_amazon_sp.streams.OrdersStream.get_orders')
class TestBackoff(TestCase):

    def test_request_backoff_on_retry_error(self, mock_get_orders):
        start_date = CONFIG.get('start_date')

        mock_get_orders.side_effect = SellingApiRequestThrottledException(
            [{
                'message': 'You exceeded your quota for the requested resource.',
                'code': 'QuotaExceeded'
            }])

        with self.assertRaises(SellingApiRequestThrottledException):

            response = tap_amazon_sp.streams.OrdersStream.get_orders(
                client=None,
                start_date=start_date,
                next_token=None,
                timer=None
            )
