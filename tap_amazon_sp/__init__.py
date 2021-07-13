import singer
from singer import utils

from tap_amazon_sp.discover import discover
from tap_amazon_sp.sync import sync

REQUIRED_CONFIG_KEYS = [
    "refresh_token",
    "client_id",
    "client_secret",
    "aws_access_key",
    "aws_secret_key",
    "role_arn",
    "start_date",
]
LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
