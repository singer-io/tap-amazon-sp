# tap-amazon-sp

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Amazon Selling Partner (SP) API](https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#connecting-to-the-selling-partner-api)
- Implements the following streams:
  - [Orders](https://github.com/amzn/selling-partner-api-docs/blob/main/references/orders-api/ordersV0.md#getorders)
  - [OrderItems](https://github.com/amzn/selling-partner-api-docs/blob/main/references/orders-api/ordersV0.md#getorderitems)
  - [Sales](https://github.com/amzn/selling-partner-api-docs/blob/main/references/sales-api/sales.md#parameters)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Authentication

[This](https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#connecting-to-the-selling-partner-api) guide walks through the process necessary for generating the required credentials for authenticating with the API.

## Config

The tap accepts the following config items:

| field                  | type   | required | description                                                                                                                                                                                                |
|------------------------|--------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| refresh_token          | string | yes      | [Auth](https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#step-1-request-a-login-with-amazon-access-token)                         |
| client_id              | string | yes      | [Auth](https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#step-1-request-a-login-with-amazon-access-token)                         |
| client_secret          | string | yes      | [Auth](https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#step-1-request-a-login-with-amazon-access-token)                         |
| aws_access_key         | string | yes      | [IAM Policies and Entities](https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#creating-and-configuring-iam-policies-and-entities) |
| aws_secret_key         | string | yes      | [IAM Policies and Entities](https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#creating-and-configuring-iam-policies-and-entities) |
| role_arn               | string | yes      | [IAM Policies and Entities](https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#creating-and-configuring-iam-policies-and-entities) |
| start_date             | string | yes      | ISO-8601  Example: "2021-08-03" or "2021-08-03T23:29:19+00:00"                                                                                                                                             |
| marketplaces            | array | no       | Array of [Marketplace Country Code](https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#marketplaceid-values) values. Default is ["US"].        |
| sales_data_granularity | string | no       | [Granularity for sales stream](https://github.com/amzn/selling-partner-api-docs/blob/main/references/sales-api/sales.md#granularity) for sales aggregation. Default is "DAY".                                                                 |

## Quick Start

1. Install

Clone this repository, and then install using setup.py. We recommend using a virtualenv:

```bash
$ virtualenv -p python3 venv
$ source venv/bin/activate
$ pip install -e .
```

2. Create your tap's config.json file. Look at [this](#config) table for format and required fields.

Run the Tap in Discovery Mode This creates a catalog.json for selecting objects/fields to integrate:

```bash
tap-amazon-sp --config config.json --discover > catalog.json
```

See the Singer docs on discovery mode [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

4. Run the Tap in Sync Mode (with catalog) and write out to state file

For Sync mode:

```bash
$ tap-amazon-sp --config tap_config.json --catalog catalog.json >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

To load to json files to verify outputs:

```bash
$ tap-amazon-sp --config tap_config.json --catalog catalog.json | target-json >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

To pseudo-load to Stitch Import API with dry run:

```bash
$ tap-amazon-sp --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

---

Copyright &copy; 2018 Stitch
