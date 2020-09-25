# Open Trading Research Infrastructure
Open Trading Research Infrastructure.

## Getting started

### Downloading

In order to start retrieving data any downloader requires a list of tickers in the database metadata table.

#### Setting up metadata table

To start you'll need to update a ticker list to the atoms table and then generate the metadata table.

You can start by importing any list of tickers, in this example we're importing Standard & Poor's 500 tickers list (as of june 2020)

```bash
python metadata_import.py -f snp500
```

We now have metadata atoms in the atoms table, we need to extract them and put them in the metadata table for other scripts to use it.

```bash
python metadata_generate.py
```

This will take metadata atoms from the atoms table, group them by ticker, merge them, and then uploading the metadata to its table.

## config.json
In order to work properly most scripts require a `config.json` with the following structure:

```JSON
{
    "postgresql_username" : "",
    "postgresql_password" : "",
    "postgresql_host" : "",
    "postgresql_port" : "",
    "postgresql_database" : "",
    "alphavantage_api_key": "",
    "tradier_api_key": ""
}
```

## Copyrigth
```
  Copyright 2020 OTRI. All rights reserved.
```
