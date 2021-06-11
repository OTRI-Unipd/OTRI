# Open Trading Research Infrastructure

**Open Trading Research Infrastructure (OTRI)** is a research platform for financial computing focusing on data retrieval, validation, and analysis.

## Getting started

### Requirements

- Any OS that supports Python 3.7
- Python 3.7 and PIP

### Installation

The easiest way to install the project is by cloning the git repository.

```bash
$~: git clone https://github.com/OTRI-Unipd/OTRI
$~: cd OTRI
$~: pip install -r requirements.txt
```

### Database

Currently, only a PostgreSQL database is supported.
The structure of the database is the following:

- An `atoms` table that contains all retrieved data [ID : BigSerial, data_json : jsonb]
- A `metadata` table that contains static metadata [ID : BigSerial, data_json : jsonb]

To bootstrap scripts, you'll have to populate the `metadata` table with rows that contain the `ticker` key.
> [!NOTE]
> There shouldn't be two metadata atoms with the same `ticker` value, consider using a `UNIQUE index`

#### Setting up metadata table

To start you'll need to update a ticker list to the atoms table and then generate the metadata table.

You can start by importing any list of tickers, in this example we're importing Standard & Poor's 500 tickers list (as of june 2020).
A collection of ticker files available can be found in the `docs` folder.

```bash
python metadata_import.py -f snp500
```

We now have metadata atoms in the atoms table, we need to extract them and put them in the metadata table for other scripts to use it.

```bash
python metadata_generate.py
```

This will take metadata atoms from the atoms table, group them by ticker, merge them, and then uploading the metadata to its table.

### Download data

When a list of tickers is ready you can start and download real data.

You can use one of the downloaders.

TODO: explain downloaders.

## Configuration files

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

Alternatively, a `/secrets` directory containing extension-less files having file name as key and file contents as value. This is useful when passing configuration values to the Docker image as a volume.

```bash
secrets
├─── postgre_username
├─── postgre_password
├─── postgre_database
├─── postgre_host
├─── alphavantage_api_key
└─── tradier_api_key
```

## Copyrigth

```txt
  Copyright 2020 OTRI. All rights reserved.
```
