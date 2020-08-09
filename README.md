# Open Trading Research Infrastructure

**Open Trading Research Infrastructure (OTRI)** is a research platform for financial computing focusing on data retrieval, validation, and analysis.

## Getting started

### Requirements

- Any OS that supports Python 3.7
- Python 3.7 and PIP

### Installation

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
> There shouldn't be two atoms with the same `ticker` value, consider using a `UNIQUE index`

### Configuration files

In order to work properly most scripts require a `config.json` with the following structure:

```JSON
{
    "postgre_username" : "",
    "postgre_password" : "",
    "postgre_database" : "",
    "postgre_host" : "",
    "alphavantage_api_key": ""
}
```

Alternatively, a `/secrets` directory containing extension-less files having file name as key and file contents as value. This is useful when passing configuration values to the Docker image as a volume.

```bash
secrets
├─── postgre_username
├─── postgre_password
├─── postgre_database
├─── postgre_host
└─── alphavantage_api_key
```

## Copyrigth

```
  Copyright 2020 OTRI. All rights reserved.
```
