# Open Trading Research Infrastructure

**Open Trading Research Infrastructure (OTRI)** is a research platform for financial computing focusing on data retrieval, validation and analysis.

## Usage

### Requirements

- Any OS that supports Python 3.7
- Python 3.7 and PIP

### Installation

```bash
$~: git clone https://github.com/OTRI-Unipd/OTRI
$~: cd OTRI
$~: pip install -r requirements.txt
```

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

Or alternatively a `/secrets` directory containing extension-less files having file name as key and file contents as value. This is useful when passing configuration values to the Docker image as a volume.

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
