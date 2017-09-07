# DataHub metastore

[![Build Status](https://travis-ci.org/datahq/metastore.svg?branch=master)](https://travis-ci.org/datahq/metastore)

A search services for DataHub.

Searches Elasticsearch and returns matching documents (returned document content structure are not defined by this module)   

## Quick Start

# Clone the repo and install

`make install`

# Run tests

`make test`

# Run server

`python server.py`


# API

**Endpoint:** `/metastore/search`

**Method:** `GET`

**HEADER:** `Auth-Token` (received from `/auth/check`)

**Query Parameters:**

* q - match-all query string
  Will search the following properties:
    - `title`
    - `datahub.owner`
    - `description`

* size - number of results to return [max 50]
* from - offset to start returning results from

all other parameters will be treated as filters for the query (requiring exact match of value)

**Returns:** All packages that match the filter:
```json
{
  "summary": {
    "total": total-number-of-matched-documents,
    "totalBytes": total-size-of-matched-datasets
  },
  "results": [
    list of matched documents
  ]
}
```
