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

**Elasticsearch:** version 5.x should be installed


**Endpoint:** `/metastore/search`

**Method:** `GET`

**HEADER:** `Auth-Token` (received from `/auth/check`)

**Query Parameters:**

* q - match-all query string
  Will search the following properties:
    - `title`
    - `datahub.owner`
    - `datahub.ownerid`
    - `datapackage.readme`

* size - number of results to return [max 100]
* from - offset to start returning results from

all other parameters will be treated as filters for the query (requiring exact match of value)

**Returns:** All packages that match the filter:
```json
{
  "summary": {
    "total": "total-number-of-matched-documents",
    "totalBytes": "total-size-of-matched-datasets"
  },
  "results": [
    "list of matched documents"
  ]
}
```

**Endpoint:** `/metastore/search/events`

**Method:** `GET`

**HEADER:** `Auth-Token` (received from `/auth/check`)

**Query Parameters:**

* q - match-all query string
* event_entity - flow|account|etc... (currently only `flow` is supported)
* event_action - create|finished|deleted|etc... (currently only `finished` is supported)
* owner - ownerid (usually hash of user's Email)
* dataset - dataset name
* status - OK|Not OK
* findability - published|unlisted|private

**Query Parameters for pagination and sorting:**
* sort - desc|asc (defaults to desc)
* size - number of results to return [max 100]
* from - offset to start returning results from

**Returns:** All packages that match the filter:
```json
{
  "results": [
    {
      "dataset": "finance-vix",
      "event_action": "finished",
      "event_entity": "flow",
      "findability": "published",
      "messsage": "",
      "owner": "core",
      "ownerid": "core",
      "status": "OK",
      "timestamp": "2017-01-01T00:00:00.000000",
      "payload": {
        "flow-id": "core/finance-vix"
      }
    }
  ],
  "summary": {
    "total": 1,
    "totalBytes": 0
  }
}
```
