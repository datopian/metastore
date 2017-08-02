# DataHub metastore

[![Build Status](https://travis-ci.org/datahq/metastore.svg?branch=master)](https://travis-ci.org/datahq/metastore)

A search services for DataHub

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
* size - number of results to return [max 50]
* from - offset to start returning results from

all other parameters will be treated as filters for the query (requiring exact match of value)

**Returns:** All packages that match the filter.
