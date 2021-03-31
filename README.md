# Cube.js opendistro elasticsearch database driver

## Some notes

>  better user opendistro-for-elasticsearch:1.13.0

* unixTimestampSql 

with fix unixtimestamp （1617184163）

* query format

only support with jdbc (for better data process)

## Usage

.env

```code
CUBEJS_DB_URL=http://<username>:<password>@localhost:9200
CUBEJS_DB_ELASTIC_QUERY_FORMAT=jdbc // must user jdbc 
CUBEJS_DB_ELASTIC_OPENDISTRO=true
CUBEJS_DEV_MODE=true
CUBEJS_SCHEDULED_REFRESH_TIMER=false
CUBEJS_API_SECRET=<key>
```

* cube.js

```code
// Cube.js configuration options: https://cube.dev/docs/config
const {OpendistroDriver,OpendistroQuery} = require("@dalongrong/opendistro-driver")
module.exports = {
    dialectFactory: (dataSource) => {
        // need config  datasource  for multitenant env
        return OpendistroQuery
    },
    dbType: ({ dataSource } = {}) => {
        return "opendistro"
    },
    driverFactory: ({ dataSource } = {}) => {
        return new OpendistroDriver({})
    }
};

```


