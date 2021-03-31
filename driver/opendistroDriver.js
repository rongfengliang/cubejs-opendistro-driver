const { Client } = require('@elastic/elasticsearch');
const SqlString = require('sqlstring');
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');
const OpendistroQuery = require('./opendistroQuery');

class OpendistroDriver extends BaseDriver {
  constructor(config) {
    super();

    const auth = {
      username: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
    };

    // TODO: This config applies to AWS ES, Elastic.co ES, Native ES and OpenDistro ES
    // They have different dialects according to their respective documentation
    this.config = {
      url: process.env.CUBEJS_DB_URL,
      ssl: this.getSslOptions(),
      auth,
      openDistro:
        (process.env.CUBEJS_DB_ELASTIC_OPENDISTRO || 'false').toLowerCase() === 'true' ||
        process.env.CUBEJS_DB_TYPE === 'odelasticsearch',
      queryFormat: process.env.CUBEJS_DB_ELASTIC_QUERY_FORMAT || 'jdbc',
      ...config
    };

    this.client = new Client({
      node: this.config.url,
      cloud: this.config.cloud,
      auth: this.config.auth,
      ssl: this.config.ssl
    });

    if (this.config.openDistro) {
      this.sqlClient = new Client({
        node: `${this.config.url}/_opendistro`,
        ssl: this.config.ssl,
        auth: this.config.auth,
      });
    } else {
      this.sqlClient = this.client;
    }
  }
  static dialectClass() {
    return OpendistroQuery;
  }  
  static driverEnvVariables() {
    return [
      'CUBEJS_DB_URL',
      'CUBEJS_DB_ELASTIC_QUERY_FORMAT',
      'CUBEJS_DB_ELASTIC_OPENDISTRO'
    ];
  }

  async testConnection() {
    return this.client.cat.indices({
      format: 'json'
    });
  }

  async release() {
    await this.client.close();

    if (this.config.openDistro && this.sqlClient) {
      await this.sqlClient.close();
    }
  }

  async query(query, values) {
    try {
      const result = (await this.sqlClient.sql.query({ // TODO cursor
        format: this.config.queryFormat,
        body: {
          query: SqlString.format(query, values)
        }
      })).body;

      // INFO: cloud left in place for backward compatibility should use jdbc
      if (this.config.cloud || ['jdbc'].includes(this.config.queryFormat)) {
        const compiled = result.datarows.map(
          r =>  {
            // fix with alias should use jdbc 
            return result.schema.reduce((prev, cur, idx) => ({ ...prev, [cur.alias]: r[idx] }), {})
          }
        );
        return compiled;
      }
      return result && result.aggregations && this.traverseAggregations(result.aggregations);
    } catch (e) {
      if (e.body) {
        throw new Error(JSON.stringify(e.body, null, 2));
      }

      throw e;
    }
  }

  traverseAggregations(aggregations) {
    const fields = Object.keys(aggregations).filter(k => k !== 'key' && k !== 'doc_count');
    if (fields.find(f => aggregations[f].hasOwnProperty('value'))) {
      return [fields.map(f => ({ [f]: aggregations[f].value })).reduce((a, b) => ({ ...a, ...b }))];
    }
    if (fields.length === 0) {
      return [{}];
    }
    if (fields.length !== 1) {
      throw new Error(`Unexpected multiple fields at ${fields.join(', ')}`);
    }
    const dimension = fields[0];
    if (!aggregations[dimension].buckets) {
      throw new Error(`Expecting buckets at dimension ${dimension}: ${aggregations[dimension]}`);
    }
    return aggregations[dimension].buckets.map(b => this.traverseAggregations(b).map(
      innerRow => ({ ...innerRow, [dimension]: b.key })
    )).reduce((a, b) => a.concat(b), []);
  }

  async tablesSchema() {
    const indices = await this.client.cat.indices({
      format: 'json'
    });

    const schema = (await Promise.all(indices.body.map(async i => {
      const props = (await this.client.indices.getMapping({ index: i.index })).body[i.index].mappings.properties || {};
      return {
        [i.index]: Object.keys(props).map(p => ({ name: p, type: props[p].type })).filter(c => !!c.type)
      };
    }))).reduce((a, b) => ({ ...a, ...b }));
    // bug fix playground schema generate
    return {
      main:schema
    };
  }
}

module.exports = OpendistroDriver;
