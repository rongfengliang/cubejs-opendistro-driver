const { BaseQuery,BaseFilter } = require('@cubejs-backend/schema-compiler');
const  R  =require('ramda');


const GRANULARITY_TO_INTERVAL = {
  day: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd')`,
  // eslint-disable-next-line no-unused-vars,@typescript-eslint/no-unused-vars
  week: (date) => `Week(${date})`, // TODO
  hour: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH')`,
  minute: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm')`,
  second: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:ss')`,
  month: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-01')`,
  year: (date) => `DATE_FORMAT(${date}, 'yyyy-01-01')`
};


class OpendistroQueryFilter extends BaseFilter {
  likeIgnoreCase(column, not, param) {
    return `${not ? ' NOT' : ''} MATCH_QUERY(${column}, ${this.allocateParam(param)})`;
  }
}

module.exports = class OpendistroQuery extends BaseQuery {
  newFilter(filter) {
    return new OpendistroQueryFilter(this, filter);
  }

  convertTz(field) {
    return `${field}`; // TODO
  }

  timeStampCast(value) {
    return `${value}`;
  }

  dateTimeCast(value) {
    return `${value}`; // TODO
  }

  subtractInterval(date, interval) {
    // TODO: Test this, note sure how value gets populated here
    return `${date} - INTERVAL ${interval}`;
  }

  addInterval(date, interval) {
    // TODO: Test this, note sure how value gets populated here
    return `${date} + INTERVAL ${interval}`;
  }

  timeGroupedColumn(granularity, dimension) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  unixTimestampSql() {
    // with fix size  should fix
    return `1617184163`;
  }

  groupByClause() {
    if (this.ungrouped) {
      return '';
    }
    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns = R.flatten(
      dimensionsForSelect.map(s => s.selectColumns() && s.dimensionSql())
    ).filter(s => !!s);

    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  orderHashToString(hash) {
    if (!hash || !hash.id) {
      return null;
    }

    const fieldAlias = this.getFieldAlias(hash.id);

    if (fieldAlias === null) {
      return null;
    }

    const direction = hash.desc ? 'DESC' : 'ASC';
    return `${fieldAlias} ${direction}`;
  }

  getFieldAlias(id) {
    const equalIgnoreCase = (a, b) => typeof a === 'string' &&
      typeof b === 'string' &&
      a.toUpperCase() === b.toUpperCase();

    let field;

    field = this.dimensionsForSelect().find(d => equalIgnoreCase(d.dimension, id));

    if (field) {
      return field.dimensionSql();
    }

    field = this.measures.find(
      d => equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id)
    );

    if (field) {
      return field.aliasName(); // TODO isn't supported
    }

    return null;
  }

  escapeColumnName(name) {
    return `${name}`; // TODO
  }
}