var _ = require('lodash'),
  async = require('async'),
  AWS = require('aws-sdk'),
  cloudwatch = new AWS.CloudWatch(),
  dynamodb = new AWS.DynamoDB(),
  lynx = require('lynx'),
  lynxInstance = undefined;

var TABLES_UPDATE_PERIOD = 1000 * 60 * 60; //Every hour;

var tables = [];
var lastTablesUpdateTime;
var metricNames = ['WriteThrottleEvents','ReadThrottleEvents'];

function metrics() {
  if (!lynxInstance) {
    lynxInstance = new lynx(process.env.STATSD_URL, 8125, {
      on_error: function(a, b) {
        console.log(a, b);
      }
    });
  }
  return lynxInstance;
}

function bucket(table, metric) {
  return ['dynamodb', metric.toLowerCase(), table].join('.');
}

function processTable(table, done) {  

  function processMetric(metric, cb) {
    var params = {
      EndTime: new Date().toISOString(),
      MetricName: metric,
      Namespace: 'AWS/DynamoDB',
      Period: 300,
      StartTime: new Date(new Date().getTime() - 60 * 60 * 1000).toISOString(),
      Statistics: ['Sum'],
      Dimensions: [{
        Name: 'TableName',
        Value: table
      }, ],
      Unit: 'Count'
    };

    return cloudwatch.getMetricStatistics(params, function(e, data) {
      if (e) {
        console.error('error', e);
        return cb(e);
      }
      var sum = 0;
      var last = _(data.Datapoints)
        .sortBy('Timestamp')
        .last();
      if (last) {
        sum = +(last.Sum);
      }
      var b = bucket(table, metric);
      if (true || sum > 0) {
        console.log(b, sum);
      }

      metrics().gauge(b, sum);
      return cb(null, sum);
    });
  }

  return async.map(metricNames, processMetric, done);
};

function processAll() {
  if (((new Date().getTime()) - lastTablesUpdateTime) >= TABLES_UPDATE_PERIOD) {
    return updateTables();
  }

  console.log('Processing...');

  return async.map(tables, processTable, function(e, results) {
    if (e) {
      console.error(e);
    }
    var total = _.reduce(results, function(sum, num) {return _.zipWith(sum, num, function (v1, v2) { return v1 + v2 })}, [0, 0]);

    _.zipWith(metricNames, total, function (name, value) { console.log(bucket('all', name), value) });
    _.zipWith(metricNames, total, function (name, value) { metrics().gauge(bucket('all', name), value) });

    var allMetricTotal = _.reduce(total, function(sum, num) {return sum + +num;}, 0);
    console.log(bucket('all', 'throttleevents'), allMetricTotal);
    metrics().gauge(bucket('all', 'throttleevents'), allMetricTotal);
    setTimeout(processAll, 60000);
  });
}

function updateTables () {
  console.log('Updating table list');
  dynamodb.listTables(function(e, data) {
    lastTablesUpdateTime = new Date().getTime();
    var prefix = process.env.NOOSE_PREFIX || '';
    tables = _.filter(data.TableNames, function(name) {
      return name.substr(0, prefix.length) === prefix;
    });
    console.log('Tables: ', tables);

    processAll();
  });
}

updateTables();
