var _ = require('lodash'),
  async = require('async'),
  AWS = require('aws-sdk'),
  cloudwatch = new AWS.CloudWatch(),
  dynamodb = new AWS.DynamoDB(),
  lynx = require('lynx'),
  lynxInstance = undefined;

var tables = [];

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
  var metricNames = ['ThrottledRequests'];

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
  console.log('processing...');

  return async.map(tables, processTable, function(e, results) {
    if (e) {
      console.error(e);         
    }

    var total = _.reduce(results, function(sum, num) {return sum + +num;}, 0); 
    
    metrics().gauge(bucket('all', 'throttleevents'), total);
    console.log(bucket('all', 'throttleevents'), total);
    setTimeout(processAll, 60000);
  });
}

dynamodb.listTables(function(e, data) {
  var prefix = process.env.NOOSE_PREFIX;
  tables = _.filter(data.TableNames, function(name) {
    return name.substr(0, prefix.length) === prefix;
  });
  
  processAll();
});
