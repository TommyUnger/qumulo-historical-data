# Historical Qumulo API to Sqlite Db

A python + sqlite app for storing and reporting on historical Qumulo API data.

### Current features

1. Set up cron jobs jobs to record historical data
2. Pull data from multiple clusters
3. Pull and store activity (IOPS and throughput) by client and path. Updated every two minutes, aggregated hourly.