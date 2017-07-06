# Historical Qumulo API to Sqlite Db

A python + sqlite app for storing and reporting on historical Qumulo API data.

### Current features

1. Set up cron jobs jobs to record historical data
2. Pull data from multiple clusters
3. Pull and store activity (IOPS and throughput) by client and path. Updated every two minutes, aggregated hourly.

To install and run:

0. Set up a python virtual environment
1. run `pip install -r requirements.txt`
2. Set up your config.json with your cluster(s) Qumulo API credentials. And *absolute* destination path for your sqlite database(s).
3. run `python historical_data.py add_cron`
4. After you've been collecting data for a few hours, or a few days, or more. 
    a. run `jupyter-notebook`
    b. Open the URL output by jupyter-notebook in your browser.
    c. Navigate to notebooks/Capacity_with_activity.ipynb
    d. Run each of the code blocks (press "Shift-Enter"). Note that you'll need to change the last block to specify your cluster name.
