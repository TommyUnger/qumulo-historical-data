import os
import re
import math
import time
import sqlite3
from qumulo.rest_client import RestClient
from api_activity import ApiActivity
from api_capacity import ApiCapacity


class QumuloApiSqlite(object):
    rc = None
    cluster = None
    db_path = None

    def __init__(self, cluster, db_path):
        seconds_in_day = 60 * 60 * 24
        self.cluster = cluster
        self.db_path = db_path
        self.rc = RestClient(cluster['cluster'], 8000)
        self.rc.login(cluster['user'], cluster['pass'])
        week_time = time.gmtime(math.floor((time.time()) / (seconds_in_day * 7)) * (seconds_in_day * 7) - seconds_in_day*4)
        ts_week = time.strftime("%Y-%m-%d", week_time)
        db_dir = '%s/%s' % (self.db_path, self.cluster['cluster'])
        if not os.path.exists(db_dir):
            os.mkdir(db_dir)
        self.db_path = '%s/%s-qumulo-api-data.db' % (db_dir, ts_week)
        self.setup_tables()


    def get_db(self):
        return self.db_path


    def create_table(self, name, cols):
        db_file_name = self.get_db()
        cn = sqlite3.connect(db_file_name)
        cur = cn.cursor()
        sql = """SELECT name FROM sqlite_master 
                WHERE type='table' 
                AND name='%s'""" % (name, )
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            cur.execute('''CREATE TABLE %s(%s)''' % (name, cols))
            cn.commit()
        cn.close()


    def setup_tables(self):
        q_activity = ApiActivity(self)
        q_activity.create_tables()
        q_capacity = ApiCapacity(self)
        q_capacity.create_tables()
