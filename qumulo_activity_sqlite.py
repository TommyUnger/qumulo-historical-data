import os
import ssl
import math
import time
import sqlite3
from collections import OrderedDict
from qumulo.rest_client import RestClient
from qumulo_activity_tree import NodeHelper

ssl._create_default_https_context = ssl._create_unverified_context

class QumuloActivitySqlite(object):
    rc = None
    cluster = None
    db_path = None

    def __init__(self, cluster, db_path):
        self.cluster = cluster
        self.db_path = db_path
        self.rc = RestClient(cluster['cluster'], 8000)
        self.rc.login(cluster['user'], cluster['pass'])


    def get_db(self):
        return '%s/qumulo-hourly-activity-%s.db' % (self.db_path, self.cluster['cluster'])


    def get_activity(self):
        activity = self.rc.analytics.current_activity_get()

        inode_ids = {}
        for entry in activity['entries']:
            if entry['id'] not in inode_ids:
                inode_ids[entry['id']] = 1
            else:
                inode_ids[entry['id']] += 1

        inode_ids = inode_ids.keys()
        lookup_count = len(inode_ids)
        found_count = 0
        inode_paths = {}
        while len(inode_ids) > 0:
            path_ids = self.rc.fs.resolve_paths(inode_ids[:500])
            for inode_path in path_ids:
                inode_paths[inode_path['id']] = inode_path['path']
                if inode_path['path'] != '':
                    found_count += 1
            del inode_ids[:500]

        return {"data": activity['entries'], "inode_paths":inode_paths}


    def process_data(self, tree):
        result_count = 0
        cn = sqlite3.connect(self.get_db())
        cur = cn.cursor()
        ts = int(math.floor(time.time() / 3600) * 3600)
        cur.execute('SELECT * FROM iops_tput_path_hour WHERE ts=?', (ts, ))
        col_names = list(map(lambda x: x[0], cur.description))
        all_data = {}
        for current_data in cur.fetchall():
            all_data[current_data[2]] = OrderedDict()
            for i, col_name in enumerate(col_names):
                all_data[current_data[2]][col_name] = current_data[i]

        for new_data in NodeHelper.walk_tree(ts, '', tree, max_level=4):
            if new_data['path'] in all_data:
                for i, col_name in enumerate(col_names):
                    if i > 3:
                        all_data[new_data['path']]['ts_latest'] = time.time()
                        all_data[new_data['path']][col_name] += new_data[col_name]
                pass
            else:
                all_data[new_data['path']] = new_data

        insert_data = []
        for data in all_data.values():
            insert_data.append(data.values())

        cn.execute("DELETE FROM iops_tput_path_hour WHERE ts=?", (ts, ))
        cn.executemany("insert into iops_tput_path_hour values (?,?,?,?,?,?,?,?,?,?,?)", insert_data)
        cn.commit()
        cn.close()


    def create_db(self):
        file_name = self.get_db()
        if os.path.exists(file_name):
            return
        cn = sqlite3.connect(file_name)
        cur = cn.cursor()
        cur.execute('''CREATE TABLE iops_tput_path_hour
                     (
                     ts INT,
                     ts_latest REAL,
                     path TEXT,
                     level INTEGER,
                     count INTEGER,
                     read_data INTEGER,
                     write_data INTEGER,
                     total_data INTEGER,
                     read_iops INTEGER,
                     write_iops INTEGER,
                     total_iops INTEGER
                     )''')
        cn.commit()
        cn.close()
