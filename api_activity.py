import sqlite3
import math
import re
import time
from collections import OrderedDict
from activity_tree import NodeHelper


class ApiActivity(object):
    db = None

    def __init__(self, db):
        self.db = db


    # api wrappers
    def get_activity(self):
        activity = self.db.rc.analytics.current_activity_get()

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
            path_ids = self.db.rc.fs.resolve_paths(inode_ids[:400])
            for inode_path in path_ids:
                inode_paths[inode_path['id']] = inode_path['path']
                if inode_path['path'] != '':
                    found_count += 1
            del inode_ids[:400]

        return {"data": activity['entries'], "inode_paths":inode_paths}


    def process_path_data(self, tree):
        result_count = 0
        cn = sqlite3.connect(self.db.get_db())
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


    def process_client_ip_data(self, tree):
        cn = sqlite3.connect(self.db.get_db())
        cur = cn.cursor()
        ts = int(math.floor(time.time() / 3600) * 3600)
        cur.execute('SELECT * FROM iops_tput_client_ip_hour WHERE ts=?', (ts, ))
        col_names = list(map(lambda x: x[0], cur.description))
        all_data = {}
        for current_data in cur.fetchall():
            all_data[current_data[2]] = OrderedDict()
            for i, col_name in enumerate(col_names):
                all_data[current_data[2]][col_name] = current_data[i]
        ip_data = {}
        for new_data in NodeHelper.walk_tree(ts, '', tree, max_level=5):
            if new_data['level'] == 2:
                ip_data[new_data['path']] = new_data
            elif new_data['level'] > 2:
                ip = re.sub(r'(/[^/]+).*', r'\1', new_data['path'])
                if new_data['total_data'] >= ip_data[ip]['total_data']*0.5 or "busiest_path" not in ip_data[ip]:
                    ip_data[ip]["busiest_path"] = [re.sub(r'/[^/]+(.*)', r'\1', new_data['path']), new_data['total_data']]        
        for k in ip_data:
            ip = k[1:]
            d = ip_data[k]
            if ip not in all_data:
                all_data[ip] = OrderedDict([
                                    ('ts', d['ts']),
                                    ('ts_latest', d['ts']),
                                    ('client_ip', ip),
                                    ('busiest_path', str(d['busiest_path'][0])),
                                    ('busiest_path_amount', str(d['busiest_path'][1]))
                                ])
                if d['busiest_path'][1] > all_data[ip]['busiest_path_amount']:
                    all_data[ip]['busiest_path_amount'] = d['busiest_path'][1]
                    all_data[ip]['busiest_path'] = d['busiest_path'][0]
            for i, col_name in enumerate(col_names):
                if i > 4:
                    all_data[ip]['ts_latest'] = time.time()
                    if col_name not in all_data[ip]:
                        all_data[ip][col_name] = 0
                    all_data[ip][col_name] += d[col_name]

        insert_data = []
        for data in all_data.values():
            insert_data.append(data.values())
        cn.execute("DELETE FROM iops_tput_client_ip_hour WHERE ts=?", (ts, ))
        cn.executemany("insert into iops_tput_client_ip_hour values (?,?,?,?,?,?,?,?,?,?,?,?)", insert_data)
        cn.commit()
        cn.close()


    def create_tables(self):
        iops_tput_path_hour_cols = """
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
                """
        self.db.create_table("iops_tput_path_hour", iops_tput_path_hour_cols)

        iops_tput_client_ip_hour_cols = """
                 ts INT,
                 ts_latest REAL,
                 client_ip TEXT,
                 busiest_path TEXT,
                 busiest_path_amount INTEGER,
                 count INTEGER,
                 read_data INTEGER,
                 write_data INTEGER,
                 total_data INTEGER,
                 read_iops INTEGER,
                 write_iops INTEGER,
                 total_iops INTEGER
                """
        self.db.create_table("iops_tput_client_ip_hour", iops_tput_client_ip_hour_cols)
