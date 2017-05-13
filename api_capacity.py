import time
import re
import sys
import utils
import sqlite3

class ApiCapacity(object):
    db = None

    def __init__(self, db):
        self.db = db

    def get_capacity_sampled(self):
        start_time = time.time()
        samples = self.db.rc.fs.get_file_samples("/", 20, "capacity")
        data = []
        for d in samples:
            try:
                attrs = self.db.rc.fs.get_attr(d['id'])
                row = [
                    start_time,
                    attrs['id'],
                    attrs['path'][:512],
                    attrs['name'][:512],
                    utils.date_to_ts(attrs['creation_time']),
                    utils.date_to_ts(attrs['modification_time']),
                    utils.date_to_ts(attrs['change_time']),
                    attrs['size'],
                    attrs['owner'],
                    attrs['group'],
                    attrs['owner_details']['id_type'],
                    attrs['owner_details']['id_value'],
                    attrs['group_details']['id_type'],
                    attrs['group_details']['id_value'],
                    attrs['mode'],
                ]
                data.append(row)
            except:
                print("Exception adding capacity samples")
                print(sys.exc_info())
        cn = sqlite3.connect(self.db.get_db())
        cn.executemany("insert into capacity_sample values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", data)
        cn.commit()
        cn.close()


    def create_tables(self):
        capacity_sample_cols = """
                        ts INT,
                        inode_id INT,
                        path TEXT,
                        name TEXT,
                        creation_ts INT,
                        modification_ts INT,
                        change_ts INT,
                        size_bytes INT,
                        owner_id INT,
                        group_id INT,
                        owner_detail_type TEXT,
                        owner_detail_id TEXT,
                        group_detail_type TEXT,
                        group_detail_id TEXT,
                        perm_mode TEXT
                        """
        self.db.create_table("capacity_sample", capacity_sample_cols)
