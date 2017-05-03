import time

class ApiCapacity(object):
    db = None

    def __init__(self, db):
        self.db = db


    def get_capacity_sampled(self):
        start_time = time.time()
        samples = self.db.rc.fs.get_file_samples("/", 200, "capacity")
        for d in samples:
            try:
                attrs = self.db.rc.fs.get_attr(d['id'])
            except:
                pass
        tt = time.time() - start_time
        print "Time taken: %s - %s" % (tt, tt / len(samples))


    def create_tables(self):
        # capacity details
        # iops_tput_path_hour_cols = """
        #          ts INT,
        #          ts_latest REAL,
        #         """
        # self.db.create_table("iops_tput_path_hour", iops_tput_path_hour_cols)
        pass
