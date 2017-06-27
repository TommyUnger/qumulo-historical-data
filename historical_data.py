import os
import re
import sys
import json
import time
import subprocess
import traceback
from qumulo_api_sqlite import QumuloApiSqlite
from activity_tree import Node, NodeHelper
from api_activity import ApiActivity
from api_capacity import ApiCapacity
from multiprocessing import Pool, Lock


full_path = os.path.abspath(__file__)
working_directory = os.path.dirname(full_path)

def setup_cron():
    import qumulo
    python_path = "export PYTHONPATH=$PYTHONPATH:" + re.sub('/qumulo$', '', os.path.dirname(qumulo.__file__))
    cron_cmd = "*/2 * * * * %s; cd %s; python %s get_activity_data get_capacity_data >> log_get_activity_data.txt 2>&1" % (python_path, working_directory, __file__)
    cron_data = subprocess.check_output("crontab -l", shell=True)
    crons = cron_data.split("\n")
    cronjob_exists = False
    for cron in crons:
        if len(cron) > 0 and cron[0] != '#' and __file__ in cron:
            cronjob_exists = cron
    if not cronjob_exists:
        new_cron_cmd = "(crontab -l ; echo \"%s\") | crontab -" % (cron_cmd, )
        print("Adding crontab via: %s" % (new_cron_cmd, ))
        os.system(new_cron_cmd)
    else:
        print("Crontab is already installed: %s" % (cronjob_exists,))


def init(l):
    global lock
    lock = l


def get_activity_data(cluster, db_path):
    try:
        start_time = time.time()
        q_api = QumuloApiSqlite(cluster, db_path)
        q_activity = ApiActivity(q_api)
        activity = q_activity.get_activity()

        # paths        
        tree = Node({'name':'/'})
        for i, entry in enumerate(activity['data']):
            inode_id = int(entry['id'])
            if inode_id in activity['inode_paths']:
                NodeHelper.add_path_and_data_to_tree(tree, activity['inode_paths'][inode_id].split('/'), entry)
        q_activity.process_path_data(tree)

        # client ip addresses
        tree = Node({'name':'/'})
        for i, entry in enumerate(activity['data']):
            inode_id = int(entry['id'])
            if inode_id in activity['inode_paths']:
                parts = ['', entry['ip']] + activity['inode_paths'][inode_id].split('/')[1:]
                NodeHelper.add_path_and_data_to_tree(tree, parts, entry)
        q_activity.process_client_ip_data(tree)

        lock.acquire()
        print("%.2f seconds for cluster activity: %s" % (round(time.time() - start_time, 2), cluster['cluster']))
        lock.release()
    except Exception, e:
        type_, value_, traceback_ = sys.exc_info()
        lock.acquire()
        print("*********************          Data/API Exception          *********************")
        print("*   %-72s   *" % ("Unable to get activity data",))
        print("*   Cluster: %(cluster)-20s,    user: %(user)-32s   *" % cluster)
        print("*   %-72s   *" % (type_,))
        print("*   %-72s   *" % (value_,))
        print("*" * 80)
        print("")
        lock.release()


def get_capacity_data(cluster, db_path):
    try:
        start_time = time.time()
        q_api = QumuloApiSqlite(cluster, db_path)
        q_capacity = ApiCapacity(q_api)
        q_capacity.get_capacity_sampled()
        lock.acquire()
        print("%.2f seconds for cluster capacity: %s" % (round(time.time() - start_time, 2), cluster['cluster']))
        lock.release()
    except Exception, e:
        type_, value_, traceback_ = sys.exc_info()
        lock.acquire()
        print("*********************          Data/API Exception          *********************")
        print("*   %-72s   *" % ("Unable to get activity data",))
        print("*   Cluster: %(cluster)-20s,    user: %(user)-32s   *" % cluster)
        print("*   %-72s   *" % (type_,))
        print("*   %-72s   *" % (value_,))
        print("*" * 80)
        print("")
        lock.release()


def main():
    l = Lock()
    try:
        import qumulo
    except:
        print("Unable to find Qumulo python API bindings")
        sys.exit()

    config_file = 'config.json'
    if len(sys.argv) < 2:
        print("Please specify an argument (add_cron, get_activity_data, or get_capacity_data).")
        sys.exit()
    cmds = sys.argv[1:]

    if not os.path.exists(config_file):
        print('Config file (%s) not found in: %s' % (config_file, os.getcwd()))
        sys.exit()

    with open(config_file) as data_file:
        config = json.load(data_file)

    db_path = config['sqlite_directory']
    if config['sqlite_directory'][0] == '.':
        db_path = working_directory + '/' + config['sqlite_directory']

    if 'create_db' in cmds:
        for cluster in config['clusters']:
            q_api = QumuloApiSqlite(cluster, db_path)
    elif 'add_cron' in cmds:
        print("Add data pull to crontab. Will run every 2 minutes.")
        setup_cron()
    else:
        results = []
        pool = Pool(processes=4, initializer=init, initargs=(l,)) 
        if 'get_activity_data' in cmds:
            print("Pulling activity data at time: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"),))
            for cluster in config['clusters']:
                results.append(pool.apply_async(get_activity_data, (cluster, db_path)))

        if 'get_capacity_data' in cmds:
            print("Pulling capacity data at time: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"),))
            for cluster in config['clusters']:
                results.append(pool.apply_async(get_capacity_data, (cluster, db_path)))

        for result in results:
            result.get(timeout=200)



if __name__ == "__main__":
    main()

