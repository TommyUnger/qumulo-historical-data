import os
import re
import sys
import json
import time
import subprocess
from qumulo_api_sqlite import QumuloApiSqlite
from activity_tree import Node, NodeHelper
from api_activity import ApiActivity

full_path = os.path.abspath(__file__)
working_directory = os.path.dirname(full_path)

def setup_cron():
    import qumulo
    python_path = "export PYTHONPATH=$PYTHONPATH:" + re.sub('/qumulo$', '', os.path.dirname(qumulo.__file__))
    cron_cmd = "*/2 * * * * %s; cd %s; python %s get_activity_data >> log_get_activity_data.txt 2>&1" % (python_path, working_directory, __file__)
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


def main():
    try:
        import qumulo
    except:
        print("Unable to find Qumulo python API bindings")
        sys.exit()

    config_file = 'config.json'
    if len(sys.argv) < 2:
        print("Please specify an argument (add_cron or get_activity_data).")
        sys.exit()
    cmd = sys.argv[1]

    if not os.path.exists(config_file):
        print('Config file (%s) not found in: %s' % (config_file, os.getcwd()))
        sys.exit()

    with open(config_file) as data_file:
        config = json.load(data_file)

    db_path = config['sqlite_directory']
    if config['sqlite_directory'][0] == '.':
        db_path = working_directory + '/' + config['sqlite_directory']

    if cmd == 'create_db':
        for cluster in config['clusters']:
            q_api = QumuloApiSqlite(cluster, db_path)
            q_api.setup_tables()
    elif cmd == 'add_cron':
        print("Add data pull to crontab. Will run every 2 minutes.")
        setup_cron()
    elif cmd == 'get_activity_data':
        print("Pulling data at time: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"),))
        for cluster in config['clusters']:
            q_api = QumuloApiSqlite(cluster, db_path)
            q_api.setup_tables()
            start_time = time.time()
            tree = Node({'name':'/'})
            q_activity = ApiActivity(q_api)
            activity = q_activity.get_activity()
            for i, entry in enumerate(activity['data']):
                if entry['id'] in activity['inode_paths']:
                    NodeHelper.add_path_and_data_to_tree(tree, activity['inode_paths'][entry['id']].split('/'), entry)
            q_activity.process_path_data(tree)
            tree = Node({'name':'/'})
            for i, entry in enumerate(activity['data']):
                if entry['id'] in activity['inode_paths']:
                    parts = ['', entry['ip']] + activity['inode_paths'][entry['id']].split('/')[1:]
                    NodeHelper.add_path_and_data_to_tree(tree, parts, entry)
            q_activity.process_client_ip_data(tree)
            print("%5s seconds for: %s" % (round(time.time() - start_time, 2), cluster['cluster']))


if __name__ == "__main__":
    main()

