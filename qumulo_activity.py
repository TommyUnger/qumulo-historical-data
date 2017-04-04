import os
import re
import sys
import json
import time
import subprocess
from qumulo_activity_sqlite import QumuloActivitySqlite
from qumulo_activity_tree import Node, NodeHelper

full_path = os.path.abspath(__file__)
working_directory = os.path.dirname(full_path)

def setup_cron():
    import qumulo
    python_path = "export PYTHONPATH=$PYTHONPATH:" + re.sub('/qumulo$', '', os.path.dirname(qumulo.__file__))
    cron_cmd = "*/2 * * * * %s; cd %s; python %s get_data >> log_qumulo_activity.txt 2>&1" % (python_path, working_directory, __file__)
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
        print("Please specify an argument (add_cron or get_data).")
        sys.exit()
    cmd = sys.argv[1]

    if not os.path.exists(config_file):
        print('Config file (%s) not found in: %s' % (config_file, os.getcwd()))
        sys.exit()

    with open(config_file) as data_file:
        config = json.load(data_file)

    if cmd == 'add_cron':
        print("Add data pull to crontab. Will run every 2 minutes.")
        setup_cron()
    elif cmd == 'get_data':
        print("Pulling data at time: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"),))
        for cluster in config['clusters']:
            db_path = config['sqlite_directory']
            if config['sqlite_directory'][0] == '.':
                db_path = working_directory + '/' + config['sqlite_directory']
            q_activity = QumuloActivitySqlite(cluster, db_path)
            q_activity.create_db()
            start_time = time.time()
            tree = Node({'name':'/'})
            activity = q_activity.get_activity()
            for i, entry in enumerate(activity['data']):
                if entry['id'] in activity['inode_paths']:
                    NodeHelper.add_path_and_data_to_tree(tree, activity['inode_paths'][entry['id']].split('/'), entry)
            q_activity.process_data(tree)
            print("%5s seconds for: %s" % (round(time.time() - start_time, 2), cluster['cluster']))


if __name__ == "__main__":
    main()

