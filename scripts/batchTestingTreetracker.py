# Batch testing treeTracker
#
# Usage:
#   batchTestingTreetracker.py -t [time in seconds]
#
# The script will run `mvn clean install` repeatedly for [time in seconds] long. For each run, a unique file
# directory with run number is created. The default format is [run number] (e.g., 1, 2, etc). If the maven process
# exists without 1, the script will name the file directory as [run_error] (e.g., 1_error, 2_error, etc).
# The log files can be synchronized with local via mutagen.
import os
import shutil
import sys, getopt
import time
import subprocess

SOURCE_CODE_PATH = "/home/ubuntu/challenge-set/treeTracker"
SOURCE_LOG_PATH = "/home/ubuntu/challenge-set/treeTracker/log"
RUN_LOGS = "/home/ubuntu/run-logs"


def main(argv):
    run_time = 0
    try:
        opts, args = getopt.getopt(argv, "ht:")
    except getopt.GetoptError:
        print('batchTestingTreetracker.py -t <time in seconds>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('batchTestingTreetracker.py -t <time in seconds>')
            sys.exit()
        elif opt == '-t':
            run_time = arg
    print(f'set time is {run_time} seconds')
    run(run_time)


def execute_process(num_run):
    os.chdir(SOURCE_CODE_PATH)
    log_path = os.path.join(RUN_LOGS, str(num_run))
    try:
        subprocess.run("~/apache-maven-3.8.1/bin/mvn clean install", shell=True, check=True)
    except subprocess.CalledProcessError:
        log_path = os.path.join(RUN_LOGS, str(num_run) + "error")
    os.makedirs(log_path, exist_ok=True)
    copytree(SOURCE_LOG_PATH, log_path)
    for filename in os.listdir(SOURCE_LOG_PATH):
        file_path = os.path.join(SOURCE_LOG_PATH, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


def run(run_time):
    num_run = 1
    start = time.time()
    print(f'start: {start}')
    while True:
        execute_process(num_run)
        later = time.time()
        print(f'later: {later}')
        print('diff: ' + str(int(later - start)))
        if int(later - start) > int(run_time):
            print(f'mvn clean install has been executed {num_run}')
            sys.exit(0)
        num_run += 1


if __name__ == '__main__':
    main(sys.argv[1:])
