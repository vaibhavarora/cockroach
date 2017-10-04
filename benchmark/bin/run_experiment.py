import subprocess
import sys
import os
import signal
import time

FIRST="128.111.44.237"
SECOND="128.111.44.163"
THIRD="128.111.44.167"

#only  at start
#trigger clean
binarypath = "$GOPATH/"
binary = "cockroach"
clean_cmd="rm cockroach-data -rf"
stop_crdb_cmd =  binary + " quit --insecure --host="
stop_crdb_cmd_main =  binary + " quit --insecure"
start_crdb_main = binary + " start --insecure --background --advertise-host="
start_crdb_others = binary + " start --insecure --join=%s:26257 --background --host=" % FIRST 
run_exp_cmd = "go run main.go "

confpath = "../conf/"

def execute_cmd_in_remote(host, cmd):
  # Ports are handled in ~/.ssh/config since we use OpenSSH
  COMMAND = cmd
  HOST = host

  CMD = ["ssh", "%s@%s" % ("migr",HOST), cmd]
  
  ssh = subprocess.Popen(CMD,
                      shell=False,
                      stdout=subprocess.PIPE,
                      stderr=subprocess.PIPE)

def clean_all_hosts():
  os.system(clean_cmd)
  execute_cmd_in_remote(SECOND,clean_cmd)
  execute_cmd_in_remote(THIRD,clean_cmd)


def stop_crdb_on_all_hosts():
  os.system(stop_crdb_cmd_main)
  execute_cmd_in_remote(SECOND, stop_crdb_cmd+SECOND)
  execute_cmd_in_remote(THIRD, stop_crdb_cmd+THIRD)


def start_crdb_on_all_hosts():
  os.system(start_crdb_main+FIRST)
  execute_cmd_in_remote(SECOND, start_crdb_others+SECOND)
  execute_cmd_in_remote(THIRD, start_crdb_others+THIRD)
  print "Started db on all servers"


def run_local_task(cmd, time_to_sleep):
  p = subprocess.Popen(cmd, stdout=subprocess.PIPE, 
                        shell=True, preexec_fn=os.setsid) 
  p.communicate()


def run_experiment():
  
  subdirs = [x[0] for x in os.walk(confpath)]
  for eachDir in subdirs:
    path = eachDir 
    confFiles = [f for f in os.listdir(path) if 'conf.json.' in f]
    for confFile in confFiles:
      cmd = run_exp_cmd + path + "/" + confFile
      print cmd
      time_to_sleep = 1800
      for each in range(0,3):
        stop_crdb_on_all_hosts()
        time.sleep(3)
        start_crdb_on_all_hosts()
        time.sleep(2)      
        run_local_task(cmd, time_to_sleep)
        
run_experiment()