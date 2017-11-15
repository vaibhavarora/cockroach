import subprocess
import sys
import os
import signal
import time

FIRST="128.111.44.163"
SECOND="128.111.44.241"
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
  os.system(stop_crdb_cmd_main)
  time.sleep(2)
  os.system(start_crdb_main+FIRST)
  time.sleep(3)
  execute_cmd_in_remote(SECOND, stop_crdb_cmd+SECOND)
  time.sleep(2)
  execute_cmd_in_remote(SECOND, start_crdb_others+SECOND)
  time.sleep(3)
  execute_cmd_in_remote(THIRD, stop_crdb_cmd+THIRD)
  time.sleep(2)
  execute_cmd_in_remote(THIRD, start_crdb_others+THIRD)
  time.sleep(3)
  print "Started db on all servers"


def run_local_task(cmd):
  p = subprocess.Popen(cmd, stdout=subprocess.PIPE, 
                        shell=True, preexec_fn=os.setsid) 
  p.communicate()


def run_experiment():
  
  # subdirs = [x[0] for x in os.walk(confpath)]
  # for eachDir in subdirs:
    path = "../conf/contentionRatio" 
    confFiles = [ "conf.json.contentionRatio.50",\
              "conf.json.contentionRatio.60", "conf.json.contentionRatio.70", "conf.json.contentionRatio.80", "conf.json.contentionRatio.90"]
    for confFile in confFiles:
      cmd = run_exp_cmd + path + "/" + confFile
      print cmd
      for each in range(0,3):
        #start_crdb_on_all_hosts()
        time.sleep(5)      
        run_local_task(cmd)
        
run_experiment()