import subprocess
import sys
import os
import signal
import time

GEDIZ="gediz"
EUPHRATES="euphrates"
RUBICON="rubicon"

#only  at start
#trigger clean
clean_cmd="rm /home/migr/tcoder/cockroachDB/cockroach-data -rf"
stop_crdb_cmd ="/home/migr/tcoder/cockroachDB/cockroach.20170124 quit --host="
start_crdb_main="/home/migr/tcoder/cockroachDB/cockroach.20170124 start --background --insecure --host="
start_crdb_others="/home/migr/tcoder/cockroachDB/cockroach.20170124 start --insecure --join=gediz:26257 --background --host="


def execute_cmd_in_remote(host, cmd):
  # Ports are handled in ~/.ssh/config since we use OpenSSH
  COMMAND = cmd
  HOST = host
  ssh = subprocess.Popen(["ssh", "%s" % HOST, COMMAND],
                       shell=False,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)



def clean_all_hosts():
  os.system(clean_cmd)
  execute_cmd_in_remote(EUPHRATES,clean_cmd)
  execute_cmd_in_remote(RUBICON,clean_cmd)


def stop_crdb_on_all_hosts():
  os.system(stop_crdb_cmd+GEDIZ)
  execute_cmd_in_remote(EUPHRATES, stop_crdb_cmd+EUPHRATES)
  execute_cmd_in_remote(RUBICON, stop_crdb_cmd+RUBICON)


def start_crdb_on_all_hosts():
  os.system(start_crdb_main+GEDIZ)
  execute_cmd_in_remote(EUPHRATES, start_crdb_others+EUPHRATES)
  execute_cmd_in_remote(RUBICON, start_crdb_others+RUBICON)

def run_local_task(cmd, time_to_sleep):
  pro = subprocess.Popen(cmd, stdout=subprocess.PIPE, 
                       shell=True, preexec_fn=os.setsid) 
  time.sleep(time_to_sleep)
  os.killpg(pro.pid, signal.SIGTERM)  # Send the signal to all the process groups

def concurrency_experiment():

  start=1 
  end=5
  step=5 
  while( start <= 300 ):
    arg0 ="/home/migr/tcoder/benchmark/concurrency/concurrency"
    arg1 =" -start=" + str(start)
    arg2 =" -max=" + str(end)
    arg3 =" -step=" + str(step)
    arg4 =" 2>> concurrencynew"
    cmd = arg0 + arg1 + arg2 + arg3 + arg4
    time_to_sleep = 800 
    for each in range(0,5):
        stop_crdb_on_all_hosts()
        time.sleep(3)
        start_crdb_on_all_hosts()
        time.sleep(2)      
        run_local_task(cmd,time_to_sleep)

    start += step
    end += step



def contention_experiment():

  contention1 = 50
  contention2 = 50
  concurrency = 100
  while(contention1 <=90):
    ratio = str(contention1) + ":" + str(contention2)
    arg0 ="/home/migr/tcoder/benchmark/contention/contention "
    arg1 ="-contention-start="
    arg2 ="-contention-end="
    arg3 ="-concurrency=" + str(concurrency)
    arg4 =" 2>> contention100"
    cmd = arg0 + arg1 + ratio + " "+ arg2 + ratio + arg3 +arg4
    time_to_sleep = 800 
    for each in range(0,5):
        stop_crdb_on_all_hosts()
        time.sleep(3)
        start_crdb_on_all_hosts()
        time.sleep(2)      
        run_local_task(cmd,time_to_sleep)

    contention1 += 10
    contention2 -= 10





#stop_crdb_on_all_hosts()
#clean_all_hosts()
#start_crdb_on_all_hosts()
#contention_experiment()
concurrency_experiment()















