import subprocess
import sys
import os
import signal
import time

FIRST="ip-172-31-4-97"
SECOND="ip-172-31-3-19"
THIRD="ip-172-31-12-49"

#only  at start
#trigger clean
binarypath = "/home/ubuntu/ravi/crdb-baseline/"
binary = "cockroach"
clean_cmd="rm " + binarypath +"cockroach-data -rf"
stop_crdb_cmd = binarypath + binary + " quit --host="
start_crdb_main = binarypath + binary + " start --insecure --background --host="
start_crdb_others = binarypath + binary + " start --insecure --join=%s:26257 --background --host=" % FIRST 


def execute_cmd_in_remote(host, cmd):
  # Ports are handled in ~/.ssh/config since we use OpenSSH
  COMMAND = cmd
  HOST = host

  CMD = ["ssh", "-i", "/home/ubuntu/ravi/ravi.pem", "%s@%s" % ("ubuntu",HOST), cmd]
  
  ssh = subprocess.Popen(CMD,
                       shell=False,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
  
  """result = ssh.stdout.readlines()
  if result == []:
    error = ssh.stderr.readlines()
    print >>sys.stderr, "ERROR: %s" % error
  else:
    print result"""



def clean_all_hosts():
  os.system(clean_cmd)
  execute_cmd_in_remote(SECOND,clean_cmd)
  execute_cmd_in_remote(THIRD,clean_cmd)


def stop_crdb_on_all_hosts():
  os.system(stop_crdb_cmd+FIRST)
  execute_cmd_in_remote(SECOND, stop_crdb_cmd+SECOND)
  execute_cmd_in_remote(THIRD, stop_crdb_cmd+THIRD)


def start_crdb_on_all_hosts():
  os.system(start_crdb_main+FIRST)
  execute_cmd_in_remote(SECOND, start_crdb_others+SECOND)
  execute_cmd_in_remote(THIRD, start_crdb_others+THIRD)

def run_local_task(cmd, time_to_sleep):
  pro = subprocess.Popen(cmd, stdout=subprocess.PIPE, 
                       shell=True, preexec_fn=os.setsid) 
  time.sleep(time_to_sleep)
  os.killpg(pro.pid, signal.SIGTERM)  # Send the signal to all the process groups

def concurrency_experiment():
  start=5 
  end=4
  step=5 
  while( start <= 25 ):
    arg0 = binarypath +"concurrency"
    arg1 =" -start=" + str(start)
    arg2 =" -max=" + str(end)
    arg3 =" -step=" + str(step)
    arg4 =" 2>> concurrency1"
    cmd = arg0 + arg1 + arg2 + arg3 + arg4
    time_to_sleep = 600
     
    for each in range(0,3):
        stop_crdb_on_all_hosts()
        time.sleep(3)
        start_crdb_on_all_hosts()
        time.sleep(2)      
        run_local_task(cmd,get_sleep_time(start))
       
    start += step
    end += step

  print count


def get_sleep_time(concurrency):
  if concurrency <= 25:
    return 600
  else concurrency <= 100:
    return 400
  else:
    return 250

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
    for each in range(0,3):
        stop_crdb_on_all_hosts()
        time.sleep(3)
        start_crdb_on_all_hosts()
        time.sleep(2)      
        run_local_task(cmd,time_to_sleep)

    contention1 += 10
    contention2 -= 10






#clean_all_hosts()
#start_crdb_on_all_hosts()
#time.sleep(5)
#stop_crdb_on_all_hosts()
#contention_experiment()
concurrency_experiment()













