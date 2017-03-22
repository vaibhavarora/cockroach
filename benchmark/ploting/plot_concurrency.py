import matplotlib.pyplot as plt
import numpy as np

concurrency = []
trans_rate = []
retries = []
avg_read = []
avg_write = []
tnx_time_wo_reties = []
tnx_time_with_reties = []

trans_rate1 = []
retries1 = []
avg_read1 = []
avg_write1 = []
tnx_time_wo_reties1 = []
tnx_time_with_reties1 = []

total = 0
#file_name = "test"
#file_name = "statconcurrency1"
#file_name = "final/concurrency/baseline2"
file_name = "final/concurrency/baseline4"
file_name1 = "final/concurrency/dyts4"
flag = 1

def plot_Tnx_rate():
    global total

    plt.figure(1)
   # plt.subplot(211)
    plt.plot(concurrency, trans_rate, label="Fixed TimeStamp Ordering",ls="--",lw=2.0)
    plt.plot(concurrency, trans_rate1, label="Dynamic TimeStamp Ordering",lw=2.0)
    plt.xlabel('Concurrent Transactions', fontsize=30)
    plt.ylabel('Transactions per second', fontsize=30)
    #plt.text(30, 1200, 'Total number of transactions  = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0),fontsize=20)
    #plt.yticks(np.arange(min(min(trans_rate),min(trans_rate1)), max(max(trans_rate),max(trans_rate1))+1, 50 ))
    plt.yticks(np.arange(0, max(max(trans_rate),max(trans_rate1))+1000, 100 ), fontsize=20)

    plt.title('Transaction Rate', fontsize=40)
    plt.legend(loc='upper right', bbox_to_anchor=(1, 1), fontsize=20)
    plt.tight_layout()
    plt.show()


def plot_total_reties():
    global total
    plt.figure(1)
    #plt.subplot(212)
    plt.plot(concurrency, retries, label="Fixed TimeStamp Ordering",ls="--",lw=2.0) 
    plt.plot(concurrency, retries1, label="Dynamic TimeStamp Ordering",lw=2.0)
    plt.xlabel('Concurrent Transactions', fontsize=30)
    plt.ylabel('Total Retries or Aborts', fontsize=30)
    #plt.text(20, 1800, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0),fontsize=20)
    #plt.yticks(np.arange(min(min(retries),min(retries1)), max(max(retries),max(retries1))+1, 100 ))    
    plt.yticks(np.arange(0, max(max(retries),max(retries1))+1600, 100 ),fontsize=20)    
    plt.title('Total number of Aborts or Retries', fontsize=40)
    plt.legend(loc='upper right', bbox_to_anchor=(1, 1), fontsize=20) 
    plt.tight_layout()
    plt.show()

def plot_avg_read_time():
    global total
    plt.figure(1)
    plt.plot(concurrency, avg_read, label="Fixed TimeStamp Ordering",ls="--",lw=2.0) 
    plt.plot(concurrency, avg_read1, label="Dynamic TimeStamp Ordering",lw=2.0)
    plt.xlabel('Concurrent Transactions', fontsize=30)
    plt.ylabel('Average read time( in milli seconds )', fontsize=20)
    #plt.text(11, 20, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0),fontsize=20)
    #plt.yticks(np.arange(min(min(avg_read),min(avg_read1)), max(max(avg_read),max(avg_read1))+1, 10.0 ))    
    plt.yticks(np.arange(0, max(max(avg_read),max(avg_read1))+30, 10.0 ),fontsize=20)    
    plt.title('Average read time', fontsize=40)
    plt.legend(loc='upper right', bbox_to_anchor=(1, 1),fontsize=20) 
    plt.tight_layout()
    plt.show()
   
def plot_avg_write_time():
    global total
    plt.figure(1)
    plt.plot(concurrency, avg_write, label="Fixed TimeStamp Ordering",ls="--",lw=2.0) 
    plt.plot(concurrency, avg_write1, label="Dynamic TimeStamp Ordering",lw=2.0) 
    plt.xlabel('Concurrent Transactions', fontsize=30)
    plt.ylabel('Average write time( in milli seconds )', fontsize=20)
    #plt.text(11, 60, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0), fontsize=20)
    #plt.yticks(np.arange(min(min(avg_write),min(avg_write1)), max(max(avg_write),max(avg_write1))+1, 5.0 ))    
    plt.yticks(np.arange(0, max(max(avg_write),max(avg_write1))+5, 10.0 ), fontsize=20)    
    plt.title('Average Write Time', fontsize=40)
    plt.legend() 
    plt.tight_layout()
    plt.show()


def plot_avg_time_wo_retries():
    global total
    plt.figure(1)
    plt.plot(concurrency, tnx_time_wo_reties, label="Fixed TimeStamp Ordering",ls="--",lw=2.0) 
    plt.plot(concurrency, tnx_time_wo_reties1, label="Dynamic TimeStamp Ordering",lw=2.0) 
    plt.xlabel('Concurrent Transactions', fontsize=30)
    plt.ylabel('Average Transaction time ( in milli seconds )', fontsize=20)
    #plt.text(20, 104, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0), fontsize=20)
    #plt.yticks(np.arange(min(min(tnx_time_wo_reties),min(tnx_time_wo_reties1)), max(max(tnx_time_wo_reties),max(tnx_time_wo_reties1))+500, 10.0 ))    
    plt.yticks(np.arange(0, max(max(tnx_time_wo_reties),max(tnx_time_wo_reties1))+20, 10.0 ),fontsize=20)    
    plt.title('Average transaction time excluding retries', fontsize=30)
    plt.legend(loc='upper right', bbox_to_anchor=(1, 1),fontsize=20) 
    plt.tight_layout()
    plt.show()


def plot_avg_time_with_retries():
    global total
    plt.figure(1)
    plt.plot(concurrency, tnx_time_with_reties, label="Fixed TimeStamp Ordering",ls="--",lw=2.0)
    plt.plot(concurrency, tnx_time_with_reties1, label="Dynamic TimeStamp Ordering",lw=2.0) 
    plt.xlabel('Concurrent Transactions', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    #plt.text(20, 104, 'Total number of transactions  = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0))
    #plt.yticks(np.arange(min(min(tnx_time_with_reties),min(tnx_time_with_reties1)), max(max(tnx_time_with_reties),max(tnx_time_with_reties1))+1, 10.0 ))    
    plt.yticks(np.arange(0, max(max(tnx_time_with_reties),max(tnx_time_with_reties1))+1, 10.0 ))    
    plt.title('Average transaction time including retries', fontsize=26)
    plt.legend(loc='upper right', bbox_to_anchor=(0.5, 1))
    plt.tight_layout()
    plt.show()


def time(duration):
    duration = sanitize(duration)
    if (duration[-2:] == "ms"):
        return float(duration[:-2])
    else:
 #       print len(duration[:-2])
        duration = duration[:-1]
        return (float(duration[:-2])/1000)

def sanitize(value):
    if value[-1] == ",":
        return value[:-1]
    else:
        return value
def take_average(values):
    output = []

    start = 0
    end = 3
    
    while start < len(values):
        total = 0
        for each in values[start:end]:
            total += each
        output.append(total / 3)
        start = end
        end += 3
    return output

def take_average_of_all():
    global concurrency
    concurrency = take_average(concurrency)
    global trans_rate
    trans_rate = take_average(trans_rate)
    global retries
    retries = take_average(retries)
    global avg_read
    avg_read = take_average(avg_read)
    global avg_write
    avg_write = take_average(avg_write)
    global tnx_time_wo_reties
    tnx_time_wo_reties = take_average(tnx_time_wo_reties)
    global tnx_time_with_reties
    tnx_time_with_reties = take_average(tnx_time_with_reties)

def take_average_of_all1():
    global trans_rate1
    trans_rate1 = take_average(trans_rate1)
    global retries1
    retries1 = take_average(retries1)
    global avg_read1
    avg_read1 = take_average(avg_read1)
    global avg_write1
    avg_write1 = take_average(avg_write1)
    global tnx_time_wo_reties1
    tnx_time_wo_reties1 = take_average(tnx_time_wo_reties1)
    global tnx_time_with_reties1
    tnx_time_with_reties1 = take_average(tnx_time_with_reties1)

def getdata(line):
    line = line.split() 
    #print line
    #return
    global flag
    global total
    if flag:
        total = int(line[10])
        flag = 0
    concurrency.append(int(line[3]))
    trans_rate.append(float(line[7]))
    retries.append(int(sanitize(line[14])))
    avg_read.append(time(line[18]))
    avg_write.append(time(line[22]))
    tnx_time_wo_reties.append(time(line[28]))
    tnx_time_with_reties.append(time(line[37]))

def getdata1(line):
    line = line.split() 
    #print line
    global flag
    global total
    if flag:
        total = int(line[10])
        flag = 0
    #contention_range.append((line[3]))
    trans_rate1.append(float(line[7]))
    retries1.append(int(sanitize(line[14])))
    avg_read1.append(time(line[18]))
    avg_write1.append(time(line[22]))
    tnx_time_wo_reties1.append(time(line[28]))
    tnx_time_with_reties1.append(time(line[37]))


with open(file_name) as f:
    for line in f:
        getdata(line)

with open(file_name1) as f:
    for line in f:
        getdata1(line)

total = 100000
take_average_of_all()
take_average_of_all1()
#print trans_rate1
#print trans_rate
#plot_Tnx_rate()
#plot_total_reties()
#plot_avg_read_time()
plot_avg_write_time()
#plot_avg_time_wo_retries()
#plot_avg_time_with_retries()
