import matplotlib.pyplot as plt
import numpy as np

concurrency = []
trans_rate = []
retries = []
avg_read = []
avg_write = []
tnx_time_wo_reties = []
tnx_time_with_reties = []
total = 0
#file_name = "test"
#file_name = "output"
file_name = "concurency_without_crash"
flag = 1

def plot_Tnx_rate():
    global total

    plt.figure(1)
   # plt.subplot(211)
    plt.plot(concurrency, trans_rate, label="Transaction rate")
    plt.xlabel('Concurrent Threads', fontsize=16)
    plt.ylabel('Transactions per second', fontsize=16)
    plt.text(30, 1200, 'Total number of transactions  = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0))
    plt.yticks(np.arange(min(trans_rate), max(trans_rate)+1, 50 ))

    plt.title('Transaction rate', fontsize=26)

    plt.tight_layout()
    plt.show()


def plot_total_reties():
    global total
    plt.figure(1)
    #plt.subplot(212)
    plt.plot(concurrency, retries, label="Total number of Retries") 
    plt.xlabel('Concurrent Threads', fontsize=16)
    plt.ylabel('Total Retries', fontsize=16)
    plt.text(20, 1800, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0))
    plt.yticks(np.arange(min(retries), max(retries)+1, 100 ))    
    plt.title('Total number of Retries', fontsize=26)
    #plt.legend() 
    plt.tight_layout()
    plt.show()

def plot_avg_read_time():
    global total
    plt.figure(1)
    plt.plot(concurrency, avg_read, label="Average read time ") 
    plt.xlabel('Concurrent Threads', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    plt.text(11, 20, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0))
    plt.yticks(np.arange(min(avg_read), max(avg_read)+1, 1.0 ))    
    plt.title('Average read time', fontsize=26)
    #plt.legend() 
    plt.tight_layout()
    plt.show()
   
def plot_avg_write_time():
    global total
    plt.figure(1)
    plt.plot(concurrency, avg_write, label="Average write time ") 
    plt.xlabel('Concurrent Threads', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    plt.text(11, 60, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0))
    plt.yticks(np.arange(min(avg_write), max(avg_write)+1, 5.0 ))    
    plt.title('Average write time', fontsize=26)
    #plt.legend() 
    plt.tight_layout()
    plt.show()


def plot_avg_time_wo_retries():
    global total
    plt.figure(1)
    plt.plot(concurrency, tnx_time_wo_reties, label="Average transaction time excluding retries ") 
    plt.xlabel('Concurrent Threads', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    plt.text(20, 104, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0))
    plt.yticks(np.arange(min(tnx_time_wo_reties), max(tnx_time_wo_reties)+1, 10.0 ))    
    plt.title('Average transaction time excluding retries', fontsize=26)
    #plt.legend() 
    plt.tight_layout()
    plt.show()


def plot_avg_time_with_retries():
    global total
    plt.figure(1)
    plt.plot(concurrency, tnx_time_with_reties, label="Average transaction time including retries ") 
    plt.xlabel('Concurrent Threads', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    plt.text(20, 104, 'Total number of transactions  = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0))
    plt.yticks(np.arange(min(tnx_time_with_reties ), max(tnx_time_with_reties)+1, 10.0 ))    
    plt.title('Average transaction time including retries', fontsize=26)
    #plt.legend() 
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
    end = 5
    
    while start < len(values):
        total = 0
        for each in values[start:end]:
            total += each
        output.append(total / 5)
        start = end
        end += 5
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

with open(file_name) as f:
    for line in f:
        getdata(line)

total = 250000
take_average_of_all()
#print trans_rate
#plot_Tnx_rate()
#plot_total_reties()
plot_avg_read_time()
plot_avg_write_time()
plot_avg_time_wo_retries()
plot_avg_time_with_retries()
