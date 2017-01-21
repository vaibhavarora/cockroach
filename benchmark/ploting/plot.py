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
file_name = "contention_stat"
flag = 1

def plot_Tnx_rate():
    global total

    plt.figure(1)

   # plt.subplot(211)
    plt.plot(concurrency, trans_rate, label="Transaction rate")
    plt.xlabel('Concurrent Threads', fontsize=16)
    plt.text(30, 1000, 'Total number of transactions per concurrency = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0))
    plt.yticks(np.arange(min(trans_rate), max(trans_rate)+1, 50 ))
    plt.title('Transaction rate', fontsize=26)

    plt.tight_layout()
    plt.show()


def plot_total_reties():
    plt.figure(1)
    #plt.subplot(212)
    plt.plot(concurrency, retries, label="Toatl number of Retries") 
    plt.xlabel('Concurrent Threads', fontsize=16)
    plt.text(15, 3000, 'Total number of transactions per concurrency = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0))
    plt.yticks(np.arange(min(retries), max(retries)+1, 100 ))    
    plt.title('Total number of Retries', fontsize=26)
    #plt.legend() 
    plt.tight_layout()
    plt.show()

def plot_avg_read_time():
    plt.figure(1)
    plt.plot(concurrency, avg_read, label="Average read time ") 
    plt.xlabel('Concurrent Threads', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    plt.text(15, 3000, 'Total number of transactions per concurrency = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0))
    plt.yticks(np.arange(min(avg_read), max(avg_write)+1, 10.0 ))    
    plt.title('Average read time', fontsize=26)
    #plt.legend() 
    plt.tight_layout()
    plt.show()
   
def time(duration):
    return duration[:-2]

def getdata(line):
    line = line.split() 
    print line
    return
    global flag
    global total
    if flag:
        total = int(line[10])
        flag = 0
    concurrency.append(int(line[3]))
    trans_rate.append(float(line[7]))
    retries.append(int(line[14]))
    avg_read.append(time(line[17]))
    avg_write.append(time(line[21]))
    tnx_time_wo_reties.append(time(line[27]))
    tnx_time_with_reties.append(time(line[36]))

with open(file_name) as f:
    for line in f:
        getdata(line)

#plot_Tnx_rate_vs_concurrency()
#plot_total_reties_vs_concurrency()
