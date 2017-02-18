import matplotlib.pyplot as plt
import numpy as np

concurrency = 100
contention_range = range(50,100,10)
trans_rate = []
retries = []
avg_read = []
avg_write = []
tnx_time_wo_reties = []
tnx_time_with_reties = []
total = 0
#file_name = "contention_output"
flag = 1

def plot_Tnx_rate():
    global total
   # print trans_rate
    plt.figure(1)
   # plt.subplot(211)
    plt.plot(contention_range, trans_rate, label="Transaction rate")
    plt.xlabel('Contention ratio', fontsize=16)
    plt.ylabel('Transactions per second', fontsize=16)
    plt.text(60, 726, 'Total number of transactions  = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.text(60, 715, 'Concurrency  = ' + str(concurrency), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    #plt.xticks(np.arange(min(contention_range), max(contention_range)+1, 10.0))
    plt.xticks([w for w in contention_range],['%i:%i'%(50+w,50-w) for w in range(0,50,10)])
    plt.yticks(np.arange(min(trans_rate), max(trans_rate)+1, 5 ))

    plt.title('Transaction rate', fontsize=26)

    plt.tight_layout()
    plt.show()


def plot_total_reties():
    global total
    plt.figure(1)
    #plt.subplot(212)
    plt.plot(contention_range, retries, label="Total number of Retries") 
    plt.xlabel('Contention ratio', fontsize=16)
    plt.ylabel('Total Retries', fontsize=16)
    plt.text(60, 26000, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.text(60, 24000, 'Concurrency  = ' + str(concurrency), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    #plt.xticks(np.arange(min(contention_range), max(contention_range)+1, 10.0))
    plt.xticks([w for w in contention_range],['%i:%i'%(50+w,50-w) for w in range(0,50,10)])
    plt.yticks(np.arange(min(retries), max(retries)+1, 1000 ))    
    plt.title('Total number of Retries', fontsize=26)
    #plt.legend() 
    plt.tight_layout()
    plt.show()

def plot_avg_read_time():
    global total
    plt.figure(1)
    plt.plot(contention_range, avg_read, label="Average read time ") 
    plt.xlabel('Contention ratio', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    plt.text(60, 17, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.text(60, 15, 'Concurrency  = ' + str(concurrency), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks([w for w in contention_range],['%i:%i'%(50+w,50-w) for w in range(0,50,10)])
    plt.yticks(np.arange(min(avg_read), max(avg_read)+1, 1.0 ))    
    plt.title('Average read time', fontsize=26)
    #plt.legend() 
    plt.tight_layout()
    plt.show()
   
def plot_avg_write_time():
    global total
    plt.figure(1)
    plt.plot(contention_range, avg_write, label="Average write time ") 
    plt.xlabel('Contention ratio', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    plt.text(70, 181, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.text(70, 178, 'Concurrency  = ' + str(concurrency), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks([w for w in contention_range],['%i:%i'%(50+w,50-w) for w in range(0,50,10)])
    plt.yticks(np.arange(min(avg_write), max(avg_write)+1, 3.0 ))    
    plt.title('Average write time', fontsize=26)
    #plt.legend() 
    plt.tight_layout()
    plt.show()


def plot_avg_time_wo_retries():
    global total
    plt.figure(1)
    plt.plot(contention_range, tnx_time_wo_reties, label="Average transaction time excluding retries ") 
    plt.xlabel('Contention ratio', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    plt.text(70, 334, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.text(70, 329, 'Concurrency  = ' + str(concurrency), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks([w for w in contention_range],['%i:%i'%(50+w,50-w) for w in range(0,50,10)])
    plt.yticks(np.arange(min(tnx_time_wo_reties), max(tnx_time_wo_reties)+1, 5.0 ))    
    plt.title('Average transaction time excluding retries', fontsize=26)
    #plt.legend() 
    plt.tight_layout()
    plt.show()


def plot_avg_time_with_retries():
    global total
    plt.figure(1)
    plt.plot(contention_range, tnx_time_with_reties, label="Average transaction time including retries ") 
    plt.xlabel('Contention ratio', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    plt.text(60, 365, 'Total number of transactions  = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.text(60, 360, 'Concurrency  = ' + str(concurrency), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks([w for w in contention_range],['%i:%i'%(50+w,50-w) for w in range(0,50,10)])
    plt.yticks(np.arange(min(tnx_time_with_reties ), max(tnx_time_with_reties)+1, 5.0 ))    
    plt.title('Average transaction time including retries', fontsize=26)
    #plt.legend() 
    plt.tight_layout()
    plt.show()


def time(duration):
    duration = sanitize(duration)
    return float(duration[:-2])

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

    global flag
    global total
    if flag:
        total = int(line[10])
        flag = 0
    #contention_range.append((line[3]))
    trans_rate.append(float(line[7]))
    retries.append(int(sanitize(line[14])))
    avg_read.append(time(line[18]))
    avg_write.append(time(line[22]))
    tnx_time_wo_reties.append(time(line[28]))
    tnx_time_with_reties.append(time(line[37]))

with open(file_name) as f:
    for line in f:
        getdata(line)

#print trans_rate
total = 250000
take_average_of_all()
#print trans_rate
plot_Tnx_rate()
plot_total_reties()
plot_avg_read_time()
plot_avg_write_time()
plot_avg_time_wo_retries()
plot_avg_time_with_retries()
