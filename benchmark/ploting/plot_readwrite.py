import matplotlib.pyplot as plt
import numpy as np

concurrency = 25
contention_range = range(50,80,10)
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
#file_name = "final/contention/baseline25"
file_name = "final/readwrite/baseline9010"
file_name1 = "final/readwrite/dyts9010"
flag = 1

def plot_Tnx_rate():
    global total
   # print trans_rate
    plt.figure(1)
   # plt.subplot(211)
    plt.plot(contention_range, trans_rate, label="Fixed TimeStamp Ordering",ls="--",lw=2.0)
    plt.plot(contention_range, trans_rate1, label="Dynamic TimeStamp Ordering",lw=2.0)
    plt.xlabel('Write:Read ratio', fontsize=16)
    plt.ylabel('Transactions per second', fontsize=16)
    #plt.text(60, 726, 'Total number of transactions  = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    #plt.text(60, 715, 'Concurrency  = ' + str(concurrency), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    #plt.xticks(np.arange(min(contention_range), max(contention_range)+1, 10.0))
    plt.xticks([w for w in contention_range],['%i:%i'%(50+w,50-w) for w in range(-30,50,20)])
    plt.yticks(np.arange(0, int(max(max(trans_rate),max(trans_rate1)))+1, 100 ))

    plt.title('Transaction rate', fontsize=26)
    #plt.legend()
    plt.legend(loc='upper right', bbox_to_anchor=(1, 0.5))
    plt.tight_layout()
    plt.show()


def plot_total_reties():
    global total
    plt.figure(1)
    #plt.subplot(212)
    plt.plot(contention_range, retries, label="Fixed TimeStamp Ordering",ls="--",lw=2.0) 
    plt.plot(contention_range, retries1, label="Dynamic TimeStamp Ordering",lw=2.0) 
    plt.xlabel('Write:Read ratio', fontsize=16)
    plt.ylabel('Total Retries/Aborts', fontsize=16)
    #plt.text(60, 26000, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    #plt.text(60, 24000, 'Concurrency  = ' + str(concurrency), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks([w for w in contention_range],['%i:%i'%(50+w,50-w) for w in range(-30,50,20)])
    plt.yticks(np.arange(0, max(max(retries),max(retries1))+1, 1000 ))    
    plt.title('Total number of Retries', fontsize=26)
    plt.legend() 
    plt.tight_layout()
    plt.show()

def plot_avg_read_time():
    global total
    plt.figure(1)
    plt.plot(contention_range, avg_read, label="Fixed TimeStamp Ordering",ls="--",lw=2.0)
    plt.plot(contention_range, avg_read1, label="Dynamic TimeStamp Ordering",lw=2.0) 
    plt.xlabel('Write:Read ratio', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    #plt.text(60, 17, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    #plt.text(60, 15, 'Concurrency  = ' + str(concurrency), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks([w for w in contention_range],['%i:%i'%(50+w,50-w) for w in range(-30,50,20)])
    plt.yticks(np.arange(0, max(max(avg_read),max(avg_read1))+1, 1.0 ))    
    plt.title('Average read time', fontsize=26)
    plt.legend(loc='upper right', bbox_to_anchor=(0.5, 0.5))
    plt.tight_layout()
    plt.show()
   
def plot_avg_write_time():
    global total
    plt.figure(1)
    plt.plot(contention_range, avg_write, label="Fixed TimeStamp Ordering",ls="--",lw=2.0) 
    plt.plot(contention_range, avg_write1, label="Dynamic TimeStamp Ordering",lw=2.0)
    plt.xlabel('Write:Read ratio', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    #plt.text(70, 181, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    #plt.text(70, 178, 'Concurrency  = ' + str(concurrency), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks([w for w in contention_range],['%i:%i'%(50+w,50-w) for w in range(-30,50,20)])
    plt.yticks(np.arange(0, max(max(avg_write),max(avg_write1))+1, 1.0 ))    
    plt.title('Average write time', fontsize=26)
    plt.legend(loc='upper right', bbox_to_anchor=(0.5, 0.5))
    #plt.legend() 
    plt.tight_layout()
    plt.show()


def plot_avg_time_wo_retries():
    global total
    plt.figure(1)
    plt.plot(contention_range, tnx_time_wo_reties, label="Fixed TimeStamp Ordering",ls="--",lw=2.0) 
    plt.plot(contention_range, tnx_time_wo_reties1, label="Dynamic TimeStamp Ordering",lw=2.0) 
    plt.xlabel('Write:Read ratio', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    #plt.text(70, 334, 'Total number of transactions = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    #plt.text(70, 329, 'Concurrency  = ' + str(concurrency), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks([w for w in contention_range],['%i:%i'%(50+w,50-w) for w in range(-30,50,20)])
    plt.yticks(np.arange(0, max(max(tnx_time_wo_reties),max(tnx_time_wo_reties1))+1, 5.0 ))    
    plt.title('Average transaction time excluding retries', fontsize=26)
    plt.legend(loc='upper right', bbox_to_anchor=(0.5, 1))
    plt.tight_layout()
    plt.show()


def plot_avg_time_with_retries():
    global total
    plt.figure(1)
    plt.plot(contention_range, tnx_time_with_reties, label="Fixed TimeStamp Ordering",ls="--",lw=2.0) 
    plt.plot(contention_range, tnx_time_with_reties1, label="Dynamic TimeStamp Ordering",lw=2.0) 
    plt.xlabel('Write:Read ratio', fontsize=16)
    plt.ylabel('( in milli seconds )', fontsize=16)
    #plt.text(60, 365, 'Total number of transactions  = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    #plt.text(60, 360, 'Concurrency  = ' + str(concurrency), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks([w for w in contention_range],['%i:%i'%(50+w,50-w) for w in range(-30,50,20)])
    plt.yticks(np.arange(0, max(max(tnx_time_with_reties),max(tnx_time_with_reties1))+1, 5.0 ))    
    plt.title('Average transaction time including retries', fontsize=26)
    #plt.legend()
    #plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=10,ncol=2, mode="expand", borderaxespad=0.) 
    plt.legend(loc='upper right', bbox_to_anchor=(0.5, 1))
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
  #  print line
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
#print trans_rate , trans_rate1
plot_Tnx_rate()
plot_total_reties()
plot_avg_read_time()
plot_avg_write_time()
plot_avg_time_wo_retries()
plot_avg_time_with_retries()
