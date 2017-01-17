import matplotlib.pyplot as plt
import numpy as np

concurrency = []
trans_rate = []
retries = []
total = 0
#file_name = "test"
file_name = "output"
flag = 1

def plotgraph():
    global total

    plt.figure(1)

    plt.subplot(211)
    plt.plot(concurrency, trans_rate, label="Transaction rate")
    plt.xlabel('Concurrent Threads', fontsize=16)
    plt.text(30, 1000, 'Total number of transactions per concurrency = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0))
    plt.yticks(np.arange(min(trans_rate), max(trans_rate)+1, 50 ))
    plt.title('Transaction rate', fontsize=26)

    plt.subplot(212)
    plt.plot(concurrency, retries, label="Toatl number of Retries") 
    plt.xlabel('Concurrent Threads', fontsize=16)
    plt.text(15, 3000, 'Total number of transactions per concurrency = ' + str(total), style='italic', verticalalignment='top', horizontalalignment='left', fontsize=15, bbox={'facecolor':'wheat', 'alpha':0.5, 'pad':10})
    plt.xticks(np.arange(min(concurrency), max(concurrency)+1, 10.0))
    plt.yticks(np.arange(min(retries), max(retries)+1, 500 ))    
    plt.title('Total number of Retries', fontsize=26)
    #plt.legend()
    plt.tight_layout()
    plt.show()


def getdata(line):
    line = line.split() 
    global flag
    global total
    if flag:
        total = int(line[10])
        flag = 0
    concurrency.append(int(line[3]))
    trans_rate.append(float(line[7]))
    retries.append(int(line[14]))

with open(file_name) as f:
    for line in f:
        getdata(line)

plotgraph()
