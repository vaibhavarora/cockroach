import matplotlib.pyplot as plt
import numpy as np
import sys

concurrency = []
contention_range = []
read_write_ratio = []
read_only_ratio = []
trans_rate = []
retries = []
tnx_time_wo_reties = []
tnx_time_with_reties = []

concurrency1 = []
contention_range1 = []
read_write_ratio1 = []
read_only_ratio1 = []
trans_rate1 = []
retries1 = []
tnx_time_wo_reties1 = []
tnx_time_with_reties1 = []
total = 0
x_range = range(10,100,10)
#file_name = "test"

flag = 1

def plot_Tnx_rate_vs_concurrency():
    global total

    plt.figure(1)
    Y = [x for y, x in sorted(zip(concurrency, trans_rate))]
    X = sorted(concurrency)

    Y1 = [x for y, x in sorted(zip(concurrency1, trans_rate1))]
    X1 = sorted(concurrency1)
    # plt.subplot(211)
    global total

    plt.figure(1)
   # plt.subplot(211)
    plt.plot(X, Y, label="Fixed TimeStamp",ls="--",lw=2.0)
    plt.plot(X1, Y1, label="Dynamic TimeStamp Ordering",lw=2.0)
    plt.xlabel('Concurrent Transactions', fontsize=20)
    plt.ylabel('Transactions per second', fontsize=20)
    plt.yticks(np.arange(0, max(max(Y),max(Y1))+1000, 100 ), fontsize=10)

    plt.title('Transaction Rate', fontsize=20)
    plt.legend(loc='upper right', bbox_to_anchor=(1, 1), fontsize=10)
    plt.tight_layout()
    plt.show()




def plot_Tnx_rate_vs_contention():
    global total
   # print trans_rate
    plt.figure(1)
   # plt.subplot(211)
    Y = [x for y, x in sorted(zip(contention_range, trans_rate))]
    X = sorted(contention_range)

    Y1 = [x for y, x in sorted(zip(contention_range, trans_rate1))]
    X1 = sorted(contention_range1)

    plt.plot(X, Y, label="Fixed TimeStamp",ls="--",lw=2.0)
    plt.plot(X1, trans_rate1, label="Dynamic TimeStamp Ordering",lw=2.0)
    plt.xlabel('Contention ratio', fontsize=20)
    plt.ylabel('Transactions per second', fontsize=20)
    plt.xticks(np.arange(min(contention_range), max(contention_range)+1, 10.0))
    plt.xticks([w for w in X],['%i:%i'%(50+w,50-w) for w in range(0,50,10)],fontsize = 15)
    plt.yticks(np.arange(0, int(max(max(Y),max(Y1)))+1100, 100 ),fontsize = 10)

    plt.title('Transaction Rate for Read-Write Transactions', fontsize=15)
    #plt.legend()
    plt.legend(loc='upper right', bbox_to_anchor=(1, 1),fontsize =10)
    plt.tight_layout()
    plt.show()


def plot_Tnx_rate_vs_rw_ratio():
    global total
   # print trans_rate
    plt.figure(1)
   # plt.subplot(211)
    Y = [x for y, x in sorted(zip(read_write_ratio, trans_rate))]
    X = sorted(read_write_ratio)

    Y1 = [x for y, x in sorted(zip(read_write_ratio1, trans_rate1))]
    X1 = sorted(read_write_ratio1)

    plt.plot(X, Y, label="Fixed TimeStamp",ls="--",lw=2.0)
    plt.plot(X1, Y1, label="Dynamic TimeStamp Ordering",lw=2.0)
    plt.xlabel('Read write ratio', fontsize=20)
    plt.ylabel('Transactions per second', fontsize=20)

    plt.yticks(np.arange(0, int(max(max(Y),max(Y1)))+1100, 100 ),fontsize = 10)

    plt.title('Transaction Rate', fontsize=20)
    #plt.legend()
    plt.legend(loc='upper right', bbox_to_anchor=(1, 1),fontsize = 10)
    plt.tight_layout()
    plt.show()

def plot_Tnx_rate_vs_ro_ratio():
    global total
   # print trans_rate
    plt.figure(1)
   # plt.subplot(211)
    Y = [x for y, x in sorted(zip(read_only_ratio, trans_rate))]
    X = sorted(read_only_ratio)

    Y1 = [x for y, x in sorted(zip(read_only_ratio1, trans_rate1))]
    X1 = sorted(read_only_ratio)

    plt.plot(X, Y, label="Fixed TimeStamp",ls="--",lw=2.0)
    plt.plot(X1, Y1, label="Dynamic TimeStamp Ordering",lw=2.0)
    plt.xlabel('Read only percentage', fontsize=20)
    plt.ylabel('Transactions per second', fontsize=20)
    plt.yticks(np.arange(0, int(max(max(Y),max(Y1)))+1100, 100 ),fontsize = 10)

    plt.title('Transaction Rate', fontsize=20)
    #plt.legend()
    plt.legend(loc='upper right', bbox_to_anchor=(1, 1),fontsize = 10)
    plt.tight_layout()
    plt.show()


def plot_total_reties():
    global total
    plt.figure(1)
    #plt.subplot(212)
    Y = [x for y, x in sorted(zip(contention_range, retries))]
    X = sorted(contention_range)

    Y1 = [x for y, x in sorted(zip(contention_range1, retries1))]
    X1 = sorted(contention_range1)

    plt.plot(X, Y, label="Fixed TimeStamp Ordering",ls="--",lw=2.0) 
    plt.plot(X1, Y1, label="Dynamic TimeStamp Ordering",lw=2.0) 
    plt.xlabel('Contention ratio', fontsize=20)
    plt.ylabel('Total Retries or Aborts', fontsize=20)
    plt.xticks(np.arange(min(contention_range), max(contention_range)+1, 10.0))
    plt.xticks([w for w in X],['%i:%i'%(50+w,50-w) for w in range(0,50,10)],fontsize = 15)
    plt.yticks(np.arange(0, max(max(Y),max(Y1))+1000, 200 ),fontsize = 10)    
    plt.title('Total number of Aborts or Retries for Read-Write Transactions', fontsize=15)
    plt.legend(fontsize = 10) 
    plt.tight_layout()
    plt.show()

def plot_total_reties_vs_concurrency():
    global total
    plt.figure(1)
    #plt.subplot(212)
    Y = [x for y, x in sorted(zip(concurrency, retries))]
    X = sorted(concurrency)

    Y1 = [x for y, x in sorted(zip(concurrency1, retries1))]
    X1 = sorted(concurrency1)

    plt.plot(X, Y, label="Fixed TimeStamp Ordering",ls="--",lw=2.0) 
    plt.plot(X1, Y1, label="Dynamic TimeStamp Ordering",lw=2.0) 
    plt.xlabel('Concurrency', fontsize=20)
    plt.ylabel('Total Retries or Aborts', fontsize=20)
    plt.yticks(np.arange(0, max(max(Y),max(Y1))+500, 200 ),fontsize = 10)    
    plt.title('Total number of Aborts or Retries', fontsize=20)
    plt.legend(fontsize = 10) 
    plt.tight_layout()
    plt.show()


def plot_total_reties_vs_rw_ratio():
    global total
    plt.figure(1)
    #plt.subplot(212)
    Y = [x for y, x in sorted(zip(read_write_ratio, retries), reverse=True)]
    X = sorted(read_write_ratio)

    Y1 = [x for y, x in sorted(zip(read_write_ratio1, retries1), reverse=True)]
    X1 = sorted(read_write_ratio1)

    plt.plot(X, Y, label="Fixed TimeStamp Ordering",ls="--",lw=2.0) 
    plt.plot(X1, Y1, label="Dynamic TimeStamp Ordering",lw=2.0) 
    plt.xlabel('Read-Write ratio', fontsize=20)
    plt.ylabel('Total Retries or Aborts', fontsize=20)
    plt.xticks([w for w in range(90, 0, -10)],['%i:%i'%(w,100-w) for w in range(90, 0, -10)])
    plt.yticks(np.arange(0, max(max(Y),max(Y1))+500, 200 ),fontsize = 10)    
    plt.title('Total number of Aborts or Retries', fontsize=20)
    plt.legend(fontsize = 10) 
    plt.tight_layout()
    plt.show()

def time(duration):
    return float(duration[:-2])


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

    global contention_range
    contention_range = take_average(contention_range)
    
    global read_write_ratio
    read_write_ratio = take_average(read_write_ratio)

    global read_only_ratio
    read_only_ratio = take_average(read_only_ratio)

    global trans_rate
    trans_rate = take_average(trans_rate)

    global retries
    retries = take_average(retries)

    global tnx_time_wo_reties
    tnx_time_wo_reties = take_average(tnx_time_wo_reties)

    global tnx_time_with_reties
    tnx_time_with_reties = take_average(tnx_time_with_reties)


def take_average_of_all1():
    global concurrency1
    concurrency1 = take_average(concurrency1)

    global contention_range1
    contention_range1 = take_average(contention_range1)
    
    global read_write_ratio1
    read_write_ratio1 = take_average(read_write_ratio1)

    global read_only_ratio1
    read_only_ratio1 = take_average(read_only_ratio1)

    global trans_rate1
    trans_rate1 = take_average(trans_rate1)

    global retries1
    retries1 = take_average(retries1)

    global tnx_time_wo_reties1
    tnx_time_wo_reties1 = take_average(tnx_time_wo_reties1)

    global tnx_time_with_reties1
    tnx_time_with_reties1 = take_average(tnx_time_with_reties1)

def getdata(line):
    
    if line == '\n':
        return
    line = line.split('=')
    global total

    contention = line[1].split(':')[0]
    contention_range.append(int(contention))

    ro_ratio = line[2].split(':')[0]
    read_only_ratio.append(int(ro_ratio))

    rw_ratio = line[3].split(':')[0]
    read_write_ratio.append(int(rw_ratio))

    conc = line[4].split(':')[0]
    concurrency.append(int(conc))

    txn_rate = line[5].split(':')[0]
    trans_rate.append(float(txn_rate))

    ret = line[7].split(':')[0]
    retries.append(int(ret))

    wo_retries = line[8].split(':')[0]
    tnx_time_wo_reties.append(time(wo_retries))

    w_retries = line[8].split(':')[0]
    tnx_time_with_reties.append(time(w_retries))


def getdata1(line):
    
    if line == '\n':
        return
    line = line.split('=')
    global total

    contention = line[1].split(':')[0]
    contention_range1.append(int(contention))

    ro_ratio = line[2].split(':')[0]
    read_only_ratio1.append(int(ro_ratio))

    rw_ratio = line[3].split(':')[0]
    read_write_ratio1.append(int(rw_ratio))

    conc = line[4].split(':')[0]
    concurrency1.append(int(conc))

    txn_rate = line[5].split(':')[0]
    trans_rate1.append(float(txn_rate))

    ret = line[7].split(':')[0]
    retries1.append(int(ret))

    wo_retries = line[8].split(':')[0]
    tnx_time_wo_reties1.append(time(wo_retries))

    w_retries = line[8].split(':')[0]
    tnx_time_with_reties1.append(time(w_retries))


# file_name = '../../../../../../benchmark_results/readOnlyRatio/readOnlyRatio.log'
# file_name1 = "../../../../../../benchmark_results/benchmark_results/readOnlyRatio/readOnlyRatio.log"

# file_name = '../../../../../../benchmark_results/contentionRatio/contentionRatio.log'
file_name1 = "../../../../../../benchmark_results/benchmark_results/contentionRatio/contentionRatio.log"

# file_name = "../../../../../../benchmark_results/benchmark_results/contentionRatio/contentionRatio.log"
# file_name1 = "../../../../../../benchmark_results/benchmark_results/contentionRatio/modified_dyts_contention.log"

# file_name = '../../../../../../benchmark_results/readWriteRatio/readWriteRatio.log'
# file_name1 = "../../../../../../benchmark_results/benchmark_results/readWriteRatio/readWriteRatio.log"

# file_name = '../../../../../../benchmark_results/concurrency/concurrency.log'
# file_name1 = "../../../../../../benchmark_results/withDyTSCode/benchmark_results/concurrency/concurrency.log"

file_name = '../../../../../../benchmark_results/contentionRatio/contentionWriteTrans.log'
#file_name1 = "../../../../../../benchmark_results/withDyTSCode/benchmark_results/contentionRatio/contentionWriteTrans.log"

with open(file_name) as f:
    for line in f:
        getdata(line)

with open(file_name1) as f:
    for line in f:
        getdata1(line)

take_average_of_all()
take_average_of_all1()

#plot_Tnx_rate_vs_contention()
#plot_Tnx_rate_vs_concurrency()
#plot_Tnx_rate_vs_rw_ratio()
#plot_Tnx_rate_vs_ro_ratio()
plot_total_reties()
#plot_total_reties_vs_concurrency()
#plot_total_reties_vs_rw_ratio()