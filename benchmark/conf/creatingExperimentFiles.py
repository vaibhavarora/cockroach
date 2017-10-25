#!/usr/bin/python
import sys, os, time
from subprocess import *
 
 
# example way to run
# To vary number of clients
# ./creatingExperimentFiles.py . concurrency_dir concurrency 5 25 5

generalConfigFileName = sys.argv[1]; 
outPutDir = sys.argv[2];
changingVariable = sys.argv[3];
start=int(sys.argv[4]);
end=int(sys.argv[5]);
step=int(sys.argv[6]);
 
outputConfigFileNamePrefix = outPutDir + "/" + generalConfigFileName;
serverOutFileNamePrefix  = outPutDir + "/" + generalConfigFileName; 
 
index=0; 
for varyingValue in range(start, end, step):
  outPutConfigFileName = outputConfigFileNamePrefix + "." + changingVariable + "." + str(varyingValue);
  
  removeOutPutConfigCommand = 'rm ' + outPutConfigFileName;
  
  generalConfigFile = open(generalConfigFileName, "r");
  outputConfigFile = open(outPutConfigFileName, "w");
   
  for eachline in generalConfigFile.readlines():
    eachline = eachline.strip();
    # Check if the line starts with the varying variable
    if not eachline.startswith("\"" + changingVariable+"\":"):
      outputConfigFile.write(eachline+"\n");
    else:
      if "Ratio" in changingVariable:
        ratio = "\"" + str(varyingValue)+":" +  str(100-varyingValue) + "\""
        outputConfigFile.write("\"" + changingVariable+"\":"+ratio+",\n"); 
      else:
        outputConfigFile.write("\"" + changingVariable+"\":"+str(varyingValue)+",\n");
  print 'Created ' + outPutConfigFileName
  outputConfigFile.close();
  generalConfigFile.close();
   
  index += 1;