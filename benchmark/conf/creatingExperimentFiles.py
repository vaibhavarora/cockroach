#!/usr/bin/python
import sys, os, time
from subprocess import *
 
 
# example way to run
# To vary number of clients
# ./creatingExperimentFiles.py . concurrency_dir concurrency 5 25 5
 
serverInfoFileName = sys.argv[1];
 
outPutDir = sys.argv[2];
 
generalConfigFileName = sys.argv[3];
 
changingVariable = sys.argv[4];
start=int(sys.argv[5]);
end=int(sys.argv[6]);
step=int(sys.argv[7]);
 
dryRun = 0;
 
outputConfigFileNamePrefix = outPutDir + "/" + generalConfigFileName;
serverOutFileNamePrefix  = outPutDir + "/" + generalConfigFileName; 
 
index=0; 
for varyingValue in range(start, end, step):
  outPutConfigFileName = outputConfigFileNamePrefix + "." + changingVariable + "." + str(varyingValue);
  serverOutFileName = serverOutFileNamePrefix +  "." + changingVariable + "." + str(varyingValue) + ".serverInfo";
  
  removeOutPutConfigCommand = 'rm ' + outPutConfigFileName;
  serverInfoFileOutCopyCommand = 'cp '+ serverInfoFileName + ' ' + serverOutFileName;
  print serverInfoFileOutCopyCommand;
  print removeOutPutConfigCommand;
   
  if dryRun == 0:    
    os.system(serverInfoFileOutCopyCommand);
    os.system(removeOutPutConfigCommand);
  
  generalConfigFile = open(generalConfigFileName, "r");
  outputConfigFile = open(outPutConfigFileName, "w");
   
  for eachline in generalConfigFile.readlines():
    eachline = eachline.strip();
    # Check if the line starts with the varying variable
    if dryRun == 0 and not eachline.startswith("\"" + changingVariable+"\":"):
      outputConfigFile.write(eachline+"\n");
    else:
      if "Ratio" in changingVariable:
        ratio = "\"" + str(varyingValue)+":" +  str(100-varyingValue) + "\""
        outputConfigFile.write("\"" + changingVariable+"\":"+ratio+",\n"); 
      else:
        outputConfigFile.write("\"" + changingVariable+"\":"+str(varyingValue)+",\n");
    # elif dryRun == 1:
    #   print eachline;
 
  # if dryRun == 0:
  #   outputConfigFile.write(changingVariable+":"+str(varyingValue)+"\n");
  # else:
  #  print changingVariable+":"+str(varyingValue);
 
  outputConfigFile.close();
  generalConfigFile.close();
   
  index += 1;