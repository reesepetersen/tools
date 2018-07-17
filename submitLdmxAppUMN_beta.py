#! /usr/bin/env python

import os,sys
import glob
import argparse
import commands
import random
import subprocess

from time import strftime

myTime = strftime("%H%M%S")
random.seed()


tag_default = ""
usage = "usage: %prog [options]"
parser = argparse.ArgumentParser(usage)
parser.add_argument("-pan","--python_analyzer",dest="python_analyzer" , help="the python script which runs your analysis"  , required=True)
parser.add_argument("-par","--python_args"    , dest="python_args"    , help="The arguments you would provide to the python analyzer, without the input files at the end. in=inputDir, out=outputDir",nargs="+",required=True)
parser.add_argument("-i","--inputDir"         , dest="inputDir"       , help="the input directory containing the sim files to be processed",required=True)
parser.add_argument("-o","--outputDir"        , dest="outputDir"      , help="the output directory for results from condor", required=True)
parser.add_argument("--noLogging"             , dest="noLogging"      , help="disable logging capabilities"                , default=False, action="store_true")
parser.add_argument("--noSubmit"              , dest="noSubmit"       , help="do not submit to cluster"                    , default=False, action="store_true")
parser.add_argument("--perJob"                , dest="perJob"         , help="files per job"                               , default=1, type=int)
parser.add_argument("--tag"                   , dest="tag"            , help="Tag to be added to output directory and root file names. default = %s"%(tag_default), default=tag_default)
arg = parser.parse_args()

# Check for trailing slash on input and output directory and delete
if arg.inputDir.split("/")[-1] == "":
  inputDir = arg.inputDir[:-1]
else: 
  inputDir = arg.inputDir
if arg.outputDir.split("/")[-1] == "":
  outputDir = arg.outputDir[:-1]
else:
  outputDir = arg.outputDir

# Check that the input and output directories exist
if not os.path.exists(inputDir):
    print "Provided root directory \"%s\" does not exist!"%(inputDir)
    print "Exiting..."
    quit()

if not os.path.exists(outputDir):
    print "Provided output directory \"%s\" does not exist!"%(outputDir)
    print "Exiting..."
    quit()

# Check that the input directory is not empty
if not os.listdir(inputDir):
    print "Provided input directory \"%s\" is empty!"%(inputDir)
    print "Exiting..."
    quit()

# Extract sim jobname and create app jobname
inFileList = sorted(glob.glob("%s/*.root"%(inputDir)), key=lambda x: int(x.split('_')[-1].split('.')[0]))
first_file_data = ((((inFileList[0]).split('/')[-1]).split('.'))[0]).split('_')
first_file_data.pop()
analyzer = (arg.python_analyzer).split('.py')[0].split('/')[-1]
if arg.tag != "":
  tag = arg.tag+"_"
else: 
  tag = arg.tag
jobname = tag+analyzer+"_"+"_".join(first_file_data)
print "jobname: "+jobname
outputDir = outputDir+"/"+jobname
print "output directory: "+outputDir

# Define working directory
workingDir = "/export/scratch/users/%s"%(os.environ['USER'])

# number of files per job
numPerJob = arg.perJob
print "files per job: "+str(numPerJob)

# Figure number of files to process
numFiles = len(inFileList)
print "number of files to process: "+str(numFiles)
print "number of jobs: "+str(numFiles/numPerJob)

# Create directories
condorDir="%s/condor"%(outputDir)
logDir="%s/logs"%(outputDir)
os.makedirs(condorDir)
os.makedirs(logDir)

# Handle the arguments to the python analyzer
arg_list = arg.python_args
if "in" in arg_list:
  i_in = arg_list.index("in")
  arg_list.insert(i_in,inputDir)
  arg_list.remove("in")
if "out" in arg_list:
  i_out = arg_list.index("out")
  arg_list.insert(i_out,outputDir)
  arg_list.remove("out")
par = " ".join(arg_list)

# Write .sh file to be submitted to Condor
scriptFile = open("%s/runAnalysisJob_%s.sh"%(condorDir,myTime), "w")
scriptFileName=scriptFile.name
scriptFile.write("#!/bin/bash\n\n")
scriptFile.write("STUBNAME=$1\n")
scriptFile.write("OUTPATH=$2\n")
scriptFile.write("mkdir -p %s;cd %s\nmkdir ${STUBNAME}\ncd ${STUBNAME}\n"%(workingDir,workingDir))
scriptFile.write("hostname > ${STUBNAME}.log\n")
scriptFile.write("source ${HOME}/bin/ldmx-sw_setup.sh >> ${STUBNAME}.log 2>>${STUBNAME}.err\n")
scriptFile.write("shift\n")
scriptFile.write("shift\n")
scriptFile.write("INFILES=$@\n")
scriptFile.write("echo ${OUTPATH} >> ${STUBNAME}.log 2>>${STUBNAME}.err\n")
scriptFile.write("date >> ${STUBNAME}.log\n")
scriptFile.write("ldmx-app %s %s ${INFILES} >> ${STUBNAME}.log 2>>${STUBNAME}.err\n"%(arg.python_analyzer,par))
scriptFile.write("date >> ${STUBNAME}.log\n")
scriptFile.write("xz *.log *.err\n")
scriptFile.write("cp *.xz ${OUTPATH}/logs\n;cd ..;rm -r ${STUBNAME}")
scriptFile.close()

# Write Condor submit file
condorSubmit = open("%s/condorSubmitAnalysis"%(condorDir), "w")
condorSubmit.write("Executable          =  %s\n"%(scriptFile.name))
condorSubmit.write("Universe            =  vanilla\n")
condorSubmit.write("Requirements        =  Arch==\"X86_64\"  &&  (Machine  !=  \"zebra01.spa.umn.edu\")  &&  (Machine  !=  \"zebra02.spa.umn.edu\")  &&  (Machine  !=  \"zebra03.spa.umn.edu\")  &&  (Machine  !=  \"zebra04.spa.umn.edu\")  && (Machine != \"scorpion6.spa.umn.edu\")\n")
condorSubmit.write("+CondorGroup        =  \"cmsfarm\"\n")
condorSubmit.write("getenv              =  True\n")
condorSubmit.write("Request_Memory      =  1 Gb\n")

numFilesProc = 0
jobNum = 0
iterator = 0
inFileStr = ""
for file in inFileList:
  stubname="%s_%04d"%(jobname,jobNum)
  
  inFileStr=inFileStr+" "+file
  iterator += 1
  numFilesProc += 1
  if iterator == numPerJob or numFilesProc == numFiles:

    condorSubmit.write("Arguments = %s %s/ %s\n"%(stubname,outputDir,inFileStr))
    condorSubmit.write("Queue\n")

    jobNum += 1
    iterator = 0
    inFileStr = ""
    if numFilesProc == numFiles:
	break

condorSubmit.close()

os.system("chmod u+rwx %s"%(scriptFileName))

if arg.noSubmit:
    quit()

command = "condor_submit " + condorSubmit.name + "\n"
subprocess.call(command.split())
