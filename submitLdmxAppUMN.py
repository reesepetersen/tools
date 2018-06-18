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

usage = "usage: %prog [options]"
parser = argparse.ArgumentParser(usage)
parser.add_argument("--python_analyzer",dest="python_analyzer",help="the python script which runs your analysis",required=True)
parser.add_argument("--endLayer", dest="endLayer"    , help="last layer of layer sum"       , default=20, type=int)
parser.add_argument("--inputDir"  , dest="inputDir"    , help="directory containing files"    , required=True)
parser.add_argument("--mode"      , dest="mode"        , help="trigger mode"                  , default=0, type=int)
parser.add_argument("--noLogging" , dest="noLogging"   , help="disable logging capabilities"  , default=False, action="store_true")
parser.add_argument("--noSubmit"  , dest="noSubmit"    , help="do not submit to cluster"      , default=False, action="store_true")
parser.add_argument("--numFiles"  , dest="numFiles"    , help="number of files to process"    , default=-1, type=int)
parser.add_argument("--outputDir" , dest="outputDir"   , help="directory to output ROOT files", default="/local/cms/user/%s/LDMX"%(os.environ['USER']))
parser.add_argument("--perJob"    , dest="perJob"      , help="files per job"                 , default=1, type=int)
parser.add_argument("--startLayer", dest="startLayer"  , help="first layer of layer sum"      , default=1, type=int)
parser.add_argument("--threshold" , dest="threshold"   , help="layer energy sum cut"          , default=24) # Units of sim-MeV
parser.add_argument("--jobname", dest="jobname", help="job name to be used for directories and root files", required=True)
arg = parser.parse_args()

# Figure number of files to process
outputDir = arg.outputDir+"/"+arg.jobname
workingDir = "/export/scratch/users/%s/%s"%(os.environ['USER'],arg.jobname)

inputDir    = arg.inputDir
numPerJob   = arg.perJob
inFileList  = sorted(glob.glob("%s/*.root"%(inputDir)), key=lambda x: int(x.split('_')[-1].split('.')[0]))

# Figure number of files to process
if arg.numFiles == -1:
    numFiles = len(inFileList)
else:
    numFiles = arg.numFiles

# Check that the input and output directories exist
if not os.path.exists(inputDir):
    print "Provided root directory \"%s\" does not exist!"%(inputDir)
    print "Exiting..."
    quit()

os.makedirs(outputDir)
if not os.path.exists(outputDir):
    print "Provided output directory \"%s\" does not exist!"%(outputDir)
    print "Exiting..."
    quit()

# Check that the input directory is not empty
if not os.listdir(inputDir):
    print "Provided input directory \"%s\" is empty!"%(inputDir)
    print "Exiting..."
    quit()

# Check for trailing slash on input and output directory and delete
if arg.inputDir.split("/")[-1] == "": inputDir = arg.inputDir[:-1]

# Check for temp directory and create one if none exists 
if not os.path.exists("./temp"): os.mkdir("temp")

condorDir="%s/condor"%(outputDir)
logDir="%s/logs"%(outputDir)
os.makedirs(condorDir)
os.makedirs(logDir)


# Write .sh file to be submitted to Condor
scriptFile = open("%s/runAnalysisJob_%s.sh"%(condorDir,myTime), "w")
scriptFileName=scriptFile.name
scriptFile.write("#!/bin/bash\n\n")
scriptFile.write("STUBNAME=$1\n")
scriptFile.write("OUTPATH=$2\n")
scriptFile.write("mkdir -p %s/${STUBNAME}\ncd %s/${STUBNAME}\n"%(workingDir,workingDir))
scriptFile.write("hostname > ${STUBNAME}.log\n")
scriptFile.write("source ${HOME}/bin/ldmx-sw_setup.sh >> ${STUBNAME}.log 2>>${STUBNAME}.err\n")
scriptFile.write("shift\n")
scriptFile.write("shift\n")
scriptFile.write("INFILES=$@\n")
scriptFile.write("ldmx-app ${HOME}/Projects/LDMX/ldmx-sw/my_standalone.py %s %s %s %s ${OUTPATH} ${STUBNAME} ${INFILES} >> ${STUBNAME}.log 2>>${STUBNAME}.err\n"%(arg.threshold,arg.startLayer,arg.endLayer,arg.mode))
scriptFile.write("xz *.log *.err\n")
scriptFile.write("cp *.xz ${OUTPATH}/logs\n;cd ..;rm -rf ${STUBNAME}")
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

    stubname="%s_%04d"%(arg.jobname,jobNum)
    if numFilesProc == numFiles:
	break

    inFileStr=inFileStr+" "+file

    iterator += 1
    numFilesProc += 1
    if iterator == numPerJob or numFilesProc == numFiles:

        condorSubmit.write("Arguments = %s %s/ %s\n"%(stubname,outputDir,inFileStr))
        condorSubmit.write("Queue\n")

        jobNum += 1
	iterator = 0
	inFileStr = ""

condorSubmit.close()

os.system("chmod u+rwx %s"%(scriptFileName))

if arg.noSubmit:
    quit()

command = "condor_submit " + condorSubmit.name + "\n"
subprocess.call(command.split())
