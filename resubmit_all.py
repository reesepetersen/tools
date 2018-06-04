#!/usr/bin/env python

# import useful libraries
import os
import sys
import argparse
import re
import subprocess
import condor_resubmit

# taking an argument from the command line
parser = argparse.ArgumentParser(description = 'Designed for failed condor jobs, due to a certain gdml file being unavailable from CERN or if a scorpion ate a job.\nIt looks for large error files, extracts their job numbers, then copies the appropriate lines from the condorSubmit file and resubmits to condor.')
parser.add_argument('jobdir', help='Give the full path: /local/cms/user/%s/LDMX/jobdir where some condor jobs failed'%(os.environ["USER"]))
parser.add_argument("--noSubmit"  , dest="noSubmit"  , help="do not submit to cluster", action="store_true")
parser.add_argument("--noNice"  , dest="noNice"  , help="do not run at nice priority, regardless of the original job priority", action="store_true")
parser.add_argument("--nice"  , dest="nice"  , help="run at nice priority, regardless of the original job priority", action="store_true")

arg = parser.parse_args()

# checking for the given jobdir or directory
if not os.path.exists(arg.jobdir):
	print "Given directory does not exist! Exiting..."
	quit()

# make sure there is a slash at the end of the path to the job directory
if arg.jobdir.split("/")[-1]=="":
	jobDir=arg.jobdir
else:
	jobDir=arg.jobdir+"/"

# checking for the logs folder
if not os.path.exists(jobDir+"logs/"):
	print "No logs folder in %s directory! Exiting..."%(jobDir)
	quit()
logDir=jobDir+"logs/"

# get the .err.xz files and .log files.xz separately
logerrFiles=os.listdir(logDir)
errFiles=[]
logFiles=[]
for file in logerrFiles:
	if file.split(".")[-2]=="err":
		errFiles.insert(-1,file)
	elif file.split(".")[-2]=="log":
		logFiles.insert(-1,file)
	else:
		print file.split(".")[-2]
		print "Found some other kind of file in logs folder."

# look for larger-than-usual .err.xz files
flaggedErrFiles=[]
for errFile in errFiles:
	if os.stat("%s%s"%(logDir,errFile)).st_size>120:
		flaggedErrFiles.insert(-1,errFile)

# check if there are any jobs to resubmit
if len(flaggedErrFiles) < 1:
	print "Couldn't find any larger-than-average error files. Exiting..."
	quit()

# extract number from flagged .err.xz files
flaggedNums=[]
for file in flaggedErrFiles:
	intString=file.split("_")[-1].split(".")[0]
	intNum=int(intString)
	flaggedNums.insert(-1,intNum)
flaggedNums.sort()
flaggedStrings=[]
for num in flaggedNums:
	flaggedString=str(num)
	flaggedStrings.append(flaggedString)

# check for condor directory
condorDir=jobDir+"condor/"
if not os.path.exists(condorDir):
	print "No condor directory! Exiting..."
	quit()

# check for condorSubmit file
if not os.path.isfile(condorDir+"condorSubmit"):
	print "condorSubmit file not found! Exiting..."
	quit()

# check for previous condorResubmit files and write the first six lines of a new one if previous are found
reSubString = ""
reSubNum = 0
reSubFlag = 0
while reSubFlag == 0:
	if os.path.isfile(condorDir+"condorResubmit%s"%(reSubString)):
		print "condorResubmit%s already exists!"%(reSubString)
		reSubNum=reSubNum+1
		reSubString=str(reSubNum)
	else:
		print "Writing condorResubmit%s"%(reSubString)
		reSubFlag=1

# count the number of lines in the file
with open(condorDir+"condorSubmit","r") as f:
	i=0
	for line in f:
		i = i + 1
	numLines=i
condorFile=open(condorDir+"condorSubmit","r")
resubmitFile=open(condorDir+"condorResubmit%s"%(reSubString),"w")

i = 1
n = numLines

# write all lines from condorSubmit to condorResubmit until you see "Arguments" then go to next step, unless you don't find any
nice_flag = 0;
while i <= n:
	currentLine=condorFile.readline()
	if currentLine[0:9] == "nice_user":
		nice_flag = 1;
		if arg.noNice == True:
			i = i + 1
			continue
	if currentLine[0:14] == "Request_Memory" and nice_flag == 0:
		if arg.nice == True:
			resubmitFile.write("nice_user = True")
	if currentLine[0:9] == "Arguments":
		break
	resubmitFile.write(currentLine)
	i = i + 1
if i >= n:
	print "No Arguments found in condorSubmit file. Exiting..."
	quit()

# loop over the remaining contents of the condorSubmit file and write lines with flagged numbers to the condorResubmit file
currentNumber=str(int(currentLine.split(" ")[-2].split("_")[-1]))
while len(flaggedStrings) > 0:
	if currentNumber == flaggedStrings[0]:
		resubmitFile.write(currentLine)
		resubmitFile.write(condorFile.readline())
		flaggedStrings.remove(flaggedStrings[0])
		if len(flaggedStrings) == 0:
			break
	else:
		currentLine=condorFile.readline()
	currentLine=condorFile.readline()
	currentNumber=str(int(currentLine.split(" ")[-2].split("_")[-1]))
resubmitFile.close()
condorFile.close()

# submit to condor
if arg.noSubmit:
	quit()
command = "condor_submit " + resubmitFile.name + "\n"
subprocess.call(command.split())
