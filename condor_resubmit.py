#!/usr/bin/env python

# import useful libraries
import os
import sys
import argparse
import re
import subprocess
import random

# taking an argument from the command line
parser = argparse.ArgumentParser(description = 'Designed for failed condor jobs, due to a certain gdml file being unavailable from CERN or if a scorpion ate a job.\nIt reads through the mac files, extracts their job numbers, then copies the appropriate lines from the condorSubmit file and resubmits to condor for all files not appearing in the output directory, as well as those with larger than average (>120) err file size.')
parser.add_argument('jobdir', help='Give the full path where some condor jobs failed')
parser.add_argument("--noNice"  , dest="noNice"  , help="do not run at nice priority, regardless of the original job priority", action="store_true")
parser.add_argument("--nice"  , dest="nice"  , help="run at nice priority, regardless of the original job priority", action="store_true")
parser.add_argument("--doAll"  , dest="doAll"  , help="check and resubmit if needed for all jobs in /local/cms/user/%s/LDMX/"%(os.environ["USER"]), action="store_true")
parser.add_argument("--noSubmit"  , dest="noSubmit"  , help="do not submit", action="store_true")

arg = parser.parse_args()

def resubmit(path,niceness):
  
  # checking for the given jobdir or directory
  if not os.path.exists(path):
	print "Given directory does not exist! Exiting..."
	quit()

  # make sure there is a slash at the end of the path to the job directory
  if arg.jobdir.split("/")[-1]=="":
	jobDir=path
  else:
	jobDir=path+"/"

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

  # extract number from flagged .err.xz files
  flaggedErrFileNums=[]
  if len(flaggedErrFiles) > 0:
    for file in flaggedErrFiles:
      errFileNum=file.split("_")[-1].split(".")[0]
      flaggedErrFileNums.insert(-1,errFileNum)
  flaggedErrFileSet = set(flaggedErrFileNums)
  print "flagged error files: {0}".format(flaggedErrFileNums)

  # look through the mac files, compare to the root files, and mark the missing root file numbers
  macDir=jobDir+"mac/"
  macFiles=os.listdir(macDir)
  rootFiles=os.listdir(jobDir)
  for rootFile in rootFiles:
    if len(rootFile.split(".root")) < 2:
      rootFiles.remove(rootFile)
  if len(rootFiles) < len(macFiles):
    macNumbers = []
    for macFile in macFiles:
      macNumbers.append((macFile.split(".")[0]).split("_")[-1])
    macSet = set(macNumbers)
    rootNumbers = []
    for rootFile in rootFiles:
      rootNumbers.append((rootFile.split(".")[0]).split("_")[-1])
    rootSet = set(rootNumbers)
    missingRootFileSet = macSet-rootSet
    missingRootFileNums = list(missingRootFileSet)
  else:
    missingRootFileNums = []
    missingRootFileSet = set(missingRootFileNums)
  print "missing root files: {0}".format(missingRootFileNums)

  # if there are no jobs to redo, quit
  if not missingRootFileSet and not flaggedErrFileSet:
    print "No jobs to resubmit, exiting..."
    quit()

  # check if there are any jobs to resubmit
  if len(flaggedErrFiles) < 1 and len(rootFiles) == len(macFiles):
	print "Couldn't find any larger-than-average error files. All root files are present. Exiting..."
	quit()

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
		if niceness == "noNice":
			i = i + 1
			continue
	if currentLine[0:14] == "Request_Memory" and nice_flag == 0:
                if niceness == "nice":
			resubmitFile.write("nice_user = True")
	if currentLine[0:9] == "Arguments":
		break
	resubmitFile.write(currentLine)
	i = i + 1
  if i >= n:
	print "No Arguments found in condorSubmit file. Exiting..."
	quit()

  # copy the pattern found in the rest of the file for the redo numbers
  space_split = currentLine.split(" ")
  job_name = space_split[-2]
  job_info = job_name.split("_")
  redoSet = missingRootFileSet | flaggedErrFileSet
  redoNums = list(redoSet)
  for redo_number in redoNums:
    redo_info = job_info
    redo_info[-1] = redo_number
    redo_name = "_".join(redo_info)
    redo_space_split = space_split
    redo_space_split[-2] = redo_name
    redoLine = " ".join(redo_space_split)
    resubmitFile.write(redoLine)
    resubmitFile.write("Queue\n")
#    mac_file = open(macDir+job_name+"_%s.mac"%(redo_number))
#    for line in mac_file:
#      if len(line.split('random')) > 1:
  resubmitFile.close()
  condorFile.close()
  # reseed the mac files, just for good measure, because it gets around a certain error when using v2
  if not arg.noSubmit:
    # submit to condor
    command = "condor_submit " + resubmitFile.name + "\n"
    subprocess.call(command.split())

if arg.jobdir != "all":
  if arg.nice:
    resubmit(arg.jobdir,"nice")
  elif arg.noNice:
    resubmit(arg.jobdir,"noNice")
  else:
    resubmit(arg.jobdir,"")

elif arg.jobdir == "all":
  dir_list=os.listdir("/local/cms/user/%s/LDMX/"%(os.environ["USER"]))
  for entry in dir_list:
    if arg.nice:
      resubmit("/local/cms/user/%s/LDMX/"%(os.environ["USER"])+entry,"nice")
    elif arg.noNice:
      resubmit("/local/cms/user/%s/LDMX/"%(os.environ["USER"])+entry,"noNice")
    else:
      resubmit("/local/cms/user/%s/LDMX/"%(os.environ["USER"])+entry,"")
