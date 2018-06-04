#! /usr/bin/env python

# useful libraries
import sys
import os
import re
import shutil

# taking an argument from the command line
parser = argparse.ArgumentParser(description = 'Looks through the contents of a given directory and tells you which, if any, jobs failed, and other stats about what has been analyzed and how.')
parser.add_argument('scanningDir', help='Give the full path to the folder that you want information about its contents')
parser.add_argument('mode', help='mode = trigger: count signal and background files and find which ones have been analyzed')
arg = parser.parse_args()

# checking for the given scanningDir or directory
if not os.path.exists(arg.scanningDir):
	print "Given directory does not exist! Exiting..."
	quit()

# make sure there is a slash at the end of the path to the scanning directory
if arg.scanningDir.split("/")[-1]=="":
	scanDir=arg.scanningDir
else:
	scanDir=arg.scanningDir+"/"

# make a list of entries in the scanDir
dir_list=os.listdir(scanDir)
print "Number of entries: {}".format(len(dir_list))

# if this is an EMST scan, find the number of files with sizes at or below 22 kB
for entry in dir_list:
  if os.path.isfile(scanDir+entry):
    print "found {}".format(entry)

if mode == "trigger":
  # this function will find the analyzed sets:
  def find_analyzed(input_list):
	analyzed_return_list = or item in input_list:
	for item in input_list:
		if os.path.isfile(loc+"%s/%s_histogram.root"%(item,item)):
			analyzed_return_list.append(item)
	return analyzed_return_list
  # this function will 
  # Get lists of background and signal jobs
  bacs = []
  sigs = []
  for item in dir_list:
	if item.split("s")[0] == "0":
		print "It's a background"
		bacs.append(item)
	elif item.split("s")[0] == "1":
		print "It's a signal"
		sigs.append(item)
	elif item.split("_")[0] == "EMST"
		print "It's an EM Shower Tail"
	else:
		print "What is %s?"%(item)
  bacs_analyzed = find_analyzed(bacs)
  sigs_analyzed = find_analyzed(sigs)
  bacs_simulated = bacs - bacs_analyzed
  sigs_simulated = bacs - sigs_analyzed
  #bac = int(item.split("b")[0].split("s")[-1])
  #setnum = int(item.split("_")[-1].split("set")[-1])
  #if sig > 0:
  #	mass = int(item.split("_")[2].split("MeV")[0])
  #	mass_index = masslist.index(mass)
  #	stats=[sig,bac,mass_index,setnum]
  #	sigs.append(stats)
  #else:
  #	stats=[sig,bac,setnum]
  #	bacs.append(stats)

  #masslist=[1,5,10,20,40,70,100,200,400,700,1000,1500]
  #for item in analyzed:
  #	sig = int(item.split("s")[0])
  #	bac = int(item.split("b")[0].split("s")[-1])
  #	setnum = int(item.split("_")[-1].split("set")[-1])
  #	if sig > 0:
  #		mass = int(item.split("_")[2].split("MeV")[0])
  #		mass_index = masslist.index(mass)
  #		stats=[sig,bac,mass_index,setnum]
  #		sigs.append(stats)
  #	else:
  #		stats=[sig,bac,setnum]
  #		bacs.append(stats)
  #bac_headers = 
  #histograms = len(analyzed)
  #print "Background Events"
  #print bac_headers
  #print "".format()
  #	if os.path.isfile(loc+"%s/%s_histogram_histogram.root"%(item,item)):
  #		histograms_histograms = histograms_histograms + 1
  #		print "WARNING: found %s_histogram_histogram.root"%(item)
  #		shutil.move(loc+"%s/%s_histogram_histogram.root"%(item,item),loc+"%s/%s_histogram.root"%(item,item))
  #		moved = moved + 1
  #	if os.path.isfile(loc+"%s/%s_histogram_histogram.root"%(item,item)) and os.path.isfile(loc+"%s/%s_histogram.root"%(item,item)):
  #		foundtwo = foundtwo + 1
  #		os.remove(loc+"%s/%s_histogram_histogram.root"%(item,item))
  #		removed = removed + 1
  #print "histogram.root's           : %d"%(histograms)
  #print "moved                      : %d"%(moved)
  #print "Found 2 *histogram.root's  : %d"%(foundtwo)
  #print "removed                    : %d"%(removed)
