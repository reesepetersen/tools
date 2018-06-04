#!/usr/bin/env python

import os
import sys
import argparse
import re
import subprocess
#import fortranformat as ff

# get number of events
parser = argparse.ArgumentParser(description = "Makes an lhe file with --numEvents number of events that have a 1.2 GeV electron and 2.8 GeV photon starting just after the target.")
parser.add_argument("--numEvents", dest = "numEvents", help = "number of events in this lhe file", default =10, type = int)
parser.add_argument("--numFiles", dest = "numFiles", help = "number of lhe file to make", default =1, type = int)
arg = parser.parse_args()
# check for needed template folder and file
template_lhe_dir = "/home/%s/Projects/LDMX/SignalMC/4GeV_nodecay/0.001GeV_lhe"%(os.environ["USER"])
template_file = "SLAC.4.0GeV.W.map.0.001.1_unweighted_events.lhe"
if not os.path.exists(template_lhe_dir):
	print "%s not found. Exiting... \n"%(template_lhe_dir)
	quit()
if not os.path.isfile("%s/%s"%(template_lhe_dir,template_file)):
	print "%s/%s not found. Exiting... \n"%(template_lhe_dir,template_file)
	quit()
# make the EMST_lhe directory if it does not exist
EMST_lhe_dir = "/home/%s/Projects/LDMX/EMST_lhe"%(os.environ["USER"])
if os.path.exists(EMST_lhe_dir):
	print "%s already exists!"%(EMST_lhe_dir)
else:
	os.makedirs(EMST_lhe_dir)
# make a list with the new file names
new_EMST_lhe_file_names = []
newFileString = ""
newFileNum = 0
writeFlag = 0
while writeFlag == 0:
	if os.path.isfile(EMST_lhe_dir+"/EMST"+newFileString+".lhe"):
		print "%s/EMST%s.lhe already exists!"%(EMST_lhe_dir,newFileString)
		newFileNum = newFileNum + 1
		newFileString=str(newFileNum)
	else:
		print "Making %s/EMST%s.lhe"%(EMST_lhe_dir,newFileString)
		writeFlag=1
new_EMST_lhe_file_names.append("EMST"+newFileString+".lhe")
while writeFlag < arg.numFiles:
	newFileNum = newFileNum + 1
	newFileString=str(newFileNum)
	print "Making %s/EMST%s.lhe"%(EMST_lhe_dir,newFileString)
	new_EMST_lhe_file_names.append("EMST"+newFileString+".lhe")
	writeFlag = writeFlag + 1
# copy the needed lines from the template lhe to the new lhe
template_lhe = open("%s/%s"%(template_lhe_dir,template_file),"r")
for line in range(435):
	template_lhe.readline()
for line in range(4):	
	template_lhe.readline()
eventLines = []
for line in range(8):
	eventLines.append(template_lhe.readline())
# edit the lines
new_first_line = "3"+eventLines[1][3:len(eventLines[1])]
px = 0.0
py = 0.0
pz = 2.8
E = 2.8
m = 0.0
new_photon_line = "%9d    1    1    2    0    0  %.11E  %.11E  %.11E  %.11E  %.11E"%(22,px,py,pz,E,m)+eventLines[4][129:len(eventLines[4])]
new_electron_line = "%9d    1    1    2    0    0  %.11E  %.11E  %.11E  %.11E  "%(11,0.0,0.0,1.199999891,1.2)+eventLines[4][112:len(eventLines[4])]
del eventLines[6]
del eventLines[5]
del eventLines[3]
del eventLines[3]
eventLines.insert(3,new_electron_line)
del eventLines[1]
eventLines.insert(1,new_first_line)
eventLines.insert(4,new_photon_line)
template_lhe.close()
for filename in new_EMST_lhe_file_names:
	new_EMST_lhe = open("%s/%s"%(EMST_lhe_dir,filename),"w")
	for event in range(arg.numEvents):
		for line in eventLines:
			new_EMST_lhe.write(line)
	new_EMST_lhe.close()
quit()
