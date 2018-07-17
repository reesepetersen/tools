#!/usr/bin/env python

import os,sys
import math
import argparse
import commands
import random
import subprocess
from time import strftime
import datetime

random.seed()

geometry_d = "v3"
beamspot_dx_d = 15
beamspot_dy_d = 35

usage = "usage: %prog [options]"
parser = argparse.ArgumentParser(usage)
parser.add_argument("--doPileup"  , dest="doPileup"  , help="Inject n additional particles into event (default = 0)", default=0, type=int)
#parser.add_argument("--enablePoisson" , dest="enablePoisson" , help="Poisson distribute number of e per event (default = False)", default=False, action="store_true")
parser.add_argument("--geometry"  , dest="geometry"  , help="specify geometry version to use (default = %s)"%(geometry_d), default=geometry_d, type=str)
parser.add_argument("--lhe"       , dest="lhe"    , help="directory containing lhe files or an lhe file", default="/default/")
parser.add_argument("--noLogging" , dest="noLogging" , help="disable logging capabilities (default enabled)", default=False, action="store_true")
parser.add_argument("--noSubmit"  , dest="noSubmit"  , help="do not submit to cluster", default=False, action="store_true")
parser.add_argument("--numEvents" , dest="numEvents" , help="number of events per job (required)", required=True, type=int)
parser.add_argument("--numJobs"   , dest="numJobs"   , help="number of jobs to run"   , default=-1, type=int)
parser.add_argument("--energy"    , dest="energy"   , help="electron energy in MeV (default = 4000 MeV)", default=4000, type=int)
parser.add_argument("--tag"       , dest="tag"   , help="tag to identify this job", default="")
parser.add_argument("--smearBeam" , dest="smearBeam" , help="smear the beamspot (default ({0}x{1}) off)".format(beamspot_dx_d,beamspot_dy_d), action="store_true")
parser.add_argument("--noPN"      , dest="noPN"      , help="disable the photonNuclear, electronNulcear, and positronNuclear processes (default enabled)", default = False, action="store_true")
parser.add_argument("--nonice"    , dest="nonice"    , help="Do not run this at nice priority (default nice)", action="store_true")
arg = parser.parse_args()

workingDir = "/export/scratch/users/%s"%(os.environ['USER'])
# create a submission log if one doesn't already exist
submit_log = open("Condor_Submit_Log.txt","a+")
submit_log.write("\n")

# Do some basic checks and echoes:
# doPileup:
if arg.doPileup < 0:
    print "Number of particles per event must be a positive integer! Exiting..."
    submit_log.write("Number of particles per event must be a positive integer! Exiting...\n")
    quit()
else:
  print "Pileup electrons: %d"%(arg.doPileup)
  submit_log.write("Pileup electrons: %d\n"%(arg.doPileup))

## enablePoisson:
#if arg.enablePoisson: 
#  print "Number of electrons is Poisson distributed"
#  submit_log.write("Number of electrons is Poisson distributed\n")

# geometry:
print "Using %s geometry"%(arg.geometry)
submit_log.write("Using %s geometry\n"%(arg.geometry))

# lhe:
if arg.lhe != "/default/":
  have_lhe = True
  if arg.lhe[-4:] == ".lhe":
    print "lhe file = {0:s}".format(arg.lhe)
    submit_log.write("lhe file = {0:s}\n".format(arg.lhe))
    lhedir = False
    lhefile = True
  elif arg.lhe[-1] == "/":
    print "lhe directory = {0:s}".format(arg.lhe)
    submit_log.write("lhe directory = {0:s}\n".format(arg.lhe))
    lhefile = False
    lhedir = True
  else:
    print "I don't recognize {0:s} \nExiting...".format(arg.lhe)
    submit_log.write("I don't recognize {0:s} \nExiting...\n".format(arg.lhe))
    quit()
  if len(arg.lhe.split("4GeV")) > 1:
    lhe_4GeV = True
    lhe_8GeV = False
    signalMassGeV_lhe = arg.lhe.split("/")[-2]
    MassNum = float(signalMassGeV_lhe.split("GeV")[0])*1000
  elif len(arg.lhe.split("8GeV")) > 1:
    lhe_8GeV = True
    lhe_4GeV = False
    signalMassGeV_lhe = arg.lhe.split("/")[-1]
    MassNum = float((signalMassGeV_lhe.split("map.")[1]).split(".alpha")[0])*1000
  signalMass = "_{0:.0f}MeV".format(MassNum)
  NumSig = 1
  NumBac = arg.doPileup
else:
  have_lhe = False
  signalMass = ""
  NumSig = 0
  NumBac = arg.doPileup+1

# noLogging:
if arg.noLogging:
  print "Logging disabled"
  submit_log.write("Logging disabled\n")

# noSubmit
if arg.noSubmit:
  print "Job not submitted"
  submit_log.write("Job not submitted\n")

# numEvents:
if arg.numEvents <= 0:
    print "Number of events per job must be a positive integer! Exiting..."
    submit_log.write("Number of events per job must be a positive integer! Exiting...\n")
    quit()
else:
  print "Doing {0:d} events per job.".format(arg.numEvents)
  submit_log.write("Doing {0:d} events per job.\n".format(arg.numEvents))

# numJobs:
if arg.numJobs > 0:
  numJobs = arg.numJobs
elif arg.numJobs == 0 or arg.numJobs < -1:
  print "numJobs = {0:d}, Exiting...".format(arg.numJobs)
  submit_log.write("numJobs = {0:d}, Exiting...\n".format(arg.numJobs))
  quit()
elif arg.lhe != "/default/":
  if lhedir:
    numJobs = len(os.listdir(arg.lhe))
  else:
    numJobs = 1
else:
  numJobs = 1
print "Number of jobs: {0:d}".format(numJobs)
submit_log.write("Number of jobs: {0:d}\n".format(numJobs))

# energy:
if arg.energy <= 5:
    print "Energy ~ Momentum not valid below Energy <= 10*m_e\nExiting..."
    submit_log.write("Energy ~ Momentum not valid below Energy <= 10*m_e\nExiting...\n")
    quit()
beamEnergy = "_"+str(arg.energy/1000)+"GeV"
if have_lhe:
  if lhe_4GeV:
    lhebeamEnergy = "_4GeV"
  elif lhe_8GeV:
    lhebeamEnergy = "_8GeV"
  if lhebeamEnergy != beamEnergy:
    if arg.doPileup:
      print "Electron beam energy: {0} != Signal lhe beam energy: {1}\nExiting...".format(beamEnergy[1:],lhebeamEnergy[1:])
      submit_log.write("Electron beam energy: {0} != Signal lhe beam energy: {1}\nExiting...".format(beamEnergy[1:],lhebeamEnergy[1:]))
      quit()
    else:
      beamEnergy = lhebeamEnergy
print "Electron beam energy: {0}".format(beamEnergy[1:])
submit_log.write("Electron beam energy: {0}\n".format(beamEnergy[1:]))

# tag:
if arg.tag != "":
  Tag = arg.tag+"_"
  print "Tag for this job: {}".format(arg.tag)
  submit_log.write("Tag for this job: {}\n".format(arg.tag))
else:
  Tag = ""

# smearBeam:
beam_dx = beamspot_dx_d
beam_dy = beamspot_dy_d
if arg.smearBeam:
  smear = "_beamspot{0}x{1}".format(beam_dx,beam_dy)
  print "smearBeam: on"
  submit_log.write("smearBeam: on\n")
  print "Beam size: {0} by {1}".format(beam_dx,beam_dy)
  submit_log.write("Beam size: {0} by {1}\n".format(beam_dx,beam_dy))
else:
  smear = ""
  print "smearBeam: off"
  submit_log.write("smearBeam: off\n")

# noPN:
if arg.noPN:
  PN = "_noPN_noEN"
  print "Photonuclear and Electronuclear processes disabled."
  submit_log.write("Photonuclear and Electronuclear processes disabled.\n")
else:
  PN = "_inclusive"
  print "Photonuclear and Electronuclear processes enabled."
  submit_log.write("Photonuclear and Electronuclear processes enabled.\n")

# nonice:
if arg.nonice:
  print "Submitting at high priority (no nice)."
  submit_log.write("Submitting at high priority (no nice).\n")
else:
  print "Submitting at low priority (nice)."
  submit_log.write("Submitting at low priority (nice).\n")

# check whether the muon conversion physics process is enabled
physics_file = open("/home/{0}/LDMX/ldmx-sw/SimApplication/src/GammaPhysics.cxx".format(os.environ['USER']),"r")
lines = physics_file.readlines()
line_check = -1
for line in lines:
  line_check = line.find("gammaConvProcess")
  if line_check > -1:
    comment_out_check = line.find("//")
    if comment_out_check > -1:
      gamma_mumu = "_noGammaMumu"
      print "Gamma -> mu mu process disabled in ldmx-sw/SimApplication/src/GammaPhysics.cxx"
      submit_log.write("Gamma -> mu mu process disabled in ldmx-sw/SimApplication/src/GammaPhysics.cxx\n")
    else:
      gamma_mumu = "_withGammaMumu"
      print "Gamma -> mu mu process enabled in ldmx-sw/SimApplication/src/GammaPhysics.cxx"
      submit_log.write("Gamma -> mu mu process enabled in ldmx-sw/SimApplication/src/GammaPhysics.cxx\n")

# give a standarized name to the output directory: tag_#s_signalMass(MeV)_#b_beamEnergy(GeV)_v#(geometry version)_totalEvents_beamSmear_PN_GammaMumu_year_month_day
now = datetime.datetime.now()
syear = "_"+str(now.year)
smonth = "_"+str(now.month)
sday = "_"+str(now.day)

if arg.numEvents*numJobs == 10:
  totalEvents = "_10"
elif arg.numEvents*numJobs == 100:
  totalEvents = "_100"
elif arg.numEvents*numJobs == 1000:
  totalEvents = "_1k"
elif arg.numEvents*numJobs == 10000:
  totalEvents = "_10k"
elif arg.numEvents*numJobs == 100000:
  totalEvents = "_100k"
elif arg.numEvents*numJobs == 1000000:
  totalEvents = "_1M"
elif arg.numEvents*numJobs == 10000000:
  totalEvents = "_10M"
elif arg.numEvents*numJobs == 100000000:
  totalEvents = "_100M"
elif arg.numEvents*numJobs == 1000000000:
  totalEvents = "_1B"
else:
  totalEvents = "_"+str(arg.numEvents*numJobs)
print "Total number of events: {0}".format(totalEvents[1:])
submit_log.write("Total number of events: {0}\n".format(totalEvents[1:]))

outputName = Tag+str(NumSig)+"s"+signalMass+"_"+str(NumBac)+"b"+beamEnergy+"_"+arg.geometry+totalEvents+"_events"+smear+PN+gamma_mumu+syear+smonth+sday

# make the output directory
outputDir = "/local/cms/user/%s/LDMX/simulation/"%(os.environ['USER'])+outputName
if os.path.exists(outputDir):
  print "Output directory already exists, exiting..."
  quit()
os.makedirs(outputDir)
if not os.path.exists(outputDir):
    print "\nUnable to create output directory \"%s\"\nExiting..."%(outputDir)
    submit_log.write("\nUnable to create output directory \"%s\"\nExiting..."%(outputDir))
    quit()
print "Output: {0}".format(outputDir)
submit_log.write("Output: {0}\n".format(outputDir))

# make condor, logs, and mac directories
condorDir="%s/condor"%(outputDir)
os.makedirs(condorDir)
logDir="%s/logs"%(outputDir)
os.makedirs(logDir)
macDir="%s/mac"%(outputDir)
os.makedirs(macDir)

# Write .sh script to be run by Condor
scriptFile = open("%s/runJob.sh"%(condorDir), "w")
scriptFileName=scriptFile.name
scriptFile.write("#!/bin/bash\n\n")
scriptFile.write("STUBNAME=$1\n")
scriptFile.write("OUTPATH=$2\n")
scriptFile.write("mkdir -p %s;cd %s\nmkdir ${STUBNAME}\ncd ${STUBNAME}\n"%(workingDir,workingDir))
scriptFile.write("hostname > ${STUBNAME}.log\n")
scriptFile.write("source ${HOME}/bin/ldmx-sw_setup.sh >> ${STUBNAME}.log 2>>${STUBNAME}.err\n")
scriptFile.write("ln -s ${LDMXBASE}/ldmx-sw/BmapCorrected3D_13k_unfolded_scaled_1.15384615385.dat .\n")
scriptFile.write("ln -s ${LDMXBASE}/ldmx-sw/Detectors/data/ldmx-det-full-%s-fieldmap/* .\n"%(arg.geometry))
scriptFile.write("date >> ${STUBNAME}.log\n")
scriptFile.write("ldmx-sim ${OUTPATH}/mac/${STUBNAME}.mac >> ${STUBNAME}.log 2>>${STUBNAME}.err\n")
scriptFile.write("date >> ${STUBNAME}.log\n")
scriptFile.write("cp ldmx_sim_events.root ${OUTPATH}/${STUBNAME}.root >> ${STUBNAME}.log 2>>${STUBNAME}.err\n")
scriptFile.write("xz *.log *.err\n")
scriptFile.write("cp *.xz ${OUTPATH}/logs\n")
scriptFile.write("cd .. && rm -r ${STUBNAME}\n")
scriptFile.close()

# Write Condor submit file 
condorSubmit = open("%s/condorSubmit"%(condorDir), "w")
condorSubmit.write("Executable          =  %s\n"%(scriptFile.name))
condorSubmit.write("Universe            =  vanilla\n")
condorSubmit.write("Requirements        =  Arch==\"X86_64\"  &&  (Machine  !=  \"scorpion6.spa.umn.edu\")  &&  (Machine  !=  \"zebra02.spa.umn.edu\")  &&  (Machine  !=  \"zebra03.spa.umn.edu\")  &&  (Machine  !=  \"zebra04.spa.umn.edu\")\n")
condorSubmit.write("+CondorGroup        =  \"cmsfarm\"\n")
condorSubmit.write("getenv              =  True\n")
if not (arg.nonice):
    condorSubmit.write("nice_user = True\n")
condorSubmit.write("Request_Memory      =  1 Gb\n")

# write the mac file
for job in range(numJobs):
    stubname="%s_%04d"%(outputName,job)
    condorSubmit.write("Arguments       = %s %s\n"%(stubname,outputDir))
    condorSubmit.write("Queue\n")
    g4Macro = open("%s/%s.mac"%(macDir,stubname),"w")
    g4Macro.write("/persistency/gdml/read detector.gdml\n\n")
    if arg.noPN:
        g4Macro.write("/ldmx/plugins/load DisablePhotoNuclear libSimPlugins.so\n\n")
    g4Macro.write("/run/initialize\n\n")
    if have_lhe:
      if os.path.exists(arg.lhe) and lhedir:
        num_lhe_files = len(os.listdir(arg.lhe))
        g4Macro.write("/ldmx/generators/lhe/open %s/%s\n\n"%(arg.lhe,os.listdir(arg.lhe)[job-(job/num_lhe_files)*num_lhe_files]))
      elif lhefile:
        g4Macro.write("/ldmx/generators/lhe/open %s\n"%(arg.lhe))
      else:
        print "No valid lhe file or directory given\n"
        print "arg.lhe: {0}\n".format(arg.lhe)
    if arg.smearBeam:
        g4Macro.write("/ldmx/generators/beamspot/enable\n")
        g4Macro.write("/ldmx/generators/beamspot/sizeX {0}\n".format(float(beam_dx)))
        g4Macro.write("/ldmx/generators/beamspot/sizeY {0}\n\n".format(float(beam_dy)))

    if arg.doPileup > 0 or not have_lhe:
        g4Macro.write("\n/ldmx/generators/mpgun/enable\n")
        #if arg.enablePoisson:
        #    g4Macro.write("/ldmx/generators/mpgun/enablePoisson\n\n")

	g4Macro.write("/ldmx/generators/mpgun/nInteractions %s\n"%(arg.doPileup+1))
        g4Macro.write("/ldmx/generators/mpgun/pdgID 11\n")
        g4Macro.write("/ldmx/generators/mpgun/vertex 0 0 20 mm\n")
        g4Macro.write("/ldmx/generators/mpgun/momentum 0 0 %s MeV\n"%(arg.energy))

    g4Macro.write("\n/random/setSeeds %d %d\n"%(random.uniform(0,100000000),random.uniform(0,100000000)))
    g4Macro.write("/run/beamOn %d\n"%(arg.numEvents))
    g4Macro.close()
    
condorSubmit.close()

os.system("chmod u+rwx %s"%(scriptFileName))

submit_log.write("Submission setup completed.\n")

if arg.noSubmit:
  submit_log.write("Job(s) not submitted.\n\n")
  submit_log.close()
  quit()
else:
  submit_log.write("Job(s) submitted.\n\n")
  submit_log.close()

command = "condor_submit " + condorSubmit.name + "\n"
subprocess.call(command.split())
