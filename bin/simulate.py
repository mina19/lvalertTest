#!/usr/bin/python
usage       = "simulate.py [--options] config.ini config.ini config.ini ..."
description = "a script to submit fake triggers to an instance of GraceDB as well as simulated follow-up processes. Generates events according to either a uniform rate or a Poisson process."
author      = "reed.essick@ligo.org"

#-------------------------------------------------

import os

import time
import numpy as np

import random

import simUtils as utils
import schedule

from lal.gpstime import tconvert

from ConfigParser import SafeConfigParser

import traceback

from optparse import OptionParser

#-------------------------------------------------

parser = OptionParser( usage=usage, description=description )

parser.add_option("-v", "--verbose", default=False, action="store_true")
parser.add_option("-V", "--Verbose", default=False, action="store_true")

### testing options
parser.add_option("-s", "--unsafe-uploads", default=False, action="store_true", help="allow event creation with group!=Test")
parser.add_option("-T", "--test", default=False, action="store_true", help="do not actually perform actions, but only print what they are")

parser.add_option("-p", "--pause", default=5, type="float", help="the amount of time waited between constructing schedule and beginning it's exectution")

### options about gracedb
parser.add_option("-g", "--gracedb-url", default="https://gracedb.ligo.org/api/", type="string" )

### options about simulation
parser.add_option("",   "--distrib",    default="uniform", type="string", help="the distribution of events in time. Either \"poisson\" or \"uniform\"")

parser.add_option("-N", "--num-events", default=None, type="int", help="the number of events to simulate.")
parser.add_option("-D", "--duration",   default=None, type="float", help="the duration of the experiment specified in seconds.")

parser.add_option("-r", "--event-rate", default=0.1, type="float", help="the rate for simulating events specified in Hz")

parser.add_option('-i', '--instruments', default=None, type='string', help='a comma delimited list of participating IFOs')

### options about logging
parser.add_option("-o", "--output-dir", default=".", type="string")

opts, args = parser.parse_args()

### ensure we have at least one config
if not args:
    raise ValueError( "please supply at least one event config.ini file\n%s"%usage )

### figure out parameters of simulation
if not (opts.num_events or opts.duration):
    raise ValueError( "please supply either --num-events or --duration\n%s"%usage )
if opts.num_events and opts.duration:
    raise ValueError( "please supply either --num-events or --duration, but not both\n%s"%usage )

if opts.distrib not in ["poisson", "uniform"]:
    raise ValueError( "--distrib=%s not understood"%(opts.distib) )

if not opts.instruments:
    raise ValueError( '--please specify at least 2 IFO via --instruments' )
opts.instruments = opts.instruments.split(',')
if len(opts.instruments) < 1:
    raise ValueError( '--please specify at least 1 IFO via --instruments' )

### verbosity
opts.Verbose = opts.Verbose or opts.test
opts.verbose = opts.verbose or opts.Verbose

if not os.path.exists(opts.output_dir):
    os.makedirs(opts.output_dir)

### safe uploads
safe    = not opts.unsafe_uploads ### require only safe uploads
execute = not opts.test ### actually do the actions

#-------------------------------------------------

### load in config files for different event types
configs = []
for arg in args:
    if opts.verbose:
        print "reading in config from : %s"%arg
    config = SafeConfigParser()
    config.read( arg )
    configs.append( config )

#-------------------------------------------------

### generating simulated event times
waits = []

if opts.num_events: ### specified the number of events 
    if opts.verbose:
        print( "simulating %d events at a rate of %.3f Hz"%(opts.num_events, opts.event_rate) )
    t = 0.0
    while len(waits) < opts.num_events:
        deltaT = utils.dt( opts.event_rate, distrib=opts.distrib )
        waits.append( deltaT )
        t += deltaT

    if opts.verbose:    
        print( "drew %d events spaning %.3f sec at a rate of %.3f Hz"%(len(waits), np.sum(waits), opts.event_rate) )

elif opts.duration: ### specified the duration of the experiment
    if opts.verbose:
        print( "simulating %d seconds with an event rate of %.3f Hz"%(opts.duration, opts.event_rate) )
    t = 0.0
    while t < opts.duration:
        deltaT = utils.dt( opts.event_rate, distrib=opts.distrib )
        t += deltaT
        if t < opts.duration: ### only keep if it is within the specified duration
            waits.append( deltaT )

    if opts.verbose:    
        print( "drew %d events spaning %.3f sec at a rate of %.3f Hz"%(len(waits), opts.duration, opts.event_rate) )

if not len(waits):
    if opts.verbose:
        print "No times found!"
    import sys
    sys.exit(0)
waits[0] = 0 ### reset the first entry to be occur immediately

#-------------------------------------------------

### generate schedule for simulation
if opts.verbose:
    print "generating global schedule"

sched = schedule.Schedule()
delay = 0.0
t0 = time.time()
start_gps = float(tconvert('now'))
for ind, wait in enumerate(waits):
    if opts.verbose:
        print "generating schedule for event %d"%(ind)

    delay += wait ### increment how long we need to delay this event

    ### generate a schedule specifically for this event
    gps = start_gps + delay
    far = np.random.randint(1,10)*1e-9 ### FIXME: should probably randomly assign this...
    instruments = opts.instruments

    ### choose the event type
    config = random.choice( configs )

    ### generate agenda
    agenda = utils.genSchedule( gps, far, instruments, config, safe=safe, gdb_url=opts.gracedb_url, directory=opts.output_dir )
    agenda.setExpiration( t0+delay ) ### delay this by the associated wait time
    if opts.Verbose:
        for action in agenda:
            print "  ", action

    ### add to overall schedule
    sched += agenda

#-------------------------------------------------

## actually perform the actions
sched.bump( opts.pause ) ### bump everything by 10 seconds 
if opts.verbose:
    print """
-----------------------------------------------------------------
waiting ~%.3f seconds to ensure everything is sequenced correctly
-----------------------------------------------------------------
"""%opts.pause
time.sleep( opts.pause )

if opts.verbose:
    print "iterating through schedule"
for action in sched: ### iterate through actions in schedule
    action.wait(verbose=opts.Verbose) ### wait until action has expired

    if opts.verbose:
        print "  ", action

    if execute:
        try:
            action.execute() ### actually perform the action
        except Exception:
            traceback.print_exc()
            if raw_input("continue? (yes/no) : ")!="yes":
                raise KeyboardInterrupt 
