description = "a module that simulates Data Quality products"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import os

import random

import numpy as np

import schedule

#-------------------------------------------------

class SegDB2GrcDB():
    def __init__(self, graceDBevent, flags=[], startDelay=10, startJitter=1, startProb=1.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.startDelay  = startDelay
        self.startJitter = startJitter
        self.startProb   = startProb

        self.flags = flags

    def genFilename(self, flag, start, dur, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        filename = "%s%s-%d-%d.xml"%(dirname, flag.replace(":","_"), start, dur)
        open(filename, 'w').close() ### may want to do more than this...
        return filename

    def genSchedule(self, directory='.'):
        '''
        generate a schedule for SegDB2GraceDB uploads
        '''
        sched = schedule.Schedule()
        if random.random() < self.startProb:
            start_dt = max(0, random.normalvariate(self.startDelay, self.startJitter))
            message = 'began searching for segments in : fakeSegDB'
            sched.insert( schedule.WriteLog( start_dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )

            for flag, (delay, jitter, prob), (start, dur) in self.flags:
                if random.random() < prob:
                    filename = self.genFilename( flag, start, dur, directory=directory )
                    start_dt += max(0, random.normalvariate(delay, jitter))
                    sched.insert( schedule.WriteLog( start_dt, self.graceDBevent, flag, filename=filename, gdb_url=self.gdb_url ) )
                else:
                    break ### process failed, so we stop

            sched.insert( schedule.WriteLog( start_dt, self.graceDBevent, 'finished searching for segments in : fakeSegDB', gdb_url=self.gdb_url ) )

        return sched

class IDQ():
    def __init__(self, 
                 graceDBevent, 
                 instruments,
                 classifiers,
                 start, 
                 dur,
                 maxFAP = 1.0,
                 minFAP = 1e-5,
                 gdb_url = 'https://gracedb.ligo.org/api/',
                 startDelay  = 1.0,
                 startJitter = 0.5,
                 startProb   = 1.0,
                 tablesDelay  = 10.0,
                 tablesJitter = 1.0,
                 tablesProb   = 1.0,
                 fapDelay  = 5.0,
                 fapJitter = 1.0,
                 fapProb   = 1.0,
                 gwfDelay  = 5.0,
                 gwfJitter = 1.0,
                 gwfProb   = 1.0,
                 timeseriesDelay  = 5.0,
                 timeseriesJitter = 1.0,
                 timeseriesProb   = 1.0,
                 activeChanDelay  = 10, 
                 activeChanJitter = 1.0,
                 activeChanProb   = 1.0,
                 calibDelay  = 20.,
                 calibJitter = 5,
                 calibProb   = 1.0,
                 rocDelay  = 20,
                 rocJitter = 5,
                 rocProb   = 1.0,
                 statsDelay  = 30,
                 statsJitter = 5,
                 statsProb   = 1.0,
                ):

        ### store variables
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.classifiers = classifiers
        self.instruments = instruments

        self.minFAP = minFAP
        self.maxFAP = maxFAP

        self.start = start
        self.dur   = dur
        self.stop  = start+dur

        self.startDelay  = startDelay
        self.startJitter = startJitter
        self.startProb   = startProb

        self.tablesDelay  = tablesDelay
        self.tablesJitter = tablesJitter
        self.tablesProb   = tablesProb

        self.fapDelay  = fapDelay
        self.fapJitter = fapJitter
        self.fapProb   = fapProb

        self.gwfDelay  = gwfDelay
        self.gwfJitter = gwfJitter
        self.gwfProb   = gwfProb

        self.timeseriesDelay  = timeseriesDelay
        self.timeseriesJitter = timeseriesJitter
        self.timeseriesProb   = timeseriesProb

        self.activeChanDelay  = activeChanDelay
        self.activeChanJitter = activeChanJitter
        self.activeChanProb   = activeChanProb

        self.calibDelay  = calibDelay
        self.calibJitter = calibJitter
        self.calibProb   = calibProb

        self.rocDelay  = rocDelay
        self.rocJitter = rocJitter
        self.rocProb   = rocProb

        self.statsDelay  = statsDelay
        self.statsJitter = statsJitter
        self.statsProb   = statsProb

    def genTablesFilename(self, instrument, classifier, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        filename = "%s%s_idq_%s-%d-%d.xml.gz"%(dirname, instrument, classifier, self.start, self.dur)
        open(filename,'w').close()

        return filename

    def genFAPFilename(self, instrument, classifier, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        filename = "%s%s_%s-%d-%d.json"%(dirname, instrument, classifier, self.start, self.dur)
        open(filename,'w').close()

        return filename

    def genGWFFilenames(self, instrument, classifier, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        fapfilename = "%s%s_idq_%s_fap-%d-%d.gwf"%(dirname, instrument, classifier, self.start, self.dur)
        open(fapfilename,'w').close()
        rnkfilename = "%s%s_idq_%s_rank-%d-%d.gwf"%(dirname, instrument, classifier, self.start, self.dur)
        open(rnkfilename,'w').close()

        return fapfilename, rnkfilename

    def genTimeseriesFilename(self, instrument, classifier, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        filename = "%s%s_%s_timeseries-%d-%d.png"%(dirname, instrument, classifier, self.start, self.dur)
        open(filename,'w').close()

        return filename

    def genActiveChanFilename(self, instrument, classifier, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        jsonFilename = "%s%s_%s_chanlist-%d-%d.json"%(dirname, instrument, classifier, self.start, self.dur)
        open(jsonFilename,'w').close()
        pngFilename = "%s%s_%s_chanstrip-%d-%d.png"%(dirname, instrument, classifier, self.start, self.dur)
        open(pngFilename,'w').close()

        return jsonFilename, pngFilename

    def genCalibFilename(self, instrument, classifier, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        jsonFilename = "%s%s_%s_calib-%d-%d.json"%(dirname, instrument, classifier, self.start, self.dur)
        open(jsonFilename,'w').close()
        pngFilename = "%s%s_%s_calib-%d-%d.png"%(dirname, instrument, classifier, self.start, self.dur)
        open(pngFilename,'w').close()

        return jsonFilename, pngFilename

    def genROCFilename(self, instrument, classifier, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        jsonFilename = "%s%s_%s_ROC-%d-%d.json"%(dirname, instrument, classifier, self.start, self.dur)
        open(jsonFilename,'w').close()
        pngFilename = "%s%s_%s_ROC-%d-%d.png"%(dirname, instrument, classifier, self.start, self.dur)
        open(pngFilename,'w').close()

        return jsonFilename, pngFilename

    def genStatsFilenames(self, instrument, classifier, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        jsonFilename = "%s%s_%s_calibStats-%d-%d.json"%(dirname, instrument, classifier, self.start, self.dur)
        open(jsonFilename,'w').close()
        pngFilename = "%s%s_%s_trainStats-%d-%d.json"%(dirname, instrument, classifier, self.start, self.dur)
        open(pngFilename,'w').close()

        return jsonFilename, pngFilename

    def drawFAP(self):
        '''
        draw FAP so it is uniform in log(FAP) between self.minFAP and self.maxFAP
        '''
        return np.exp( np.log(self.minFAP) + random.random()*(np.log(self.maxFAP)-np.log(self.minFAP)) )

    def genSchedule(self, directory='.'):
        '''
        generate a schedule for iDQ uploads
        '''
        sched = schedule.Schedule()

        for instrument in self.instruments:
            if random.random() < self.startProb:
                dt = max(0, random.normalvariate(self.startDelay, self.startJitter))
                message = 'Started Searching for iDQ information within [%d, %d] at %s'%(self.start, self.stop, instrument)
                sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )
            
                for classifier in self.classifiers:
                    if random.random() < self.tablesProb:
                        dt += max(0, random.normalvariate(self.tablesDelay, self.tablesJitter))
                        message = 'iDQ glitch tables %s:'%instrument
                        filename = self.genTablesFilename(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, filename=filename, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.fapProb:
                        dt += max(0, random.normalvariate(self.fapDelay, self.fapJitter))
                        fap = self.drawFAP()
                        message = 'minimum glitch-FAP for %s at %s within [%d, %d] is %.6f'%(classifier, instrument, self.start, self.stop, fap)
                        filename = self.genFAPFilename(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, filename=filename, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.gwfProb:
                        dt += max(0, random.normalvariate(self.gwfDelay, self.gwfJitter))
                        fapMessage = 'iDQ fap timesereis for %s at %s within [%d, %d] :'%(classifier, instrument, self.start, self.stop)
                        rnkMessage = 'iDQ glitch-rank frame for %s at %s within [%d, %d] :'%(classifier, instrument, self.start, self.stop)
                        fapGWF, rnkGWF  = self.genGWFFilenames(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, fapMessage, filename=fapGWF, gdb_url=self.gdb_url ) )
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, rnkMessage, filename=rnkGWF, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.timeseriesProb:
                        dt += max(0, random.normalvariate(self.timeseriesDelay, self.timeseriesJitter))
                        message = 'iDQ fap and glitch-rank timeseries plot for %s at %s:'%(classifier, instrument)
                        pngFilename = self.genTimeseriesFilename(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, filename=pngFilename, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if 'ovl' not in classifier: ### warning! this is dangerous hard coding!
                        pass
                    elif random.random() < self.activeChanProb:
                        dt += max(0, random.normalvariate(self.activeChanDelay, self.activeChanJitter))
                        jsonMessage = 'iDQ (possible) active channels for %s at %s'%(classifier, instrument)
                        pngMessage = 'iDQ channel strip chart for %s at %s'%(classifier, instrument)
                        jsonFilename, pngFilename = self.genActiveChanFilename(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, jsonMessage, filename=jsonFilename, gdb_url=self.gdb_url ) )
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, pngMessage, filename=pngFilename, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.calibProb:
                        dt += max(0, random.normalvariate(self.calibDelay, self.calibJitter))
                        jsonMessage = 'iDQ calibration sanity check for %s at %s'%(classifier, instrument)
                        pngMessage = 'iDQ calibration sanity check figure for %s at %s'%(classifier, instrument)
                        jsonFilename, pngFilename = self.genCalibFilename(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, jsonMessage, filename=jsonFilename, gdb_url=self.gdb_url ) )
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, pngMessage, filename=pngFilename, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.rocProb:
                        dt += max(0, random.normalvariate(self.rocDelay, self.rocJitter))
                        jsonMessage = 'iDQ local ROC curves for %s at %s'%(classifier, instrument)
                        pngMessage = 'iDQ local ROC figure for %s at %s'
                        jsonFilename, pngFilename = self.genROCFilename(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, jsonMessage, filename=jsonFilename, gdb_url=self.gdb_url ) )
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, pngMessage, filename=pngFilename, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.statsProb:
                        dt += max(0, random.normalvariate(self.statsDelay, self.statsJitter))
                        calibMessage = 'iDQ local calibration vital statistics for %s at %s'%(classifier, instrument)
                        trainMessage = 'iDQ local training vital statistics for %s at %s'%(classifier, instrument)
                        calibFilename, trainFilename = self.genStatsFilenames(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, calibMessage, filename=calibFilename, gdb_url=self.gdb_url ) )
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, trainMessage, filename=trainFilename, gdb_url=self.gdb_url ) )
                    else:
                        break

                else: ### we made it all the way through! add finish statement
                    message = 'Finished searching for iDQ information within [%d, %d] at %s'%(self.start, self.stop, instrument)
                    sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )

        return sched
        
class VirgoDQ():
    '''
    Template for the VirgoDQ information
    #FIXME: Currently implementation will only report "DID/ DID 
    NOT FIND injections" for hardware injections from virgo
    '''
    def __init__(self, 
                 graceDBevent, 
                 instruments,
                 group,
                 pipeline,
                 start, 
                 dur,
                 startDelay = 1.0,
                 startJitter= 0.5,
                 startProb  = 1.0,
                 ifostatsDelay = 1.0,
                 ifostatsJitter= 0.5,
                 ifostatsProb  = 1.0,
                 vetoesDelay   = 1.0,
                 vetoesJitter  = 0.5,
                 vetoesProb    = 1.0,
                 rmsChanDelay  = 1.0,
                 rmsChanJitter = 0.5,
                 rmsChanProb   = 1.0,
                 injDelay      = 1.0,
                 injJitter     = 0.5,
                 injProb       = 0.0,
                 gdb_url = 'https://gracedb.ligo.org/api/'):
        self.graceDBevent   = graceDBevent
        self.instruments    = instruments
        self.group          = group
        self.pipeline       = pipeline
        self.start          = start
        self.dur            = dur
        self.stop           = start + dur
        
        self.startDelay     = startDelay
        self.startJitter    = startJitter
        self.startProb      = startProb
        
        self.ifostatsDelay  = ifostatsDelay
        self.ifostatsJitter = ifostatsJitter
        self.ifostatsProb   = ifostatsProb
        
        self.vetoesDelay    = vetoesDelay
        self.vetoesJitter   = vetoesJitter
        self.vetoesProb     = vetoesProb
        
        self.rmsChanDelay   = rmsChanDelay
        self.rmsChanJitter  = rmsChanJitter
        self.rmsChanProb    = rmsChanProb
        
        self.injDelay       = injDelay
        self.injJitter      = injJitter
        self.injProb        = injProb
        
        self.gdb_url        = gdb_url
        
    def genIFOStatusFilename(self, directory='.'):
        '''
        generate the IFO status file. Filename convention based on 
        uploaded gracedb test events
        '''
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.mkdirs(dirname)
        txtFileName = "%sVirgo_%s_%d_DQ_META.txt"%(dirname, self.graceDBevent.__graceid__, self.start)
        open(txtFileName,'w').close()
        return txtFileName
        
    def genRMSChanFilename(self, directory='.'):
        '''
        generate the IFO status file. Filename convention based on 
        uploaded gracedb test events
        '''
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.mkdirs(dirname)
        txtFileName = "%sVirgo_%s_%d_DQ_BRMSMon_FLAG.txt"%(dirname, self.graceDBevent.__graceid__, self.start)
        open(txtFileName,'w').close()
        return txtFileName
        
    def genSchedule(self, directory='.'):
        '''
        generate and return the schedule for Virgo DQ information
        '''
        # Create Schedule object
        sched = schedule.Schedule()
        if 'V1' not in self.instruments:
            # don't schedule anything, no action taken
            pass
        else:
            if random.random() <= self.startProb:
                dt = max(0, random.normalvariate(self.startDelay, self.startJitter))
                message = "Starting V1 detchar analysis: [%d;%d]"%(self.start, self.stop)
                sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )
                
                if random.random() <= self.ifostatsProb:
                    dt += max(0, random.normalvariate(self.ifostatsDelay, self.ifostatsJitter))
                    message = "Testing V1 interferometer status:"
                    filename = self.genIFOStatusFilename(directory = directory)
                    sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, filename=filename, gdb_url=self.gdb_url ) )
                    
                if random.random() <= self.vetoesProb:
                    #FIXME: Currently implementing "IS/IS NOT vetoed" only, needs more logic
                    dt += max(0, random.normalvariate(self.vetoesDelay, self.vetoesJitter))
                    message = "Testing %s V1 veto channel: this event IS NOT vetoed"%(self.pipeline,)
                    sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )
                else:
                    dt += max(0, random.normalvariate(self.vetoesDelay, self.vetoesJitter))
                    message = "Testing %s V1 veto channel: this event IS vetoed"%(self.pipeline,)
                    sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )
                    
                if random.random() <= self.rmsChanProb:
                    dt += max(0, random.normalvariate(self.rmsChanDelay, self.rmsChanJitter))
                    message = "Testing V1 band RMS channels:"
                    filename = self.genRMSChanFilename(directory = directory)
                    sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, filename=filename, gdb_url=self.gdb_url ) )
                    
                if random.random() < self.injProb:
                    #FIXME: Currently implementing "FOUND/ DID NOT FIND injections" 
                    #Need to implement what happens when injections are found
                    dt += max(0, random.normalvariate(self.injDelay, self.injJitter))
                    message = "Testing V1 hardware injection: FOUND injections"
                    sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )
                else:
                    dt += max(0, random.normalvariate(self.injDelay, self.injJitter))
                    message = "Testing V1 hardware injection: DID NOT FIND injections"
                    sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )
                # write finishing test
                message = "V1 detchar analysis finished"
                sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )
        return sched
