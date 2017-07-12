description = "a module housing helper routines for simulateGraceDBEvent and simulateGraceDBStream"

#-------------------------------------------------

import os

import numpy as np

import random

import schedule

import pipelines
import humans
import dq
import pe
import misc

#-------------------------------------------------
### scheduling fake events
#-------------------------------------------------

def dt( rate, distrib="poisson" ):
    """
    draws the time between events based on distrib and rate
    distrib can be either "poisson" or "uniform"
    """
    if distrib=="poisson":
        return poisson_dt( rate )
    elif distrib=="uniform":
        return uniform_dt( rate )
    else:
        raise ValueError("distrib=%s not understood"%distrib)

def uniform_dt( rate ):
    return 1.0/rate

def poisson_dt( rate ):
    return - np.log(1.0 - np.random.rand()) / rate

#-------------------------------------------------
# generate a schedule for a single event
#-------------------------------------------------

def genRandStr():
    alpha = 'A B C D E F G H I J K L M N O P Q R S TU V W Y X Z'.split()
    return "".join(random.choice(alpha) for _ in xrange(6))

def genSchedule(gps, far, instruments, config, safe=True, gdb_url='https://gracedb.ligo.org/api/', directory='.'):
    '''
    generates a schedule of actions for a single event based on the info in config
    '''
    graceDBevent = schedule.GraceDBEvent(genRandStr()) ### needs to be passed to all objects which make schedules

    sched = schedule.Schedule(t0=0)

    ### add schedule for event creation
    group = config.get('general', 'group')
    if safe and (group!='Test'):
        raise ValueError('in "safe" mode, we only allow group=Test')
    pipeline = config.get('general', 'pipeline')
    if config.has_option('general', 'search'):
        search = config.get('general', 'search')
    else:
        search = None
    if config.has_option('general', 'offline'):
        offline = config.getboolean('general', 'offline'):
    else:
        offline = False

    pipeObj = pipelines.initPipeline(gps, far, instruments, group, pipeline, graceDBevent, search=search, offline=offline, gdb_url=gdb_url)
    sched += pipeObj.genSchedule(directory=directory)

    ### add schedule for human interactions
    if config.has_section('humans'):
        request = config.getboolean('humans', 'request')
        respond = config.getboolean('humans', 'respond')
        if request or respond:
            requestDelay   = config.getfloat('humans', 'request delay')
            requestJitter  = config.getfloat('humans', 'request jitter')

            site_respondDelay  = config.getfloat('humans', 'site respond delay')
            site_respondJitter = config.getfloat('humans', 'site respond jitter')
            site_respondProb   = config.getfloat('humans', 'site respond prob')
            site_successProb   = config.getfloat('humans', 'site success prob')

            adv_respondDelay  = config.getfloat('humans', 'adv respond delay')
            adv_respondJitter = config.getfloat('humans', 'adv respond jitter')
            adv_respondProb   = config.getfloat('humans', 'adv respond prob')
            adv_successProb   = config.getfloat('humans', 'adv success prob')

            ### request signoff from each participating site
            for ifo in instruments:
                site = humans.Site( ifo, 
                                    graceDBevent, 
                                    gdb_url              = gdb_url,
                                    requestTimeout       = requestDelay, 
                                    requestJitter        = requestJitter, 
                                    respondTimeout       = site_respondDelay, 
                                    respondJitter        = site_respondJitter, 
                                    respondProb          = site_respondProb,
                                    respondProbOfSuccess = site_successProb, 
                                  )
                sched += site.genSchedule(request=request, respond=respond)

            ### EM Advocate responses
            adv = humans.Adv( graceDBevent, 
                              gdb_url              = gdb_url,
                              requestTimeout       = requestDelay,
                              requestJitter        = requestJitter,
                              respondTimeout       = adv_respondDelay,
                              respondJitter        = adv_respondJitter,
                              respondProb          = adv_respondProb,
                              respondProbOfSuccess = adv_successProb,
                            )
            sched += adv.genSchedule(request=request, respond=respond)

    ### add schedule for dq
    # VirgoDQ
    if config.has_section('VirgoDQ'):
        startDelay      = config.getfloat('VirgoDQ','start delay')
        startJitter     = config.getfloat('VirgoDQ','start jitter')
        startProb       = config.getfloat('VirgoDQ','start prob')
        
        ifostatsDelay   = config.getfloat('VirgoDQ','ifostats delay')
        ifostatsJitter  = config.getfloat('VirgoDQ','ifostats jitter')
        ifostatsProb    = config.getfloat('VirgoDQ','ifostats prob')
        
        vetoesDelay     = config.getfloat('VirgoDQ','vetoes delay')
        vetoesJitter    = config.getfloat('VirgoDQ','vetoes jitter')
        vetoesProb      = config.getfloat('VirgoDQ','vetoes prob')
        
        rmsChanDelay    = config.getfloat('VirgoDQ','rmsChan delay')
        rmsChanJitter   = config.getfloat('VirgoDQ','rmsChan jitter')
        rmsChanProb     = config.getfloat('VirgoDQ','rmsChan prob')
        
        injDelay        = config.getfloat('VirgoDQ','inj delay')
        injJitter       = config.getfloat('VirgoDQ','inj jitter')
        injProb         = config.getfloat('VirgoDQ','inj prob')
        
        pad_left        = config.getfloat('VirgoDQ', 'pad left')
        pad_right       = config.getfloat('VirgoDQ', 'pad right')
        
        virgodq     = dq.VirgoDQ(   graceDBevent,
                                    instruments,
                                    group,
                                    pipeline,
                                    gps-pad_left,
                                    pad_left+pad_right,
                                    startDelay      = startDelay,
                                    startJitter     = startJitter,
                                    startProb       = startProb,
                                    ifostatsDelay   = ifostatsDelay,
                                    ifostatsJitter  = ifostatsJitter,
                                    ifostatsProb    = ifostatsProb,
                                    vetoesDelay     = vetoesDelay,
                                    vetoesJitter    = vetoesJitter,
                                    vetoesProb      = vetoesProb,
                                    rmsChanDelay    = rmsChanDelay,
                                    rmsChanJitter   = rmsChanJitter,
                                    rmsChanProb     = rmsChanProb,
                                    injDelay        = injDelay,
                                    injJitter       = injJitter,
                                    injProb         = injProb,
                                    gdb_url         = gdb_url
                                )
        sched += virgodq.genSchedule(directory=directory)
        
    # idq
    if config.has_section('idq'):

        instruments = config.get('idq', 'instruments').split()
        classifiers = config.get('idq', 'classifiers').split()

        pad_left  = config.getfloat('idq', 'pad left')
        pad_right = config.getfloat('idq', 'pad right')

        maxFAP = config.getfloat('idq', 'maxFAP')
        minFAP = config.getfloat('idq', 'minFAP')

        startDelay  = config.getfloat('idq', 'start delay')
        startJitter = config.getfloat('idq', 'start jitter')
        startProb   = config.getfloat('idq', 'start prob')

        tablesDelay  = config.getfloat('idq', 'tables delay')
        tablesJitter = config.getfloat('idq', 'tables jitter')
        tablesProb   = config.getfloat('idq', 'tables prob')

        fapDelay  = config.getfloat('idq', 'fap delay')
        fapJitter = config.getfloat('idq', 'fap jitter')
        fapProb   = config.getfloat('idq', 'fap prob')

        gwfDelay  = config.getfloat('idq', 'gwf delay')
        gwfJitter = config.getfloat('idq', 'gwf jitter')
        gwfProb   = config.getfloat('idq', 'gwf prob')

        timeseriesDelay  = config.getfloat('idq', 'timeseries delay')
        timeseriesJitter = config.getfloat('idq', 'timeseries jitter')
        timeseriesProb   = config.getfloat('idq', 'timeseries prob')

        activeChanDelay  = config.getfloat('idq', 'activeChan delay')
        activeChanJitter = config.getfloat('idq', 'activeChan jitter')
        activeChanProb   = config.getfloat('idq', 'activeChan prob')

        calibDelay  = config.getfloat('idq', 'calib delay')
        calibJitter = config.getfloat('idq', 'calib jitter')
        calibProb   = config.getfloat('idq', 'calib prob')

        rocDelay  = config.getfloat('idq', 'roc delay')
        rocJitter = config.getfloat('idq', 'roc jitter')
        rocProb   = config.getfloat('idq', 'roc prob')

        statsDelay  = config.getfloat('idq', 'stats delay')
        statsJitter = config.getfloat('idq', 'stats jitter')
        statsProb   = config.getfloat('idq', 'stats prob')

        idq = dq.IDQ( graceDBevent,
                      instruments, 
                      classifiers,
                      gps-pad_left,
                      pad_left+pad_right,
                      maxFAP           = maxFAP,
                      minFAP           = minFAP,
                      gdb_url          = gdb_url,
                      startDelay       = startDelay,
                      startJitter      = startJitter,
                      startProb        = startProb,
                      tablesDelay      = tablesDelay,
                      tablesJitter     = tablesJitter,
                      tablesProb       = tablesProb,
                      fapDelay         = fapDelay,
                      fapJitter        = fapJitter,
                      fapProb          = fapProb,
                      gwfDelay         = gwfDelay,
                      gwfJitter        = gwfJitter,
                      gwfProb          = gwfProb,
                      timeseriesDelay  = timeseriesDelay,
                      timeseriesJitter = timeseriesJitter,
                      timeseriesProb   = timeseriesProb,
                      activeChanDelay  = activeChanDelay,
                      activeChanJitter = activeChanJitter,
                      activeChanProb   = activeChanProb,
                      calibDelay       = calibDelay,
                      calibJitter      = calibJitter,
                      calibProb        = calibProb,
                      rocDelay         = rocDelay,
                      rocJitter        = rocJitter,
                      rocProb          = rocProb,
                      statsDelay       = statsDelay,
                      statsJitter      = statsJitter,
                      statsProb        = statsProb,
                    )

        sched += idq.genSchedule(directory=directory)

    # segDB2grcDB
    if config.has_section('segDB2grcDB'):
        startDelay  = config.getfloat('segDB2grcDB', 'start delay')
        startJitter = config.getfloat('segDB2grcDB', 'start jitter')
        startProb   = config.getfloat('segDB2grcDB', 'start prob')

        flags = []
        for flag in config.get('segDB2grcDB', 'flags').split():
            if not config.has_section(flag):
                raise ValueError('could not find section for segDB flag=%s')

            delay  = config.getfloat(flag, 'delay')
            jitter = config.getfloat(flag, 'jitter')
            prob   = config.getfloat(flag, 'prob')
            win    = config.getfloat(flag, 'win')
            flags.append( (flag, (delay, jitter, prob), (gps-win, 2*win)) )
        
        segDB = dq.SegDB2GrcDB( graceDBevent, 
                                flags       = flags,
                                gdb_url     = gdb_url,
                                startDelay  = startDelay, 
                                startJitter = startJitter, 
                                startProb   = startProb,
                              )

        sched += segDB.genSchedule(directory=directory)

    ### add schedule for pe
    if config.has_section('plot skymaps'):
        plotDelay  = config.getfloat('plot skymaps', 'plotSkymap delay')
        plotJitter = config.getfloat('plot skymaps', 'plotSkymap jitter')
        plotProb   = config.getfloat('plot skymaps', 'plotSkymap prob')
    else:
        plotDelay  = 0.0
        plotJitter = 0.0
        plotProb   = 0.0

    if config.has_section('skyviewer'):
        skyviewerDelay  = config.getfloat('skyviewer', 'skyviewer delay')
        skyviewerJitter = config.getfloat('skyviewer', 'skyviewer jitter')
        skyviewerProb   = config.getfloat('skyviewer', 'skyviewer prob')
    else:
        skyviewerDelay  = 0.0
        skyviewerJitter = 0.0
        skyviewerProb   = 0.0

    # bayestar
    if config.has_section('bayestar'):
        lvem        = config.getboolean('bayestar', 'lvem')

        startDelay  = config.getfloat('bayestar', 'start delay')
        startJitter = config.getfloat('bayestar', 'start jitter')
        startProb   = config.getfloat('bayestar', 'start prob')

        finishDelay  = config.getfloat('bayestar', 'finish delay')
        finishJitter = config.getfloat('bayestar', 'finish jitter')
        finishProb   = config.getfloat('bayestar', 'finish prob')

        skymapDelay  = config.getfloat('bayestar', 'skymap delay')
        skymapJitter = config.getfloat('bayestar', 'skymap jitter')
        skymapProb   = config.getfloat('bayestar', 'skymap prob')

        bayestar = pe.Bayestar( graceDBevent,
                                gdb_url           = gdb_url,
                                startTimeout      = startDelay, 
                                startJitter       = startJitter, 
                                startProb         = startProb, 
                                skymapTimeout     = skymapDelay, 
                                skymapJitter      = skymapJitter, 
                                skymapProb        = skymapProb, 
                                finishTimeout     = finishDelay, 
                                finishJitter      = finishJitter, 
                                finishProb        = finishProb, 
                                plotSkymapTimeout = plotDelay, 
                                plotSkymapJitter  = plotJitter, 
                                plotSkymapProb    = plotProb, 
                                skyviewerTimeout  = plotDelay, 
                                skyviewerJitter   = plotJitter, 
                                skyviewerProb     = plotProb
                              )

        agenda = bayestar.genSchedule(directory=directory, lvem=lvem)

        for action in sched:
            print action
            if isinstance(action, schedule.WriteLog):
                if action.filename and (os.path.basename(action.filename)=="psd.xml.gz"):
                    agenda.bump( action.dt )
                    sched += agenda ### only add Bayestar stuff if we plan to upload a psd.xml.gz
                    break
        else: ### could not find trigger for Bayestar process...
            pass

    # lalinference
    if config.has_section('lalinference'):
        lvem        = config.getboolean('lalinference', 'lvem')

        startDelay  = config.getfloat('lalinference', 'start delay')
        startJitter = config.getfloat('lalinference', 'start jitter')
        startProb   = config.getfloat('lalinference', 'start prob')
        
        finishDelay  = config.getfloat('lalinference', 'finish delay')
        finishJitter = config.getfloat('lalinference', 'finish jitter')
        finishProb   = config.getfloat('lalinference', 'finish prob')

        skymapDelay  = config.getfloat('lalinference', 'skymap delay')
        skymapJitter = config.getfloat('lalinference', 'skymap jitter')
        skymapProb   = config.getfloat('lalinference', 'skymap prob')

        lalinf = pe.LALInference( graceDBevent,
                                  gdb_url           = gdb_url,
                                  startTimeout      = startDelay,
                                  startJitter       = startJitter,
                                  startProb         = startProb,
                                  skymapTimeout     = skymapDelay,
                                  skymapJitter      = skymapJitter,
                                  skymapProb        = skymapProb,
                                  finishTimeout     = finishDelay,
                                  finishJitter      = finishJitter,
                                  finishProb        = finishProb,
                                  plotSkymapTimeout = plotDelay,
                                  plotSkymapJitter  = plotJitter,
                                  plotSkymapProb    = plotProb,
                                  skyviewerTimeout  = plotDelay,
                                  skyviewerJitter   = plotJitter,
                                  skyviewerProb     = plotProb
                                )

        sched += lalinf.genSchedule(directory=directory, lvem=lvem)

    # lib
    if config.has_section('lib'):
        lvem        = config.getboolean('lib', 'lvem')

        startDelay  = config.getfloat('lib', 'start delay')
        startJitter = config.getfloat('lib', 'start jitter')
        startProb   = config.getfloat('lib', 'start prob')
        
        finishDelay  = config.getfloat('lib', 'finish delay')
        finishJitter = config.getfloat('lib', 'finish jitter')
        finishProb   = config.getfloat('lib', 'finish prob')

        skymapDelay  = config.getfloat('lib', 'skymap delay')
        skymapJitter = config.getfloat('lib', 'skymap jitter')
        skymapProb   = config.getfloat('lib', 'skymap prob')

        lib = pe.LIB( graceDBevent,
                      gdb_url           = gdb_url,
                      startTimeout      = startDelay,
                      startJitter       = startJitter,
                      startProb         = startProb,
                      skymapTimeout     = skymapDelay,
                      skymapJitter      = skymapJitter,
                      skymapProb        = skymapProb,
                      finishTimeout     = finishDelay,
                      finishJitter      = finishJitter,
                      finishProb        = finishProb,
                      plotSkymapTimeout = plotDelay,
                      plotSkymapJitter  = plotJitter,
                      plotSkymapProb    = plotProb,
                      skyviewerTimeout  = plotDelay,
                      skyviewerJitter   = plotJitter,
                      skyviewerProb     = plotProb
                    )

        sched += lib.genSchedule(directory=directory, lvem=lvem)

    # bayeswave
    if config.has_section('bayeswave'):
        lvem        = config.getboolean('bayeswave', 'lvem')

        startDelay  = config.getfloat('bayeswave', 'start delay')
        startJitter = config.getfloat('bayeswave', 'start jitter')
        startProb   = config.getfloat('bayeswave', 'start prob')
        
        finishDelay  = config.getfloat('bayeswave', 'finish delay')
        finishJitter = config.getfloat('bayeswave', 'finish jitter')
        finishProb   = config.getfloat('bayeswave', 'finish prob')

        skymapDelay  = config.getfloat('bayeswave', 'skymap delay')
        skymapJitter = config.getfloat('bayeswave', 'skymap jitter')
        skymapProb   = config.getfloat('bayeswave', 'skymap prob')

        bayeswave = pe.BayesWave( graceDBevent,
                                 gdb_url           = gdb_url,
                                 startTimeout      = startDelay,
                                 startJitter       = startJitter,
                                 startProb         = startProb,
                                 skymapTimeout     = skymapDelay,
                                 skymapJitter      = skymapJitter,
                                 skymapProb        = skymapProb,
                                 finishTimeout     = finishDelay,
                                 finishJitter      = finishJitter,
                                 finishProb        = finishProb,
                                 plotSkymapTimeout = plotDelay,
                                 plotSkymapJitter  = plotJitter,
                                 plotSkymapProb    = plotProb,
                                 skyviewerTimeout  = plotDelay,
                                 skyviewerJitter   = plotJitter,
                                 skyviewerProb     = plotProb
                               )

        sched += bayeswave.genSchedule(directory=directory, lvem=lvem)

    # cwbPE
    if config.has_section('cwbPE'):
        lvem        = config.getboolean('cwbPE', 'lvem')

        finishDelay  = config.getfloat('cwbPE', 'finish delay')
        finishJitter = config.getfloat('cwbPE', 'finish jitter')
        finishProb   = config.getfloat('cwbPE', 'finish prob')

        skymapDelay  = config.getfloat('cwbPE', 'skymap delay')
        skymapJitter = config.getfloat('cwbPE', 'skymap jitter')
        skymapProb   = config.getfloat('cwbPE', 'skymap prob')

        cwbpe = pe.CoherentWaveBurst( graceDBevent,
                                      gdb_url           = gdb_url,
                                      skymapTimeout     = skymapDelay,
                                      skymapJitter      = skymapJitter,
                                      skymapProb        = skymapProb,
                                      finishTimeout     = finishDelay,
                                      finishJitter      = finishJitter,
                                      finishProb        = finishProb,
                                      plotSkymapTimeout = plotDelay,
                                      plotSkymapJitter  = plotJitter,
                                      plotSkymapProb    = plotProb,
                                      skyviewerTimeout  = plotDelay,
                                      skyviewerJitter   = plotJitter,
                                      skyviewerProb     = plotProb
                                    )

        sched += cwbpe.genSchedule(directory=directory, lvem=lvem)

    ### add schedule for misc stuff
    # external triggers
    if config.has_section('external triggers'):
        timeout = config.getfloat('external triggers', 'delay')
        jitter  = config.getfloat('external triggers', 'jitter')
        respondProb = config.getfloat('external triggers', 'respond prob')
        successProb = config.getfloat('external triggers', 'success prob')

        exTrg = misc.ExternalTriggers( graceDBevent, 
                                       gdb_url       = gdb_url,
                                       timeout       = delay, 
                                       jitter        = jitter, 
                                       probOfReport  = respondProb, 
                                       probOfSuccess = successProb, 
                                     )
        sched += exTrg.genSchedule()

    # unblind injections
    if config.has_section('unblind injections'):
        timeout = config.getfloat('unblind injections', 'delay')
        jitter  = config.getfloat('unblind injections', 'jitter')
        respondProb = config.getfloat('unblind injections', 'respond prob')
        successProb = config.getfloat('unblind injections', 'success prob')

        unBld = misc.UnblindInjections( graceDBevent,
                                        gdb_url       = gdb_url,
                                        timeout       = delay,
                                        jitter        = jitter,
                                        probOfReport  = respondProb,
                                        probOfSuccess = successProb,
                                      ) 
        sched += unBld.genSchedule()

    ### we're done, so return the total schedule
    return sched
