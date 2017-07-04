description = "a module that simulates how humans will interact with the automatic vetting process"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import random

import schedule

#-------------------------------------------------

### define parent class

class HumanSignoff(object):
    '''
    a class that represents a human's signoff or more general interaction with the automatic vetting process
    '''
    name = 'human'

    def __init__(self, graceDBevent, respondTimeout=60.0, respondJitter=10.0, respondProb=1.0, respondProbOfSuccess=1.0, requestTimeout=0.0, requestJitter=0.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent ### pointer to shared object that will contain graceid assigned to this event
        self.gdb_url = gdb_url

        ### request options
        self.requestTimeout = requestTimeout
        self.requestJitter  = requestJitter

        ### response options
        self.respondTimeout = respondTimeout ### mean amount of time we wait
        self.respondJitter  = respondJitter  ### the stdv of the jitter aound self.timeout
        self.respondProb    = respondProb ### probability of actually responding
        self.respondSuccess = respondProbOfSuccess ### probablity of returning OK

    def request(self):
        '''
        generate label for request
        '''
        return "%sREQ"%self.name

    def decide(self):
        '''
        flip a coinc and decide if we get an OK or a NO label
        '''
        if random.random() < self.respondSuccess: ### we succeed -> OK label
            return "OK"
        else: ### we reject -> NO label
            return "NO"

    def genSchedule(self, request=True, respond=True):
        '''
        generate a schedule for this human signoff
        '''
        sched = schedule.Schedule()
        if request:
            request_dt = max(0, random.normalvariate(self.requestTimeout, self.requestJitter) )
            request = schedule.WriteLabel( request_dt, self.graceDBevent, self.request(), gdb_url=self.gdb_url )
            sched.insert( request )
        if respond and (random.random() < self.respondProb):
            respond_dt = max(0, random.normalvariate(self.respondTimeout, self.respondJitter))
            if request:
                respond_dt = max(request_dt, respond_dt)
                ### currently, RemoveLable is not implemented...
#                remove = schedule.RemoveLabel( respond_dt, self.graceDBevent, self.request(), gdb_url=self.gdb_url )
#                sched.insert( remove )
            respond = schedule.WriteSignoff( respond_dt, self.graceDBevent, self.instrument, self.signoff_type, self.decide(), gdb_url=self.gdb_url)
            sched.insert( respond )
        return sched

#------------------------

### define daughter classes

class Site(HumanSignoff):
    '''
    signoff from a particular site
    '''
    knownSites = ['H1', 'L1', 'V1']

    def __init__(self, siteName, graceDBevent, respondTimeout=60.0, respondJitter=10.0, respondProb=1.0, respondProbOfSuccess=1.0, requestTimeout=0.0, requestJitter=0.0, gdb_url='https://gracedb.ligo.org/api/'):
        assert siteName in self.knownSites, 'siteName=%s is not in the list of known sites'%siteName ### ensure we know about this site
        self.name = siteName
        self.instrument = siteName
        self.signoff_type = 'OP'
        super(Site, self).__init__(graceDBevent, 
                                   gdb_url=gdb_url,
                                   requestTimeout=requestTimeout, 
                                   requestJitter=requestTimeout, 
                                   respondTimeout=respondTimeout, 
                                   respondJitter=respondJitter, 
                                   respondProb=respondProb, 
                                   respondProbOfSuccess=respondProbOfSuccess, 
                                  )

    def request(self):
        return "%sOPS"%self.name

class Adv(HumanSignoff):
    '''
    signoff from EM Advocates
    '''
    def __init__(self, graceDBevent, respondTimeout=60.0, respondJitter=10.0, respondProb=1.0, respondProbOfSuccess=1.0, requestTimeout=0.0, requestJitter=0.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.name = 'ADV'
        self.instrument = ''
        self.signoff_type = 'ADV'
        super(Site, self).__init__(graceDBevent, 
                                   gdb_url=gdb_url,
                                   requestTimeout=requestTimeout, 
                                   requestJitter=requestTimeout,
                                   respondTimeout=respondTimeout,
                                   respondJitter=respondJitter,
                                   respondProb=respondProb,
                                   respondProbOfSuccess=respondProbOfSuccess,
                                  )
