description = "a module that dummies-up GraceDb interface for testing purposes"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import os
import glob
import shutil

import getpass

import pickle
import json

import time

import numpy as np

from glue.ligolw import utils as ligolw_utils
from glue.ligolw import ligolw
from glue.ligolw import table
from glue.ligolw import lsctables

from ligoTest.lvalert import lvalertTestUtils as lvutils

#-------------------------------------------------

class FakeTTPResponse():
    """
    a "fake" httpResponse that provides some basic functionality
    """

    def __init__(self, data):
        self.data = json.dumps( data )

    def read(self):
        ans = self.data
        self.data = ''
        return ans

    def json(self):
        return json.loads( self.read() )

class FakeTTPError(Exception):
    """
    a "fake" httpError
    """

#-------------------------------------------------

class FakeDb():
    """
    a "fake" GraceDb that provides some basic functionality managed through the local filesystem.

    NOTE: we do NOT support the full functionality of GraceDb
    """
    __group2letter__ = {'CBC'   : 'G', 
                        'Burst' : 'G',
                        'Test'  : 'T',
                        'MDC'   : 'M',
                       }

    __allowedGroupPipelineSearch__ = {'Test'  : {'CWB'          : ['AllSky', None],
                                                 'LIB'          : ['AllSky', None],
                                                 'pycbc'        : ['AllSky', None],
                                                 'gstlal'       : ['LowMass', 'HighMass', None], 
                                                 'gstlal-spiir' : ['LowMass', 'HighMass', None],
                                                 'MBTAOnline'   : ['LowMass', 'HighMass', None],
                                                },
                                      'CBC'   : {'pycbc'        : ['AllSky', None],
                                                 'gstlal'       : ['LowMass', 'HighMass', None], 
                                                 'gstlal-spiir' : ['LowMass', 'HighMass', None],
                                                 'MBTAOnline'   : ['LowMass', 'HighMass', None],
                                                },
                                      'Burst' : {'CWB'          : ['AllSky', None],
                                                 'LIB'          : ['AllSky', None]
                                                },
                                     }

    __allowedLabels__ = ['EM_Throttled',
                         'EM_Selected', 'EM_Superseded', 
                         'EM_READY', 
                         'PE_READY', 
                         'DQV', 
                         'INJ', 
                         'ADVREQ', 'ADVOK', 'ADVNO', 
                         'H1OPS', 'H1OK', 'H1NO',
                         'L1OPS', 'L1OK', 'L1NO',
                        ]

    ### basic instantiation ###

    def __init__(self, directory='.'):
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.home = directory
        self.lvalert = os.path.join(directory, 'lvalert.out') ### file into which we write lvalert messages

    ### write lvalert messages into a file ###

    def sendlvalert(self, message, node ):
        file_obj = open(self.lvalert, 'a')
        print >> file_obj, lvutils.alert2line(node, json.dumps(message))
        file_obj.close()

    def __node__(self, graceid):
        '''
        figures out the node name given a graceid
        '''
        event = self.event(graceid).json() ### load in the parameters

        return "%s_%s_%s"%(event['group'], event['pipeline'], event['search']) if event.has_key('search') else "%s_%s"%(event['group'], event['pipeline'])
                
    ### conditionals on allowed actions ###

    def check_group_pipeline_search(self, group, pipeline, search):
        if not self.__allowedGroupPipelineSearch__.has_key(group):
            raise FakeTTPError('bad group : %s'%group)

        if not self.__allowedGroupPipelineSearch__[group].has_key(pipeline):
            raise FakeTTPError('bad group, pipeline : %s, %s'%(group, pipeline))

        if search not in self.__allowedGroupPipelineSearch__[group][pipeline]:
            raise FakeTTPError('bad group, pipeline, search : %s, %s, %s'%(group, pipeline, search))

    def check_label(self, label):
        if label not in self.__allowedLabels__:
            raise FakeTTPError('label=%s not allowed'%label)

    def check_graceid(self, graceid):
        if not os.path.exists(self.__directory__(graceid)):
            raise FakeTTPError('could not find graceid=%s'%graceid)



    ### generic utils and data management ###

    def __is_label__(self, label):
        return label in self.__allowedLabels__

    def __is_graceid__(self, graceid):
        if (graceid[0] in self.__group2letter__.values()): ### ensure it starts with the correct letter
            try:
                num = int(graceid[1:]) ### ensure we can convert this to an integer
                return True

            except ValueError as e:
                return False

        else:
            return False

    def __get_all_graceids__(self):
        graceids = []
        for path in os.listdir(self.home):
            path = os.path.basename(path)
            if self.__is_graceid__(path):
                graceids.append( path )
        return graceids

    def __genGraceID__(self, group):
        '''
        looks up the known GraceIDs within the directory.
        returns the biggest one +1
        if none exist, starts at 000000
        '''
        existing = [int(graceid[1:]) for graceid in self.__get_all_graceids__()]
        if existing:
            ind = max(existing)+1
        else:
            ind = 0

        return "%s%06d"%(self.__group2letter__[group], ind)
            
    def __directory__(self, graceid):
        '''
        generates the directory associated with this graceid
        '''
        return os.path.join(self.home, graceid)

    def __topLevelPath__(self, graceid):
        return os.path.join(self.home, graceid, 'toplevel.pkl')

    def __filesPath__(self, graceid):
        return os.path.join(self.home, graceid, 'files.pkl')

    def __labelsPath__(self, graceid):
        return os.path.join(self.home, graceid, 'labels.pkl')

    def __logsPath__(self, graceid):
        return os.path.join(self.home, graceid, 'logs.pkl')

    def __path2len__(self, path):
        return len(self.__extract__(path))

    def __append__(self, stuff, path):
        '''append to pkl file'''
        ans = self.__extract__(path)
        ans.append(stuff)
        self.__write__(ans, path)

        return len(ans)-1

    def __write__(self, stuff, path):
        '''write stuff into pkl file'''
        file_obj = open(path, 'w')
        pickle.dump(stuff, file_obj)
        file_obj.close()

    def __extract__(self, path):
        '''read from pkl file'''
        file_obj = open(path, 'r')
        ans = pickle.load(file_obj)
        file_obj.close()

        return ans

    def __createDirectory__(self, graceid):
        '''
        generate local data structure for this graceid
        '''
        d = self.__directory__(graceid)
        if os.path.exists(d):
            raise ValueError('graceid=%s already exists!'%graceid)
        else:
            os.makedirs(d) ### make directory
            ### touch a bunch of files to make sure they exist
            paths = [self.__filesPath__(graceid), 
                     self.__labelsPath__(graceid), 
                     self.__logsPath__(graceid),
                    ]
            for path in paths:
                file_obj = open(path, 'w')
                pickle.dump([], file_obj)
                file_obj.close()

    def __newfilename__(self, graceid, filename):
        return os.path.join(self.home, graceid, os.path.basename(filename))

    def __copyFile__(self, graceid, filename):
        newFilename = self.__newfilename__(graceid, filename)
        shutil.copyfile(filename, newFilename)
        self.__append__( newFilename, self.__filesPath__(graceid) )

    ### insertion ###

    def __createEvent__(self, graceid, group, pipeline, filename, search=None):
        labelsPath = self.__labelsPath__(graceid)
        jsonD = {'graceid':graceid,
                 'group'  :group,
                 'pipeline':pipeline,
                 'created':time.time(),
                 'submitter':getpass.getuser()+'@ligo.org',
                 'labels' : dict((label, labelsPath) for label in self.__extract__(self.__labelsPath__(graceid))), ### NOTE: this is overkill for now, but we may want to support labeling during event creation, at which point we will want to perform this query.
                 'links': {'neighbors':'',
                           'files':self.__filesPath__(graceid),
                           'log':self.__logsPath__(graceid),
                           'tags':'',
                           'self':self.__directory__(graceid),
                           'labels':labelsPath,
                           'filemeta':self.__topLevelPath__(graceid),
                           'emobservations':'',
                          },
                }
        if search!=None:
            jsonD['search'] = search

        ### extract these by parsing filename!
        jsonD.update( self.__file2extraattributes__(pipeline, filename) )
         
        ### write top level data to file 
        self.__write__( jsonD, self.__topLevelPath__(graceid) )

        lvalert = {"alert_type": "new",
                   "description": "",
                   "file": self.__newfilename__(graceid, filename),
                   "object": jsonD,
                   "uid": graceid,
                  }

        return jsonD, lvalert

    def __file2extraattributes__(self, pipeline, filename):
        if pipeline.lower() == 'cwb':
            file_obj = open(filename, 'r')
            ans = {'extra_attributes':{
                                      },
                  }

            far = np.infty

            readme = False
            for line in file_obj:
                try:
                    key, val = line.split(':')
                    key = key.strip()
                    if key == "likelihood":
                        ans['likelihood'] = float(val.strip())
                    elif key == 'time':
                        ans['gpstime'] = float(val.split()[0].strip())
                    elif key == 'ifo':
                        ans['instruments'] = ",".join(val.split())
                except:
                    if readme:
                        fields = [float(_) for _ in line.strip().split()]
                        ans['far'] = fields[1]
                        break
                    
                    readme = "significance based on " in line ### next line is a FAR statement
                        
        elif pipeline.lower() == 'lib':
            file_obj = open(filename, 'r')
            a = json.loads( file_obj.read() )
            file_obj.close()

            ### old oLIB format (pre-O2)
#            ans = {'gpstime'    : a['gpstime'],
#                   'FAR'        : a['FAR'],
#                   'instruments': a['instruments'],
#                   'likelihood' : a['likelihood'],
#                   'nevents'    : a['nevents'],
#                   'extra_attributes' : {'raw FAR'    : a['raw FAR'],
#                                         'BCI'        : a['BCI'], 
#                                         'BSN'        : a['BSN'],
#                                         'Omicron SNR': a['Omicron SNR'],
#                                         'timeslides' : a['timeslides'], 
#                                         'hrss'       : a['hrss'],
#                                         'frequency'  : a['frequency'],
#                                         'quality'    : a['quality'],
#                                        },
#                  }

            ans = {'gpstime'    : a['gpstime'],
                   'far'        : a['FAR'],
                   'instruments': a['instruments'],
                   'likelihood' : a['likelihood'],
                   'nevents'    : a['nevents'],
                   'extra_attributes' :
                       { "LalInferenceBurst": 
                           {"omicron_snr_network" : a['Omicron_SNR_Network'], 
                            "omicron_snr_H1"      : a['Omicron_SNR_H1'] if 'H1' in a['instruments'] else None, 
                            "omicron_snr_L1"      : a['Omicron_SNR_L1'] if 'L1' in a['instruments'] else None, 
                            "omicron_snr_V1"      : a['Omicron_SNR_V1'] if 'V1' in a['instruments'] else None,
                            "bsn"                 : a['BSN'], 
                            "bci"                 : a['BCI'], 
                            "quality_median"      : a['quality_posterior_median'], 
                            "quality_mean"        : a['quality_posterior_mean'], 
                            "frequency_median"    : a['frequency_posterior_median'], 
                            "frequency_mean"      : a['frequency_posterior_mean'],
                            "hrss_mean"           : a['hrss_posterior_median'], 
                            "hrss_median"         : a['hrss_posterior_mean'], 
                           },
                      }, 
                  }

        elif pipeline in ['gstlal', 'gstlal-spiir', 'mbtaonline', 'pycbc']:
            xmldoc = ligolw_utils.load_filename(filename, contenthandler=lsctables.use_in(ligolw.LIGOLWContentHandler))

            ### extract table
            coinc = table.get_table(xmldoc, lsctables.CoincInspiralTable.tableName)

            ### fill in ans with
            for row in coinc:
                ans = {'far' : row.false_alarm_rate,
                       'instruments' : row.ifos,
                       'gpstime'     : row.end_time + 1e-9*row.end_time_ns,
                       'extra_attributes': {
                                           },
                      }

        else:
            raise ValueError('pipeline=%s not understood'%pipeline)

        return ans

    def createEvent(self, group, pipeline, filename, search=None, filecontents=None, **kwargs):
        self.check_group_pipeline_search( group, pipeline, search )

        #group    = group.lower()
        #pipeline = pipeline.lower()
        #if search: 
        #    search = search.lower()

        graceid = self.__genGraceID__(group) ### generate the graceid
        self.__createDirectory__(graceid) ### create local directory and all necessary files

        ### write top level data
        jsonD, lvalert = self.__createEvent__( graceid, group, pipeline, filename, search=search )
        self.sendlvalert( lvalert, self.__node__(graceid) )

        ### write filename to local
        self.writeLog( graceid, 'initial data', filename=filename ) ### sends alert about log message

        return FakeTTPResponse( jsonD )

    ### annotation ###

    def __log__(self, graceid, message, filename=None, tagname=[]):
        username = getpass.getuser()

        if filename:
            shortFilename = os.path.basename(filename)
        else:
            shortFilename = ''

        ind = self.__path2len__(self.__logsPath__(graceid))
        jsonD = {'comment': message,
                 'created': time.time(),
                 'self': self.__logsPath__(graceid),
                 'file_version': 0,  
                 'filename': shortFilename,
                 'tag_names': tagname,
                 'file': '',
                 'N': ind+1,  
                 'tags': '',
                 'issuer': {'username': username+'@LIGO.ORG',
                            'display_name': username,
                           },
                }

        ind = self.__append__( jsonD, self.__logsPath__(graceid)) ### should give the same number as self.__path2len__(self.__logsPath(graceid))+1
        if filename:
            self.__copyFile__(graceid, filename)

        lvalert = {'uid':graceid, 
                   "alert_type": "update",
                   "description": message,
                   "file": shortFilename,
                   "object": {
                              "N": ind,
                              "comment": message,
                              "created": time.time(),
                              "file": shortFilename,
                              "file_version": 0,
                              "filename": shortFilename,
                              "issuer": {
                                         "display_name": username,
                                         "username": username+"@ligo.org"
                                        },
                              "self": "",
                              "tag_names": tagname,
                              "tags": "",
                             },
                  } 

        return jsonD, lvalert

    def writeLog(self, graceid, message, filename=None, filecontents=None, tagname=[], displayName=None):
        self.check_graceid(graceid)

        jsonD, lvalert = self.__log__(graceid, message, filename=filename, tagname=tagname )

        self.sendlvalert( lvalert, self.__node__(graceid) )
        return FakeTTPResponse( jsonD )
 
    def writeFile(self, graceid, filename, filecontents=None):
        self.check_graceid(graceid)

        return self.writeLog( graceid, '', filename=filename, filecontents=filecontents)

    def __label__(self, graceid, label ):
        jsonD = {'self':self.__labelsPath__(graceid), 
                 'creator':getpass.getuser(), 
                 'name':label, 
                 'created':time.time(),
                }
        lvalert = {'uid':graceid, 
                   'alert_type':'label', 
                   'description':label, 
                   'file':'',
                  }

        self.writeLog( graceid, 'applying label : %s'%label )
        self.__append__( jsonD, self.__labelsPath__(graceid) )

        return jsonD, lvalert

    def writeLabel(self, graceid, label):
        self.check_graceid(graceid)
        self.check_label( label )

        jsonD, lvalert = self.__label__( graceid, label )

        self.sendlvalert( lvalert, self.__node__(graceid) )
        return FakeTTPResponse( jsonD )

    def removeLabel(self, graceid, label):
        self.check_graceid(graceid)

        raise NotImplementedError('this is not implemented in the real GraceDb, so we do not implement it here. At least, not yet.')

    ### queries ###

    def events(self, query=None, orderby=None, count=None, columns=None):
        """
        WARNING: we only support limitted syntax for these queries at this time. Specifically, we support three types of clauses that can be supplied in any order
            graceid's
            labels
            gpsstart .. gpsstop

        eg: "ADVNO 1177672330 .. 1177672360"

        NOTE: we only support query!=None at this time (we require orderby=count=columns=None but keep these kwargs present for consistency with ligo.gracedb.rest.GraceDb)

        more complete syntatic coverage may be available via sqlparse (https://sqlparse.readthedocs.io/en/latest/)
        """
        assert (orderby==None) and (count==None) and (columns==None), 'FakeDb.events only supports orderby=None, count=None, columns=None at this time!'

        if query: ### downselect events
            labels = []
            graceids = []
            gpstimes = []
            bits = query.split()
            N = len(bits)
            i = 0
            while i < N:
                bit = bits[i]
                try: ### interpret as the first part of a "gps .. gps" clause
                    gpsstart = float(bit)
                    if (i<N-2) and (bits[i+1]=='..'):
                        gpsstop = float(bits[i+2])
                    else:
                        raise FakeTTPError('Invalid query: could not parse "gps .. gps" clause')
                    gpstimes.append( (gpsstart, gpsstop) )
                    i += 3

                except ValueError as e: ### check to see if this is a label or a GraceId
                    if self.__is_label__(bit): ### this is a known label
                        labels.append( bit )

                    elif self.__is_graceid__(bit): ### is a graceid
                        graceids.append(bit)

                    else:
                        raise FakeTTPError('Invalid query: query contained an invalid label or graceid')
                    i += 1

            if graceids: ### check if users sepecified graceids
                if len(graceids)==1:
                    try:
                        self.check_graceid(graceids[0])
                        events = graceids
                    except FakeTTPError as e: ### could not find this event
                        events = [] 
                else:
                    events = [] ### more than one graceid, must return an empty list
            else:
                events = self.__get_all_graceids__() ### just grab all events
                                                     ### FIXME: downselecting based on this may not scale well

            if labels: ### check if users specified labels
                retained = []
                for label in labels:
                    for graceid in events:
                        these_labels = [d['name'] for d in self.labels(graceid).json()['labels']]
                        if label in these_labels:
                            retained.append( graceid )
                events = retained

            if gpstimes: ### check if users specified gpstimes
                retained = []
                for gpsstart, gpsstop in gpstimes:
                    for graceid in events:
                        gpstime = self.__extract__(self.__topLevelPath__(graceid))['gpstime']
                        if (gpsstart<=gpstime) and (gpstime<=gpsstop):
                            retained.append( graceid )
                events = retained
                
        else: ### return all events
            events = self.__get_all_graceids__()

        ### FIXME: we should modify events to account for orderby, count here

        for graceid in events:
            topLevel = self.event(graceid).json()

            ### FIXME: incorporate columns here

            yield topLevel


    def event(self, graceid):
        self.check_graceid(graceid)

        topLevel = self.__extract__( self.__topLevelPath__(graceid) )
        topLevel.update( {'labels':dict( (label['name'], label['self']) for label in self.__extract__( self.__labelsPath__(graceid) ) )} )

        return FakeTTPResponse( topLevel )

    def logs(self, graceid):
        self.check_graceid(graceid)

        logs = self.__extract__( self.__logsPath__(graceid) )
        logsPath = self.__logsPath__(graceid)
        return FakeTTPResponse( {'numRows':len(logs),
                                 'start':0,
                                 'log': logs,
                                 'links':{'self'  : logsPath,
                                          'first' : logsPath,
                                          'last'  : logsPath, 
                                         },
                                }
                              )

    def labels(self, graceid, label=''):
        self.check_graceid(graceid)

        return FakeTTPResponse( {'labels':self.__extract__( self.__labelsPath__(graceid) ),
                                 'links': [{'self':self.__labelsPath__(graceid),
                                           'event':self.__directory__(graceid),
                                           }
                                          ],
                                }
                              )

    def files(self, graceid, filename=None, raw=False):
        self.check_graceid(graceid)

        return FakeTTPResponse( dict( (os.path.basename(filename), filename) for filename in self.__extract__( self.__filesPath__(graceid) ) ) )
