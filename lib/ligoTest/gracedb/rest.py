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
                         'V1OPS', 'V1OK', 'V1NO',
                        ]

    __allowedEMGroups__ = [
        'Test', 
        'LV-EM Followup', 
        'ARI LJMU', 
        'CRTS', 
        'FIGARO', 
        'FRBSG', 
        'GOTO', 
        'GWU LSC Group', 
        'H.E.S.S.', 
        'INAF', 
        'IUCAA',
        'LCOGT', 
        'MAGIC', 
        'Pan-STARRS', 
        'PIRATE', 
        'RoboPol', 
        'Swift', 
        'TOROS', 
        'VERITAS', 
        'ZTF', 
        'AROMA', 
        'CALET', 
        'DES', 
        'EWE', 
        'FLEAS',
        'HAWC', 
        'HETGS', 
        'ISDC', 
        'LOFAR-TKSP', 
        'LSQ', 
        'MAXI', 
        'MWA', 
        'OVRO', 
        'PESSTO', 
        'Pi of the Sky', 
        'SVOM', 
        'HTRU', 
        'TZAC', 
        'VAST', 
        'GRA SAO RAS', 
        'ARAE', 
        'Berger Time-Domain Research Group', 
        'Caltech-NRAO Radio Transient Group', 
        'Fast Spectroscopy with LBT and Gemini', 
        'Fermi', 
        'J-GEM', 
        'KU', 
        'LGT-Lulin', 
        'LV Swift MIT EM Follow-up', 
        'MASTER', 
        'RAPTOR', 
        'RATIR', 
        'SAAO', 
        'SkyMapper', 
        'SRO7', 
        'Terskol-GW follow-up', 
        'TLS', 
        'TTU LSC group', 
        'TTU Observatory', 
        'Leicester transient science team', 
        'USO', 
        'Wise-GECO', 
        'XMM-Newton', 
        'ATLAS', 
        '1M2H', 
        'AAOGW', 
        'AGILE', 
        'ANTARES', 
        'Apertif-EVN', 
        'AST3', 
        'Auger', 
        'AZTEC-GW', 
        'BlackGEM', 
        'COSI', 
        'CTA', 
        'CZTI-IUCAA', 
        'DLT40', 
        'DWF', 
        'FAST', 
        'GROND', 
        'Huntsman', 
        'HXMT', 
        'IceCube', 
        'IKI_GRB', 
        'IPN', 
        'MeerKAT', 
        'NenuFAR', 
        'NRAO', 
        'NTE', 
        'OGWARTS', 
        'RIMAS', 
        'SRG-eROSITA', 
        'TLC X-ray Imaging', 
        'UNC-LFP',
    ]

    __allowedWavebands__ = {
        'em.radio'           : 'Radio part of the spectrum', 
        'em.radio.200-400MHz': 'Radio between 200 and 400 MHz', 
        'em.UV.100-200nm'    : 'Ultraviolet between 100 and 200 nm', 
        'em.IR'              : 'Infrared part of the spectrum', 
        'em.mm.100-200GHz'   : 'Millimetric between 100 and 200 GHz', 
        'em.UV.50-100nm'     : 'Ultraviolet between 50 and 100 nm', 
        'em.opt.B'           : 'Optical band between 400 and 500 nm', 
        'em.mm'              : 'Millimetric part of the spectrum', 
        'em.mm.30-50GHz'     : 'Millimetric between 30 and 50 GHz', 
        'em.radio.3-6GHz'    : 'Radio between 3 and 6 GHz', 
        'em.IR.NIR'          : 'Near-Infrared, 1-5 microns', 
        'em.opt.U'           : 'Optical band between 300 and 400 nm', 
        'em.opt.V'           : 'Optical band between 500 and 600 nm', 
        'em.UV'              : 'Ultraviolet part of the spectrum', 
        'em.opt.R'           : 'Optical band between 600 and 750 nm', 
        'em.X-ray.medium'    : 'Medium X-ray (2 - 12 keV)', 
        'em.UV.10-50nm'      : 'Ultraviolet between 10 and 50 nm', 
        'em.IR.FIR'          : 'Far-Infrared, 30-100 microns', 
        'em.mm.750-1500GHz'  : 'Millimetric between 750 and 1500 GHz', 
        'em.IR.8-15um'       : 'Infrared between 8 and 15 micron', 
        'em.IR.60-100um'     : 'Infrared between 60 and 100 micron', 
        'em.UV.FUV'          : 'Far-Infrared, 30-100 microns', 
        'em.gamma.hard'      : 'Hard gamma ray (>500 keV)', 
        'em.X-ray.soft'      : 'Soft X-ray (0.12 - 2 keV)', 
        'em.IR.4-8um'        : 'Infrared between 4 and 8 micron', 
        'em.UV.200-300nm'    : 'Ultraviolet between 200 and 300 nm', 
        'em.mm.1500-3000GHz' : 'Millimetric between 1500 and 3000 GHz', 
        'em.radio.750-1500MHz': 'Radio between 750 and 1500 MHz', 
        'em.X-ray.hard'      : 'Hard X-ray (12 - 120 keV)', 
        'em.gamma.soft'      : 'Soft gamma ray (120 - 500 keV)', 
        'em.radio.400-750MHz': 'Radio between 400 and 750 MHz', 
        'em.radio.1500-3000MHz': 'Radio between 1500 and 3000 MHz', 
        'em.IR.H'            : 'Infrared between 1.5 and 2 micron', 
        'em.opt.I'           : 'Optical band between 750 and 1000 nm', 
        'em.IR.J'            : 'Infrared between 1.0 and 1.5 micron', 
        'em.IR.K'            : 'Infrared between 2 and 3 micron', 
        'em.radio.12-30GHz'  : 'Radio between 12 and 30 GHz', 
        'em.mm.50-100GHz'    : 'Millimetric between 50 and 100 GHz', 
        'em.gamma'           : 'Gamma rays part of the spectrum', 
        'em.mm.400-750GHz'   : 'Millimetric between 400 and 750 GHz', 
        'em.IR.MIR'          : 'Medium-Infrared, 5-30 microns', 
        'em.radio.6-12GHz'   : 'Radio between 6 and 12 GHz', 
        'em.IR.3-4um'        : 'Infrared between 3 and 4 micron', 
        'em.X-ray'           : 'X-ray part of the spectrum', 
        'em.opt'             : 'Optical part of the spectrum', 
        'em.IR.30-60um'      : 'Infrared between 30 and 60 micron', 
        'em.radio.100-200MHz': 'Radio between 100 and 200 MHz', 
        'em.IR.15-30um'      : 'Infrared between 15 and 30 micron', 
        'em.mm.200-400GHz'   : 'Millimetric between 200 and 400 GHz', 
        'em.radio.20-100MHz' : 'Radio between 20 and 100 MHz',
    }

    __allowedVOEventTypes__ = {
        'PR': 'preliminary', 
        'RE': 'retraction', 
        'UP': 'update', 
        'IN': 'initial',
    }

    __obsStatuses__ = {
        'PR': 'PREDICTION',
        'NA': 'NOT APPLICABLE',
        'TE': 'TEST',
        'OB': 'OBSERVATION'
    }

    __allowedEELStatuses__ = {
        'SO': 'SOURCE', 
        'CI': 'CIRCULAR', 
        'CO': 'COMMENT', 
        'FO': 'FOOTPRINT',
    }

    __allowedOBSStatuses__ = {
        'PR': 'PREDICTION', 
        'NA': 'NOT APPLICABLE', 
        'TE': 'TEST', 
        'OB': 'OBSERVATION',
    }

    __allowedSignoffs__ = ['H1OK' , 'H1NO',
                           'L1OK' , 'L1NO',
                           'V1OK' , 'V1NO',
                           'ADVOK', 'ADVNO',
                          ]

    @property
    def groups(self):
        return self.__allowedGroupPipelineSearch__.keys()

    @property
    def pipelines(self):
        ans = []
        for key, val in self.__allowedGroupPipelineSearch__.items():
            ans += val.keys()
        return sorted(set(ans))

    @property
    def searches(self):
        ans = []
        for key, val in self.__allowedGroupPipelineSearch__.items():
            for v in val.values():
                ans += v
        return [_ for _ in sorted(set(ans)) if _!=None]
    @property
    def obs_statuses(self):
        return self.__obsStatuses__

    @property
    def em_groups(self):
        return self.__allowedEMGroups__

    @property
    def wavebands(self):
        return self.__allowedWavebands__

    @property
    def voevent_types(self):
        return self.__allowedVOEventTypes__

    @property
    def eel_statuses(self):
        return self.__allowedEELStatuses__

    @property
    def obs_statuses(self):
        return self.__allowedOBSStatuses__

    @property
    def service_info(self):
        result= {'templates'    : {
                                   'event-detail-template': self.service_url+'{graceid}', 
                                   'voevent-list-template': self.service_url+'{graceid}/voevents.pkl', 
                                   'signoff-list-template': self.service_url+'{graceid}/signoffs.pkl',
                                   },
                 'searches'     : self.searches,
                 'pipelines'    : self.pipelines,
                 'links'        : {'performance': None,
                                       'self': self.service_url,
                                       'events': None,
                                  },
                 'wavebands'    : self.wavebands,
                 'labels'       : self.__allowedLabels__,
                 'eel-statuses' : self.eel_statuses,
                 'obs-statuses' : self.obs_statuses,
                 'em-groups'    : self.em_groups,
                 'groups'       : self.groups,
                 'voevent-types': self.voevent_types,
                }
        return result

    ### basic instantiation ###

    def __init__(self, directory='.'):
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.service_url = directory
        self.lvalert = os.path.join(directory, 'lvalert.out') ### file into which we write lvalert messages

    ### write lvalert messages into a file ###

    def sendlvalert(self, message, node ):
        file_obj = open(self.lvalert, 'a')
        print >> file_obj, lvutils.alert2line(node, json.dumps(message))
        file_obj.close()

    ### simulate get(...) according to GraceDb.get()
    def get(self, url):
        '''
        Returns the content of pickle file as a FakeTTPResponse
        '''
        content = self.__extract__(url)
        return FakeTTPResponse(content)
        
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

    def check_signoff(self, signoff):
        if signoff not in self.__allowedSignoffs__:
            raise FakeTTPError('signoff=%s not allowed'%signoff)

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
        for path in os.listdir(self.service_url):
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
        return os.path.join(self.service_url, graceid)

    def __topLevelPath__(self, graceid):
        return os.path.join(self.service_url, graceid, 'toplevel.pkl')

    def __filesPath__(self, graceid):
        return os.path.join(self.service_url, graceid, 'files.pkl')

    def __labelsPath__(self, graceid):
        return os.path.join(self.service_url, graceid, 'labels.pkl')

    def __signoffsPath__(self, graceid):
        return os.path.join(self.service_url, graceid, 'signoffs.pkl')

    def __logsPath__(self, graceid):
        return os.path.join(self.service_url, graceid, 'logs.pkl')

    def __voeventsPath__(self, graceid):
        return os.path.join(self.service_url, graceid, 'voevents.pkl')

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
        
    def __signOffappend__( self, signoffObject, path ):
        '''append signoffObject to signoff key'''
        content  = self.__extract__(path) # this returns a dictionary
        listofSignoffs = content.get('signoff')
        listofSignoffs.append(signoffObject)
        self.__write__(content, path)

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
                     self.__voeventsPath__(graceid),
                    ]
            for path in paths:
                file_obj = open(path, 'w')
                pickle.dump([], file_obj)
                file_obj.close()
            # make signoffs.pkl file which is a DICTIONARY not a list!
            file_obj = open(self.__signoffsPath__(graceid), 'w')
            signoffDict = {'numRows': None,
                           'start'  : None,
                           'signoff': [],
                           'links'  : None
                          }
            pickle.dump(signoffDict, file_obj)
            file_obj.close()

    def __newfilename__(self, graceid, filename):
        return os.path.join(self.service_url, graceid, os.path.basename(filename))

    def __copyFile__(self, graceid, filename):
        newFilename = self.__newfilename__(graceid, filename)
        shutil.copyfile(filename, newFilename)
        self.__append__( newFilename, self.__filesPath__(graceid) )

    ### insertion ###

    def __createEvent__(self, graceid, group, pipeline, filename, search=None, offline=False):
        print('createEvent offline: {0}'.format(offline))
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
                 'offline':offline,   
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

    def createEvent(self, group, pipeline, filename, search=None, offline=False, filecontents=None, **kwargs):
        self.check_group_pipeline_search( group, pipeline, search )

        #group    = group.lower()
        #pipeline = pipeline.lower()
        #if search: 
        #    search = search.lower()

        graceid = self.__genGraceID__(group) ### generate the graceid
        self.__createDirectory__(graceid) ### create local directory and all necessary files

        ### write top level data
        jsonD, lvalert = self.__createEvent__( graceid, group, pipeline, filename, search=search, offline=offline)
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

    def __signoff__(self, graceid, instrument, signoff_type, status ):
        signoff = '{0}{1}'.format(instrument, status) if instrument else '{0}{1}'.format(signoff_type, status)
        jsonD = {'self':self.__labelsPath__(graceid),
                 'creator':getpass.getuser(),
                 'name':signoff,
                 'created':time.time(),
                } # we use the labelsPath because when we query FakeTTP for H1OK, etc they are recorded as labels and need to be in the labels.pkl file
        signoffObject = {'submitter'   : None,
                         'comment'     : None,
                         'instrument'  : instrument,
                         'signoff_type': signoff_type, 
                         'status'      : status
                        }
        lvalert = {'uid':graceid,
                   'alert_type':'signoff',
                   'description': '',
                   'object': signoffObject,
                   'file':'',
                  }
        
        self.writeLog( graceid, 'applying label from signoff : %s'%signoff )
        self.__append__( jsonD, self.__labelsPath__(graceid) )
        self.__signOffappend__( signoffObject, self.__signoffsPath__(graceid) )

        return jsonD, lvalert

    def writeSignoff(self, graceid, instrument, signoff_type, status):
        signoff = '{0}{1}'.format(instrument, status) if instrument else '{0}{1}'.format(signoff_type, status)
        self.check_graceid(graceid)
        self.check_signoff( signoff )

        jsonD, lvalert = self.__signoff__( graceid, instrument, signoff_type, status )
        self.sendlvalert( lvalert, self.__node__(graceid) )
        return FakeTTPResponse(jsonD)

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

    #--- methods that aren't really supported yet in any meaningful way

    def createVOEvent(self, *args, **kwargs):
        """
        WARNING: this is not actually implemented and is only present for syntactic completion.
        If users actually need to reference the VOEvents that they create, then we'll have to actually implement somthing.
        """
        pass

    def voevents(self, graceid):
        """
        WARNING: not implemented yet. Should return some sort of list of VOEvents that have been created for this GraceId
        """
        self.check_graceid(graceid)

        voevents = self.__extract__( self.__voeventsPath__(graceid) )
        voeventsPath = self.__voeventsPath__(graceid)
        return FakeTTPResponse( {'numRows':len(voevents),
                                 'start':0,
                                 'links':{'self'  : voeventsPath,
                                          'first' : voeventsPath,
                                          'last'  : voeventsPath,
                                         },
                                 'voevents': voevents,
                                }
                              )

    def replaceEvent(self, graceid, filename, filecontents=None):
        """
        WARNING: not implemented
        """
        raise NotImplementedError

    def numEvents(self, query=None):
        """
        WARNING: not implemented
        """
        raise NotImplementedError

    def eels(self, graceid):
        """
        WARNING: not implemented
        """
        raise NotImplementedError

    def writeEel(self, graceid, group, waveband, eel_status, obs_status, **kwargs):
        """
        WARNING: not implemented
        """
        raise NotImplementedError

    def emobservations(self, graceid):
        """
        WARNING: not implemented
        """
        raise NotImplementedError

    def writeEMObservation(self, graceid, group, raList, raWidthList, decList, decWidthList, startTimeList, durationList, comment=None):
        """
        WARNING: not implemented
        """
        raise NotImplementedError

    def tags(self, graceid, n):
        """
        WARNING: not implemented
        """
        raise NotImplementedError

    def createTag(self, graceid, n, tagname, displayName=None):
        """
        WARNING: not implemented
        """
        raise NotImplementedError

    def deleteTag(self, graceid, n, tagname):
        """
        WARNING: not implemented
        """
        raise NotImplementedError

    def ping(self):
    
        """
        WARNING: not implemented
        probably want to do something like
            return os.path.exists(self.service_url)
        but with an appropriate wrapper (FakeTTPResponse?)
        """
        raise NotImplementedError

    @property
    def links(self):
        return self.service_info.get('links')

    @property
    def templates(self):
        return self.service_info.get('templates')

    def request(self, method, *args, **kwargs):
        raise NotImplementedError
