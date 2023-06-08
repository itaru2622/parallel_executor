#!/usr/bin/env python3

from    pydantic import BaseModel, Extra
from    typing   import Any, Union
import  re
import  sys
import  numpy as np

class PingRespRecord(BaseModel, extra=Extra.allow): # refer pydantic doc for detail.
      '''datamodel for raw ping responce in pydantic BaseModel.

      Parameters:
          seq:  ping sequence number
          err:  indicator of error or not
          rtt:  raw RTT data
          errMsg: error message if received.
      '''
      seq:int=None
      err:bool=False
      rtt:Union[float,None]=None
      errMsg:Union[str,None]=None

# keep results
class _Host(object):
   '''Holder of parsed results for each host, and helper functions.'''

   def __init__(self):
       self.params:dict[str,Any] = {'resp':[], 'maxSeq':0 } # keep everything. 'resp' and 'maxSeq' is reserved for list[PingRespRecord] and seq:int

   def set(self, **kwargs):
       '''setter of parameters.'''
       self.params.update(kwargs)
       return self

   def get(self, key:str, default:Any=None):
       '''getter of parameter.'''
       return self.params.get(key, default)

   def append(self, rec:PingRespRecord):
       '''store parsed result for each ping record.'''

       seq:Union[int,None] = rec.seq

       if seq is not None and seq <= self.get('maxSeq'):
           print(f'seq dupplication detected,  discard current result {rec}', file=sys.stderr)
           return self

       self.params['resp'].append(rec)
       if seq is not None:
          self.set(maxSeq=seq)

       return self

   def getRTT(self, seq:bool=False, noNone:bool=False) -> list[Any]:
       '''RTT records getter.

       Args:
           seq:    if requester want sequence number or not.
           noNone: if requester wants nonNull value or not.

       Return:
           list[rtt] or list[(seq, rtt)]: it may include None, when requested.
       '''

       resps:list[PingRespRecord] = self.params['resp']
       
       # pick rtt from list of model if its value is not None
       if not seq:
           rtn    = [ r.rtt for r in resps]
           if noNone:
              rtn = [ r.rtt for r in resps if r.rtt is not None ]
       else:
           rtn =    [ (r.seq, r.rtt) for r in resps ]
           if noNone:
              rtn = [ (r.seq, r.rtt) for r in resps if r.rtt is not None]
       return rtn

   def isAlive(self) -> bool:
       '''the corresponding host is alive or not.

       Returns:
           bool: True when the host sent responce.
       '''

       p = self.getRTT(seq=False, noNone=True) # just RTTs without None.
       if  any(p):     # some data exists.
          return True
       return False

   def getErrors(self, seq:bool=False, uniq:bool=True) -> list[Any]:
       '''get error messages while pinging.

       Args:
           seq:    if requester wants sequence number or not.
           uniq:   if requester wants one for error
          
       Return:
           list[errMsg] or list[(seq, errMsg)]: it may include None, when requested.
       '''

       resps:list[PingRespRecord] = self.params['resp']

       # pick errMsg from list of model if its value is not None
       if not seq:
          rtn = [ r.errMsg for r in resps if r.errMsg is not None ]
          if uniq:
             rtn = set(rtn)
       else:
          rtn = [ (r.seq, r.errMsg) for r in resps if r.errMsg is not None ]

       return rtn

   def getRTTStatistics(self) -> Union[ tuple[float], None ]:
       '''get basic statistic numbers for the host.

       Returns:
           tuple[float] or None:  see below source code for detail.
       '''

       data = self.getRTT(seq=False, noNone=False)     # no-seq, include None.
       valid_data = [x for x in data if x is not None] # notNone values
       num_data  = len(data)
       num_valid = len(valid_data)
       num_none  = num_data - num_valid

       if any(valid_data):
          return num_none, num_data, num_valid, np.min(valid_data), np.max(valid_data), np.median(valid_data), np.mean(valid_data)
       return None


   def getHistogramData(self, min_val:float=0, max_val:float=1000, bin_width:float=10):
       '''building histogram data(numbers) for this host.

       Args:
           min_val(float):   min value of data
           max_val(float):   max value of data
           bin_width(float): width of bin.

       Returns:
           ...: refer numpy documents.
       '''

       data = self.getRTT(seq=False, noNone=False)
       valid_data = [x for x in data if x is not None] # notNone values
       num_none = len(data) - len(valid_data)          # None

       bins = np.arange(min_val, max_val + bin_width, bin_width)
       histogram, _ = np.histogram(valid_data, bins=bins)
       histogram = np.append(histogram, num_none)
       return histogram, bins

   def displayHistogramData(self, min_val:float=None, max_val:float=None, bin_width:float=None):
       '''display histogram for this host in print() base.

       Args:
           min_val(float):   min value of data
           max_val(float):   max value of data
           bin_width(float): width of bin.

       Returns:
           ...: refer numpy documents.
       '''

       # as how to use getHistogramData()
       v = self.getRTTStatistics()

       if v is None:
           print(f'######### no valid data for  {self.get("dest")}')
           return

       vmin = v[3]
       vmax = v[4]

       if not min_val:
          #min_val = vmin - 0.001
          min_val = vmin
       if not max_val:
          #max_val = vmax + 0.001
          max_val = vmax
       if not bin_width:
          #bin_width = (0.002+max_val-min_val)/7
          bin_width = (max_val-min_val)/7

       histogram, bins = self.getHistogramData( min_val, max_val, bin_width)
       print(f'\n\n######### RTT histogram for  {self.get("dest")}  ###########')
       for i in range(len(histogram)):
           if i == len(histogram) - 1:
               print(f'None: {histogram[i]}')
           else:
               bar = ''.join ( [ '*' for i in range(0, histogram[i]) ] )
               print(f'range {bins[i]:>4.3f} - {bins[i+1]:>4.3f}: ({histogram[i]:4d}) {bar}')


# Ping LogFile Parser.
class PingLogParser(object):
    def __init__(self):
        self.results:dict[str,_Host] = {}             # holder for all parsed results,  key:destIP
        self.maxCount:int = 0                         # holder for max sequence number, to use pretty-print

    def getResults(self):
        return self.results, self.maxCount

    def run(self, logpath:str, dest:str, verbose:bool=False):
        '''Parse one Logfile of ping.
           one of main function of this class.

        Args:
           logpath(str):   log of ping result   (i.e pingCmd dest > logfile. )
           dest(str):      destination of ping. (i.e pingCmd dest > logfile. )
           verbose(bool):  verbose print while parsing or not
        '''

        if verbose:
           print(f'start parsing for {dest} in {logpath}', file=sys.stderr)

        # phase1) get contents from logfile

        logs:list[str] = None
        with open(logpath, encoding='utf-8') as logfp:
             tmp = logfp.read()
             logs = tmp.splitlines()

        if not any(logs):
            return

        # phase2) parse contents line by line
        hrec = _Host().set(dest=dest, log=logpath)
        self.results[ dest ] = hrec

        for line in logs:
            p, seq, end = self.__parseResp(line, hrec)
            self.__updateCounter(seq)
            
            if verbose:
                print(f'{dest} {line}  => {p}    {logpath}', file=sys.stderr)
            if end:
                break

        # end of contents
        return

    def __updateCounter(self, seq:Union[int,None] ):
        '''helper function to update most biggest sequence numbers, for later use (pretty-print).'''
        if seq is None:
           return

        self.maxCount = max (self.maxCount, seq)
        return self.maxCount

    def __parseResp(self, line:str, hrec:_Host):
        '''parse  each raw ping responce message.

           the most core part of this class.

        Args:
           line(str):    resp from dest host
           hrec(Host):   mbuf of dst host.

        Returns:
           tuple( result:dict[str,Any], seq:Union[int|None], end:bool):  return three items in tuple.
                  result:    parsed result from given responce line.
                  seq:       current sequence number
                  end:       end of records or not.
        '''

        #
        # CAUTION:   MOST IMPORTANT DEFINITIONS.
        # regex expression to get meaningful info from each responce line.
        #
        #pattern_ok    = r"^(?P<size>\d+) bytes from (?P<dest>[^:]+):.*icmp_seq=(?P<seq>\d+).*ttl=(?P<ttl>\d+).*time=(?P<rtt>\S+) ms"     # ttl is that in line.
        pattern_ok     = r"^(?P<size>\d+) bytes from (?P<dest>[^:]+):.*icmp_seq=(?P<seq>\d+).*ttl=(?P<ttl>\d+).*time=(?P<rtt>[0-9.]+) ms" # ttl is that in line.
        pattern_ng     = r"^[Ff]rom (?P<reporter>\S+).*icmp_seq=(?P<seq>\d+)[\s]+(?P<msg>.*)$"  # in error, 'from' may be one of routers between dest and src.
        pattern_timeout= r"^[nN]o [aA]nswer yet for icmp_seq=(?P<seq>\d+)"                                                                # timeout when 'ping -O'

        # initializing value
        end:bool = False                  # if log reached to the last raw records(True) or not (False)
        seq:Union[int,None] = None        # sequence number of current records

        ok = re.match(pattern_ok, line)
        ng = re.match(pattern_ng, line)
        timeout = re.match(pattern_timeout, line)

        if ok:
            result = ok.groupdict()
            result['result'] = 'ok'
            seq = int(result['seq'])
            hrec.append( PingRespRecord(seq=seq, rtt=result['rtt'] ))

        elif ng:
            result = ng.groupdict()
            result['result'] = 'NG'
            seq = int(result['seq'])
            hrec.append( PingRespRecord(seq=seq, errMsg=result['msg'], reporter=result['reporter'] ))

        elif timeout:
            result = timeout.groupdict()
            result['result'] = 'NG'
            seq = int(result['seq'])
            hrec.append( PingRespRecord(seq=seq, errMsg='no answer yet' ))

        else:
            if line.startswith('PING'): # first line
               result=f'starting to {hrec.get("dest")}'
            elif 'ping statistics' in line: # ending line
               result=f'ending by  {line}'
               end=True
            else:
               result = { 'result': 'NG?', 'resp':line }

        return result, seq, end

    def mkdict(self, keys:list[str], vals:list[Any]) -> dict[str,Any]:
        '''make dict from list of keys and values

        Args:
            keys: key of dict
            vals: val of dict
        '''

        if len(keys) != len(vals):
              raise RuntimeError(f'num of keys and vals is mismatched, keys:{keys}   vals:{vals}')
        return dict(zip(keys, vals))


    def mkData(self, dstColName:str, aliveColName:str, prefixDataColName:str='rtt', includes_err:bool=True) -> list[dict[str,Any]]:
        '''make data for output from records as list of dict

        Args:
            output(str):            path to output.
            dstColName(str):        the name of dest column,  for CSV header.
            prefixDataColName(str): the name of Data columns, for CSV header.
            includes_error(bool):   output error messages found in Ping Log file(True), or not(False).

        Returns:
            list[dict[str, Any]]:
        '''

        rtn = []
        recs, count = self.getResults()                        # get all results
        if not any(recs) or count==0:
           print(f'########### no records found !')
           return

        #
        # phase1) make keys for pretty-print
        #

        keys = [dstColName, aliveColName]                      # keys: initial value

        #    remained keys for raw data part, for pretty-printing
        ndigits = len(str(count-1))
        nformat = '{:02d}' if ndigits==1 else '{:0'+str(ndigits)+'d}'
        nformat = prefixDataColName +nformat
        for c in range(1,count+1):
            rtt = nformat.format(c)
            keys.append(rtt)

        if includes_err:
            keys.append('err')
        # mk keys done.

        #
        # phase2) make data parts
        #

        for dst,hrec in recs.items():                          # make values
              vals = [ dst, hrec.isAlive() ]                   #   inivial value.
              rtt = hrec.getRTT()
              for n in range(len(rtt),count):                  # !!! fill None when val is missing, by timeout etc.
                  rtt.append(None)
              vals.extend(rtt)

              if includes_err:
                  errs = hrec.getErrors()
                  if errs:
                      errs = ','.join(errs)
                      vals.append(errs)
                  else:
                      vals.append(None)
              rtn.append ( self.mkdict(keys,vals) )                # register data
        #end loop to make data.

        return rtn


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

if __name__ == '__main__':
    import argparse
    import os
    from   collections import OrderedDict
    import pandas as pd


    parser = argparse.ArgumentParser(description='Ping multiple hosts and collect responses.')
    parser.add_argument('-i','--input',             type=str, default='/dev/stdin',    help='path of log files list')
    parser.add_argument('-o','--output',            type=str, default='/dev/stdout',   help='path of CSV file to output')
    parser.add_argument('-d','--dstColName',        type=str, default='dest',          help='column name of dest in output csv header')
    parser.add_argument('-a','--aliveColName',      type=str, default='alive',         help='column name of "alive" in output csv header')
    parser.add_argument('-p','--prefixDataColName', type=str, default='rtt',           help='prefix for data column names in output csv header')
    parser.add_argument('-v','--verbose',           action="store_true",               help='verbose output or not')
    parser.add_argument('-H','--histogram',         action="store_true",               help='print histograms in stdout')
    args = parser.parse_args()
    print(args, file=sys.stderr)

    # CAUTION( Current Ristriction )
    #
    #   * the input file contains the path of ping logfile. and its content has to meet below condition.
    #      - one file path in each line
    #      - no delimiters between paths  (no comma etc )
    #      - file path naming rule has to meet below: 
    #        any folder(relative|abs) you can take, but the filename part has to be equal to IP address.
    #
    #         any/folder/192.168.1.1       => OK    the end of file path equals to IP.
    #        /any/folder/192.168.1.1       => OK    the end of file path equals to IP.
    #         any/folder/192.168.1.1.txt   => NG    it has extension
    #         any/folder/192.168.1.1.log   => NG    it has extension
    #         any/folder/fqdn.example.com  => NG    its not IP but FQDN.
    #
    #   * content of ping log file has to meet below condition, mainly on executing ping command.
    #      - the logfile is assumed to be taken by the following command:
    #
    #        bash$  LANG=C ping -O -c counts 192.168.1.1 > log/192.168.1.1
    #
    #      - any other style is not tested.
    #      - do not end ping command by CTRL-C.        => it may give damage in content in logfile.
    #      - NO l18n SUPPORT                           => if message in l18n,  no valid output.
    #      - if no message at lossing resp(no -O opt)  => sequencing will be broken in output.
    #      - do NOT modify any output                  => it makes result fake.
    #


    logFiles:dict[str,str] = OrderedDict() # dict of { log-path, destIP }

    if not args.input:
        raise RuntimeError('ping log files required')

    with open(args.input, encoding='utf-8') as fp:
        tmp = fp.read()
        content = tmp.splitlines()

    if not any(content):
        print('... empty content', file=sys.stderr)
    for path in content:
        f = os.path.basename(path)
        logFiles[path] = f

    if args.verbose:
        print(logFiles)


    logparser = PingLogParser()
    for path, dest in logFiles.items():
        logparser.run(path, dest, verbose=args.verbose)

    ldict = logparser.mkData(dstColName=args.dstColName, aliveColName=args.aliveColName, prefixDataColName=args.prefixDataColName, includes_err=True)
    df = pd.DataFrame( ldict)
    df.to_csv(args.output, index=False)

    if args.histogram:
        recs,_ = logparser.getResults()
        for k,rec in recs.items():
            rec.displayHistogramData()
