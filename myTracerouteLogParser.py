#!/usr/bin/env python3

from    pydantic  import BaseModel, Extra, IPvAnyAddress, ValidationError, validator, Field
from    ipaddress import IPv4Address
import  pandas    as     pd

from    typing   import Any, Union
import  re
import  sys
import  json

class TracerouteRespRecord(BaseModel, extra=Extra.allow):
    '''Datamodel for raw traceroute response record.
    '''
    hopCount:  int=None                    # auto conversion by pydantic, with self check.
    ip:        IPvAnyAddress=None          # auto conversion by pydantic, with self check.
    timeouts:  Union[int,str]=None         # self conversion from str to int in validator..

    @validator('hopCount')
    def check_hopCount(cls:Any, v:Any, values:dict[str,Any], field:Field):
        alias = field.alias
        if alias and alias not in values:  # keep v in values when having alias.
            values[alias] = v

        if isinstance(v, int):             # if it is already int, then pass it.
            return v
        v = field.default
        return v                           # default, otherwise

    @validator('ip')
    def check_ip(cls:Any, v:Any, values:dict[str,Any], field:Field):
        alias = field.alias
        if alias and alias not in values:                  # keep v in values when having alias.
            values[alias] = v

        if isinstance(v, (IPvAnyAddress, IPv4Address,) ):  # already IPAddress class, pass it.
            return v
        v = field.default
        return v                                           # default, othewise.

    @validator('timeouts')
    def check_timeouts(cls:Any, v:Any, values:dict[str,Any], field:Field):
        alias = field.alias
        if alias and alias not in values: # keep v in values when having alias.
            values[alias] = v

        default = field.default
        if isinstance(v, str):                 # when str
            if v in [None, '']:
                v = default                    #  default value, when v is None or empty
                return v
            if '*' not in v:
                v = default                    #  default when it doesn't have '*'
                return v

            v = v.split('*')                   #  when having '*', counts its num and return
            v = len(v)-1                       #  '*'.split('*') get two-item array(['','']), then say 1.
            return v

        elif isinstance(v, int):               # when it is already number pass it.
            return v

        v = default
        return v                               # return default, in othercases.

class _Host(object):
    '''Holder of parsed results for each host, and helper function.'''

    def __init__(self):
        # keep everything. 'resp' and 'maxHop' is reserved for list[TracerouteRespRecord] and hopCount:int
        self.params:dict[str,Any] = {'resp':[], 'maxHop':0 }

    def set(self, **kwargs):
        '''setter of parameters.'''
        self.params.update(kwargs)
        return self

    def get(self, key:str, default:Any=None):
        '''getter of parameter.'''
        return self.params.get(key, default)

    def append(self, rec:TracerouteRespRecord):
        '''store parsed result for each record.'''

        hopCount:Union[int,None] = rec.hopCount

        if hopCount is not None and hopCount <= self.get('maxHop'):
            print(f'hopCount dupplication detected,  discard current result {rec}', file=sys.stderr)
            return self

        self.params['resp'].append(rec)
        if hopCount is not None:
           self.set(maxHop=hopCount)

        return self

    def getTrace(self, hop:bool=False, noNone:bool=False) -> list[Any]:
        '''TraceRoute Records getter.
        Args:
            hop:    if requester want sequence number or not.
            noNone: if requester wants nonNull value or not.

        Return:
            list[ipaddr] or list[(hopCount, ipaddr)]: it may include None, when requested.
        '''

        resps:list[TraceRouteRespRecord] = self.params['resp']

        # pick ip from list of model
        if not hop:
            rtn    =      [ r.ip for r in resps]
            if noNone:
                rtn =     [ r.ip for r in resps if r.ip is not None ]
        else:
            rtn =         [ (r.hopCount, r.ip) for r in resps ]
            if noNone:
                rtn =     [ (r.hopCount, r.ip) for r in resps if r.ip is not None]
        return rtn


class TracerouteLogParser(object):
    def __init__(self):
        self.results:dict[str,Any] = {}
        self.maxHops:int=0

    def getResults(self):
        return self.results, self.maxHops

    def run(self, logpath:str, dest:str, verbose:bool=False):
        '''Parse one Logfile of traceroute.
           one of main function of this class.

        Args:
           logpath(str):   log of traceroute result   (i.e tracerouteCmd dest > logfile. )
           dest(str):      destination of ping.       (i.e tracerouteCmd dest > logfile. )
           verbose(bool):  verbose print while parsing or not
        '''

        with open(logpath, encoding='utf-8') as logfp:
            tmp = logfp.read()
            logs = tmp.splitlines()
        if not any(logs):
            raise RuntimeError('no content')

        hrec = _Host().set(dest=dest, log=logpath)
        self.results[dest] = hrec

        for line in logs:
            p, hopCount = self.__parseResp(line, hrec)
            self.__updateCounter(hopCount)
            if verbose:
                print(f'{dest} {line} => {p}   {logpath}', file=sys.stderr)
        #endof loop
        return


    def __updateCounter(self, hopCount:Union[int,None] ):
        '''helper function to update most biggest hopCount, for later use (pretty-print).'''
        if hopCount is None:
           return

        self.maxHops = max (self.maxHops, hopCount)
        return self.maxHops

    def __parseResp(self, line:str, hrec:_Host):
        '''parse  each raw traceroute responce message.

           the most core part of this class.

        Args:
           line(str):    resp from dest host
           hrec(Host):   mbuf of dst host.

        Returns:
           tuple( result:dict[str,Any], seq:Union[int|None]):  return two items in tuple.
                  result:    parsed result from given responce line.
                  hopCount   current hopCount number
        '''

        #
        # MOST IMPORTANT VARIABLE.
        #
        # regex expression to parse traceroute log record.
        #
        pattern_ok      = r'^(?P<hopCount>\s*\d+)\s*(?P<timeouts>[\*\s]*)(?P<ip>([\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}))\s+(?P<extra>.*)$'
        pattern_timeout = r'^(?P<hopCount>\s*\d+)\s*(?P<timeouts>[\*\s]*)\s*$'


        result=None
        ok = re.match(pattern_ok, line)
        timeout = re.match(pattern_timeout, line)

        if ok:
            result = ok.groupdict()
        if timeout:
            result = timeout.groupdict()
            result['ip']='0.0.0.0'

        if result:
            rec = TracerouteRespRecord(**result)
            hrec.append(rec)
            hopCount=rec.hopCount
        else:
            hopCount=None
            if line.startswith('traceroute'):
                result = f'starting {line}'
            else:
                result = f'#error ####### unknown rectrd detected, {line}'
                print(result, file=sys.stderr)

        return result, hopCount

    def mkDict(self, keys:list[str], vals:list[Any]) -> dict[str,Any]:
        '''make dict from list of keys and list of vals.

        Args:
            keys: key of dict
            vals: val of dict

        Returns:
            dict[str,Any]: generated dict.
        '''

        if len(keys) != len(vals):
              raise RuntimeError(f'num of keys and vals is mismatched, lkeys:{len(keys)}, lvals:{len(vals)}, keys:{keys}   vals:{vals}')
        return dict(zip(keys, vals))

    def mkData(self, dstColName:str, src:str=None, prefixDataColName:str='hop')->list[dict[str,Any]]:
        '''make data for output in list of dict

        Args:
            dstColName(str):        the name of dest column,  for CSV header.
            src(str):               the sender node IPaddress

        Returns:
            list[dict[str,Any]]:    parsed results of traceroute records.
        '''

        rtn = []                                             # place holder for return value

        recs, count = self.getResults()                      # get all results
        if not any(recs) or count==0:
           print(f'########### no records found !')
           return rtn

        #
        # phase1) make keys for pretty-print
        #
        keys = [dstColName]                                  # keys initial value.

        # make keys for pretty-print

        idx_start = 1     # default range starts from 1
        adjust    = 0
        if src is not None:
            idx_start=0   # range starts 0 for src
            adjust   =1

        ndigits = len(str(count-1+adjust))
        nformat = '{:02d}' if ndigits==1 else '{:0'+str(ndigits)+'d}'
        nformat = prefixDataColName +nformat
        for c in range(idx_start,count+1):                   # keys for data part(idx_start .. count)
           rtt = nformat.format(c)
           keys.append(rtt)
        # phase1 done.

        #
        # phase2) make dict for each record and retrun value...
        #
        for dst,hrec in recs.items():                        # for each series of traceroute
            vals = [dst]
            if src is not None:
                vals.append(src)
            v = hrec.getTrace()
            vals.extend(v)                                   # fill data.
            for n in range(len(vals), len(keys)):            #   fill None if len(vals)!=len(keys), i.e: reach to dest shorter than others.
                vals.append(None)
            rtn.append ( self.mkDict(keys, vals) )
        #end loop to make data
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
    parser.add_argument('-p','--prefixDataColName', type=str, default='hop',           help='prefix for data column names in output csv header')
    parser.add_argument('-s','--src',               type=str, default=None,            help='sender node IP address, to record in CSV')
    parser.add_argument('-v','--verbose',           action="store_true",               help='verbose output or not')

    args = parser.parse_args()
    print(args, file=sys.stderr)

    if not args.input:
        raise RuntimeError('log files required')

    with open(args.input, encoding='utf-8') as fp:
        tmp = fp.read()
        content = tmp.splitlines()

    if not any(content):
        print('... empty content', file=sys.stderr)

    logFiles:dict[str,str] = OrderedDict() # dict of { log-path, routerIP }
    for path in content:
        f = os.path.basename(path)
        logFiles[path] = f

    if args.verbose:
        print(logFiles)

    logparser = TracerouteLogParser()
    for path, dest in logFiles.items():
        logparser.run(path, dest, verbose=args.verbose)

    ldict = logparser.mkData(dstColName=args.dstColName, src=args.src, prefixDataColName=args.prefixDataColName)
    df = pd.DataFrame(ldict)
    df.to_csv(args.output, index=False)
