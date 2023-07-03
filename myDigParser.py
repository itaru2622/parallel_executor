#!/usr/bin/env python3

from    pydantic import BaseModel, Extra
from    collections import defaultdict
from    typing   import Any, Union
import  re
import  sys
import  numpy as np

class DigRespRecord(BaseModel, extra=Extra.allow): # refer pydantic doc for detail.
      '''datamodel for dig  responce in pydantic BaseModel.

      Parameters:
          name:  the name in query.
          clazz: class in DNS record (IN | IN6 etc.)
          type:  type in DNS record  (A  | CNAME | AAAA etc.)
          val:   value depends on type (IPv4 or alias name etc.)
      '''
      Name:str=None
      Class:str=None
      Type:str=None
      Val:str=None

# keep results
class _Host(object):
   '''Holder of parsed results for each host, and helper functions.'''

   def __init__(self):
       self.params:dict[str,Any] = {'resp':defaultdict(list) } # keep everything. 'resp' for dict[str,list[DigRespRecord]]

   def set(self, **kwargs):
       '''setter of parameters.'''
       self.params.update(kwargs)
       return self

   def get(self, key:str, default:Any=None):
       '''getter of parameter.'''
       return self.params.get(key, default)

   def append(self, rec:DigRespRecord):
       '''store parsed result for each dig record.'''

       ty = rec.Type
       self.params['resp'][ty].append(rec)
       return self


# Dig LogFile Parser.
class DigLogParser(object):
    def __init__(self):
        self.results:dict[str,_Host] = {}             # holder for all parsed results,  key:destIP
        self.maxCount = defaultdict(int)              # holder for max records for each type, to use pretty-print

    def getResults(self):
        return self.results, self.maxCount

    def run(self, logpath:str, dest:str, verbose:bool=False):
        '''Parse one Logfile of ping.
           one of main function of this class.

        Args:
           logpath(str):   log of dig result   (i.e digCmd dest > logfile. )
           dest(str):      target of dig       (i.e digCmd dest > logfile. )
           verbose(bool):  verbose print while parsing or not
        '''

        if verbose:
           print(f'start parsing for {dest} in {logpath}', file=sys.stderr)

        # phase1) get contents from logfile
        logs:list[str] = None
        with open(logpath, encoding='utf-8') as logfp:
             tmp = logfp.read().splitlines()

        if not any(tmp):
            return

        # phase2) pick line in section (answer or authority)
        logs = []
        flg = False
        for line in tmp:
            if flg in [ False ]:
               if line.startswith(';; ANSWER SECTION:'):
                     flg=True
               continue
            if flg in [ True ]:
               if line == '':
                  flg=False
                  continue
               logs.append(line)

               if verbose:
                   print(f'{dest} {line}  => {p}    {logpath}', file=sys.stderr)
        # end of picking line.

        # phase3) pick data from picked line.
        hrec = _Host().set(dest=dest, log=logpath)
        self.results [ dest ] = hrec

        resp = self.__parseResp(logs, hrec)
        for k,v in resp.items():
              l = len(v)
              if self.maxCount[k] < l:
                    self.maxCount[k] = l

        return

    def __parseResp(self, block:list[str], hrec: _Host):
        '''parse each line in dig answer section.

           cf. https://github.com/kellyjonbrazil/jc/blob/master/jc/parsers/dig.py#L473

        Args:
           block(list[str]): lines in answer section.
           hrec(_Host):      record to keep result.

        Returns:
           dict[str, list[DigRespRecord]]: records in answer section, groupby type(IN|CNAME etc)
        '''
        
        for log in block:
              l = log.split(maxsplit=4)
              #if l[3] in 'CNAME':
              #      l[4] = l[4][0:-1] # chop last '.' from CNAME DATA.
              if l[4].endswith('.'):
                 l[4] = l[4][0:-1]     # chop last '.'.
              d = DigRespRecord(Name=l[0], Class=l[2], Type=l[3], Val=l[4], Ttl=l[1])
              hrec.append(d)
        return hrec.get('resp')

          
    def mkdict(self, keys:list[str], vals:list[Any]) -> dict[str,Any]:
        '''make dict from list of keys and values

        Args:
            keys: key of dict
            vals: val of dict
        '''

        if len(keys) != len(vals):
              raise RuntimeError(f'num of keys and vals is mismatched, keys:{keys}   vals:{vals}')
        return dict(zip(keys, vals))

    def mkData(self, fields:list[str]=['A','CNAME'], rtoh:dict[str,str]={'CNAME':'cname', 'A':'ip'} ):
        '''make data for output.

        Returns:
           list[dict[str,Any]]: data to output
        '''

        rtn = []

        recs, count = self.getResults()                        # get all results
        if not any(recs) or not any(count):
           print(f'########### no records found !')
           return rtn

        #
        # phase1) make keys for pretty-print
        #
        keys = [ 'target']
        for ih in rtoh.values():
              keys.append('num_' + ih)

        for k in fields:                          # typed record in dig result.
              c = count[k]                        #  num of record.
              k = rtoh[k]
              ndigits = len(str(c-1))
              nformat = '{:02d}' if ndigits==1 else '{:0'+str(ndigits)+'d}'
              nformat = k +'_' +nformat
              for cn in range(1,c+1):
                    h = nformat.format(cn)
                    keys.append(h)
        #end, making keys.

        #
        # phase2) make data parts
        #
        for q, hrec in recs.items():
            vals = [q]                   # initail value of data.
            recs = hrec.get('resp')
            for k in fields:             # get and fill num of CNAME and IP
                v = recs[k]
                l = len(v)
                vals.append(l)

            for k in fields:             # get and fill exact value of dig result
                vl   = recs[k]
                l    = len(vl)
                for d in vl:
                      vals.append(d.Val)    # fill exact data.
                for n in range(l,count[k]): # fill None if num of record < MAX count.
                      vals.append(None)
            d = self.mkdict(keys,vals)
            rtn.append(d)
        #end, making data part
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
    parser.add_argument('-r','--rev',               type=bool,default=False,           help='parse for reverse-resolve')
    parser.add_argument('-v','--verbose',           action="store_true",               help='verbose output or not')
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
    #         any/folder/fqdn1             => OK    the end of file path equals to FQDN
    #        /any/folder/fqdn2             => OK    the end of file path equals to FQDN
    #         any/folder/fqdn1.txt         => NG    it has extension
    #
    #   * content of ping log file has to meet below condition, mainly on executing ping command.
    #      - the logfile is assumed to be taken by the following command:
    #
    #        bash$  LANG=C dig fqdn1 > log/fqdn1
    #
    #      - any other style is not tested.
    #      - do not end ping command by CTRL-C.        => it may give damage in content in logfile.
    #      - NO l18n SUPPORT                           => if message in l18n,  no valid output.
    #      - if no message at lossing resp(no -O opt)  => sequencing will be broken in output.
    #      - do NOT modify any output                  => it makes result fake.
    #


    logFiles:dict[str,str] = OrderedDict() # dict of { log-path, fqdn }

    if not args.input:
        raise RuntimeError('dig log files required')

    with open(args.input, encoding='utf-8') as fp:
        content = fp.read().splitlines()

    if not any(content):
        print('... empty content', file=sys.stderr)
    for path in content:
        f = os.path.basename(path)
        logFiles[path] = f

    if args.verbose:
        print(logFiles)


    logparser = DigLogParser()
    for path, dest in logFiles.items():
        logparser.run(path, dest, verbose=args.verbose)


    #outheader = {'A':'ip', 'CNAME':'cname', 'PTR': 'name'}
    #outheader = {'A':'ip'}
    outheader = {'A':'ip', 'CNAME':'cname', 'PTR': 'name'}
    if args.rev:
        outheader = {'PTR':'name'}

    ldict = logparser.mkData(fields=outheader.keys(), rtoh=outheader)

    df = pd.DataFrame( ldict)
    if args.output.endswith('.xlsx'):
        df.to_excel(args.output, index=False)
    else:
        df.to_csv(args.output, index=False)
