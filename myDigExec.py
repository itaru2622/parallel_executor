#!/usr/bin/env python3

import subprocess
from typing import Any
import jc


def queryDig(dst:str, rev:bool=False) -> list[dict]:

    cmd = [ 'dig' ]
    if rev:
       cmd.append('-x')
    cmd.append(dst)

    res = subprocess.check_output(cmd, text=True)
    out = jc.parse('dig', res)
    rtn = out[0]['answer']
    return rtn,res

def extract(ldict:list[str,Any], cond:dict[str,Any], key:str='data' ) -> list[Any]:

    rtn = []
    lcond = len(cond.keys()) 
    for d in ldict:
        match = 0
        for k,v in cond.items():
           if d.get(k,None) not in [v]:
              continue
           match +=1
        if match == lcond:
           rtn.append( d.get(key) )
    return rtn

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

if __name__ == '__main__':
    import argparse
    import sys
    import jc

    parser = argparse.ArgumentParser()
    parser.add_argument('dest',                     type=str,  default=None,    help='destination')
    parser.add_argument('--no-revresolve',          action='store_true',        help='skip reverse resolv')

    args = parser.parse_args()
    print(args, file=sys.stderr)

    js,raw = queryDig(args.dest)
    if args.no_revresolve in [ False ]:
        ips = extract(js, cond={'type': 'A'} )
        for ip in ips:
            js2,raw2 = queryDig(ip, rev=True)
            js.extend(js2)
            raw +=raw2
    print(raw)
