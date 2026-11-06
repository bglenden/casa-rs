#!/usr/bin/env python3
"""CASA 6.7.6-14 Stokes spectral assignment reference."""
import json
import numpy as np
from casaconfig import config
config.data_auto_update=False
config.measures_auto_update=False
from casatools import componentlist

def clean(x):
    if isinstance(x,dict): return {k:clean(v) for k,v in x.items()}
    if isinstance(x,np.ndarray): return clean(x.tolist())
    if isinstance(x,(list,tuple)): return [clean(v) for v in x]
    if isinstance(x,np.generic): return x.item()
    return x

def snapshot(c):
    return [clean(c.getcomponent(i)) for i in range(c.length())]

T={'type':'tabular','tabularfreqs':[1e9,2e9],'tabulari':[1.,2.],'tabularq':[3.,4.],'tabularu':[5.,6.],'tabularv':[7.,8.]}
cases=[('default',0,{}),('constant',0,{'type':'constant'}),('spectral_one',0,{'type':'spectral index','index':[-.7]}),('spectral_four',0,{'type':'spectral','index':[-.7,.1,2,.3],'reffreq':'1.2GHz'}),('spectral_two',0,{'type':'spectral index','index':[.5,.6]}),('spectral_five',0,{'type':'spectral index','index':[.5,.6,.7,.8,.9]}),('uppercase',0,{'type':'SPECTRAL'}),('mixed',0,{'type':'mixed spectral data'}),('constant_ref',1,{'type':'constant','reffreq':'3GHz','frame':'BARY'}),('bad',0,{'type':'bad'}),('empty_type',0,{'type':''}),('plp_abbrev',0,{'type':'plp'}),('plp_full',0,{'type':'Power Logarithmic Polynomial','index':[.2,.3]}),('tabular',0,T),('tab_second',1,T),('short_tab',0,dict(T,tabularfreqs=[1e9])),('bad_i',0,dict(T,tabulari=[1])),('bad_q',0,dict(T,tabularq=[1])),('bad_u',0,dict(T,tabularu=[1])),('bad_v',0,dict(T,tabularv=[1])),('bad_frame',0,dict(T,frame='garbage')),('mismatch_frame',0,dict(T,frame='TOPO')),('rest_frame',0,dict(T,frame='REST')),('wrong_refunit',0,{'type':'spectral','reffreq':'3m'}),('negative_index',-1,{'type':'spectral'}),('index_out',2,{'type':'spectral'}),('negative_tab',-1,T),('index_none',None,{'type':'spectral'}),('index_float',0.,{'type':'spectral'}),('ref_none',0,{'type':'spectral','reffreq':None}),('tab_none',0,{'type':'constant','tabularfreqs':None})]
cases.extend([('numpy_bool_true',np.bool_(True),{}),('numpy_bool_false',np.bool_(False),{}),
              ('empty_ref_float',0,{'reffreq':np.array([],dtype=float)}),
              ('empty_ref_int',0,{'reffreq':np.array([],dtype=int)}),
              ('plp_negative',-1,{'type':'power logarithmic polynomial'}),
              ('plp_out',2,{'type':'power logarithmic polynomial'})])
out=[]
for name,which,kwargs in cases:
 c=componentlist(); c.addcomponent(flux=3,dir='J2000 10deg 20deg',freq='LSRK 2GHz',label='first');c.addcomponent(flux=5,dir='GALACTIC 30deg -15deg',freq='LSRK 1.4GHz',label='second')
 before=snapshot(c)
 try: result={'result':c.setstokesspectrum(which,**kwargs)}
 except Exception as e: result={'error':{'type':type(e).__name__,'message':str(e)}}
 out.append({'case':name,'which':clean(which),'kwargs':clean(kwargs),'before':before,**result,'after':snapshot(c)});c.done()
print(json.dumps({'casatools':'6.7.6-14','cases':out},indent=2))
