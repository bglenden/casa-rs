#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6.14 spectral velocity definitions, conversions and record forms."""
import argparse
from copy import deepcopy
import json
import resource
from pathlib import Path
from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import measures, version_string
import numpy as np
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--out', type=Path, required=True)
args = parser.parse_args()
assert version_string() == '6.7.6-14'


def clean(v):
    if isinstance(v,bytes):return {"__python_bytes__":list(v)}
    if isinstance(v, dict):return {k:clean(x) for k,x in v.items()}
    if isinstance(v, (list,tuple,np.ndarray)):return [clean(x) for x in v]
    if isinstance(v,np.generic):return v.item()
    return v


cases=[]
m=measures()
def capture(name, method, values, kwargs=None, frame=None):
    kw=kwargs or {}; item={'name':name,'method':method,'args':clean(values),'kwargs':clean(kw)}
    tool=measures()
    if frame:
        item['frame']=frame
        for element in frame:tool.doframe(element)
    try:item['result']=clean(getattr(tool,method)(*values,**kw))
    except Exception as error:item['error']={'type':type(error).__name__,'message':str(error)}
    finally:tool.done()
    cases.append(item)

for method, refs in [('doppler',['RADIO','OPTICAL','Z','RELATIVISTIC','BETA','RATIO','GAMMA','bad','']),('radialvelocity',['LSRK','LSRD','BARY','GEO','TOPO','GALACTO','LGROUP','CMB','bad',''])]:
    capture(method+'_default',method,[])
    for refer in refs:
        for i,value in enumerate(['10km/s','0.1','-2km/s',0.1,{'value':3,'unit':'km/s'},'1GHz','bad']):
            capture(f'{method}_{refer}_{i}',method,[refer,value])
    offset=getattr(m,method)(refs[0],'1km/s')
    for off in [offset,dict(offset,offset=deepcopy(offset)),m.frequency('LSRK','1GHz'),{}, {'bad':1}]:
        capture(method+'_offset_'+str(len(cases)),method,[refs[0],'10km/s',off])

frequency=m.frequency('BARY','1.3GHz'); rest=m.frequency('REST','1.42GHz')
velocity=m.radialvelocity('LSRK','10km/s'); doppler=m.doppler('RADIO','10km/s')
records=[frequency,velocity,doppler,{},m.direction('J2000','1deg','2deg')]
for rf in ['RADIO','OPTICAL','BETA','RATIO','GAMMA','bad']:
    for i,value in enumerate(records):
        for j,rfq in enumerate(['1.42GHz',{'value':1.42,'unit':'GHz'},rest,[]]):
            capture(f'todoppler_{rf}_{i}_{j}','todoppler',[rf,value,rfq])
for rf in ['LSRK','BARY','bad']:
    for i,value in enumerate(records):
        capture(f'toradialvelocity_{rf}_{i}','toradialvelocity',[rf,value])
        for j,rfq in enumerate([{'value':1.42,'unit':'GHz'},rest,{}, {'value':1,'unit':'m'}]):
            capture(f'tofrequency_{rf}_{i}_{j}','tofrequency',[rf,value,rfq])
for i,value in enumerate(records):
    for j,d in enumerate([doppler,velocity,{}]):capture(f'torestfrequency_{i}_{j}','torestfrequency',[value,d])

for kind,base in [('frequency',frequency),('radialvelocity',velocity),('doppler',doppler)]:
    variants=[deepcopy(base),deepcopy(base),deepcopy(base)]
    variants[2]['m0']['value']=[base['m0']['value']]
    variants[0]['m0']['value']=[base['m0']['value'],base['m0']['value']*2]
    off=deepcopy(base);off['m0']['value']*=.1
    variants[1]['offset']=off
    for j,value in enumerate(variants):
        capture(f'{kind}_{j}_todoppler','todoppler',['BETA',value,'1.42GHz'])
        capture(f'{kind}_{j}_measure','measure',[value,'BARY' if kind!='doppler' else 'OPTICAL'])
        capture(f'{kind}_{j}_extra','addxvalue',[value])
        capture(f'{kind}_{j}_offset','getoffset',[value])
        if kind=='doppler':
            capture(f'{kind}_{j}_tofrequency','tofrequency',['LSRK',value,rest])
            capture(f'{kind}_{j}_toradialvelocity','toradialvelocity',['LSRK',value])
# Retain the owner's empty-frame rule. Add real-frame comparisons separately.
real_frame=[m.epoch('UTC','2020/01/01'),m.observatory('ALMA'),m.direction('J2000','10deg','20deg')]
for case in list(cases):
    if case['name'] in ['radialvelocity_0_measure','radialvelocity_1_measure','radialvelocity_2_measure']:
        case['owner_decision']='measures-tool-default-frame'
        case['native_result']={}
        capture(case['name']+'_explicit_frame','measure',case['args'],frame=real_frame)
# New measure kinds must integrate with the existing record queries.
for kind,base in [('doppler',doppler),('radialvelocity',velocity)]:
    variants=[deepcopy(base),deepcopy(base),deepcopy(base)]
    variants[1]['m0']['value']=[base['m0']['value'],base['m0']['value']*2]
    variants[2]['offset']=deepcopy(base)
    for i,record in enumerate(variants):
        for method in ['getref','gettype','getvalue','ismeasure','listcodes','show','dirshow']:
            capture(f'inspect_{kind}_{i}_{method}',method,[record])
# Existing offset helpers now share the richer record decoder.
dir=m.direction('J2000','10deg','20deg');off=m.direction('J2000','1deg','2deg')
with_offset=deepcopy(dir);with_offset['offset']=deepcopy(off)
capture('support_direction_offset_addxvalue','addxvalue',[with_offset])
with_offset['offset']['extra']=np.array([1,2])
capture('support_offset_extra_array','getoffset',[with_offset])
with_offset['offset'].pop('extra');with_offset['offset']['offset']=m.direction('J2000','3deg','4deg')
capture('support_nested_offset','getoffset',[with_offset])
# The two rest-frequency parameters use different CASA readers.
for name,text,q in [('wavelength','21cm',{'value':21.,'unit':'cm'}),('period','0.7ns',{'value':.7,'unit':'ns'}),('angular_rate','2rad/s',{'value':2.,'unit':'rad/s'}),('energy','1e-24J',{'value':1e-24,'unit':'J'})]:
    capture('rest_units_'+name+'_text','todoppler',['RADIO',frequency,text])
    capture('rest_units_'+name+'_quantity','todoppler',['RADIO',frequency,q])
    capture('rest_units_'+name+'_tofrequency','tofrequency',['LSRK',doppler,q])
capture('rest_units_reference_text','todoppler',['RADIO',frequency,'BARY 1.42GHz'])
capture('rest_units_numeric','todoppler',['RADIO',frequency,1.42e9])
rest_length=deepcopy(rest);rest_length['m0']={'value':21.,'unit':'cm'}
capture('rest_units_frequency_record','todoppler',['RADIO',frequency,rest_length])
capture('rest_units_frequency_record_tofrequency','tofrequency',['LSRK',doppler,rest_length])
# Binding protocols, including UTF-8 bytes and explicit None versus omission.
for method,rf in [('doppler','RADIO'),('radialvelocity','LSRK')]:
    for i,ref in enumerate([rf.encode(),None,3,rf+'\0ignored']):capture(f'protocol_{method}_rf_{i}',method,[ref,'10km/s'])
    for i,value in enumerate([b'10km/s',b'10km/s\0ignored',None,[],10.0,[1.0,2.0],{'value':10.0,'unit':b'km/s'}]):capture(f'protocol_{method}_value_{i}',method,[rf,value])
    for i,off in enumerate([None,3,[]]):capture(f'protocol_{method}_off_{i}',method,[rf,'10km/s',off])
for method,values in [('todoppler',['RADIO',frequency,'1.42GHz']),('tofrequency',['LSRK',doppler,rest]),('toradialvelocity',['LSRK',doppler])]:
    for i,rf in enumerate([values[0].encode(),None,3,values[0]+'\0ignored']):capture(f'protocol_{method}_rf_{i}',method,[rf,*values[1:]])
    for i,value in enumerate([None,[],3]):capture(f'protocol_{method}_v0_{i}',method,[values[0],value,*values[2:]])
for index in [0,1]:
    for i,value in enumerate([None,[],3]):
        values=[frequency,doppler];values[index]=value
        capture(f'protocol_torestfrequency_arg{index}_{i}','torestfrequency',values)
for i,value in enumerate([None,[],3]):capture(f'protocol_tofrequency_rfq_{i}','tofrequency',['LSRK',doppler,value])
capture('protocol_todoppler_bytes_rest','todoppler',['RADIO',frequency,b'1.42GHz'])
# The rest-frequency helper also converts Doppler references internally.
doff=deepcopy(doppler);doff['offset']=m.doppler('RADIO','1km/s')
foff=deepcopy(frequency);foff['offset']=m.frequency('BARY','100MHz')
fvec=deepcopy(frequency);fvec['m0']['value']=[1.3e9,2.6e9]
dvec=deepcopy(doppler);dvec['m0']['value']=[1e4,2e4]
for name,f,d in [('offset',frequency,doff),('frequency_offset',foff,doppler),('frequency_vector',fvec,doppler),('doppler_vector',frequency,dvec),('both_vectors',fvec,dvec)]:
    capture('torestfrequency_'+name,'torestfrequency',[f,d])
# Preserve raw CASA failures/truncation and independent scalar controls.
for case in cases:
    if case['name'] not in ['frequency_0_todoppler','doppler_0_tofrequency','doppler_0_toradialvelocity','torestfrequency_frequency_vector','torestfrequency_doppler_vector','torestfrequency_both_vectors']:continue
    vector_positions=[i for i,arg in enumerate(case['args']) if isinstance(arg,dict) and isinstance(arg.get('m0',{}).get('value'),list)]
    width=len(case['args'][vector_positions[0]]['m0']['value'])
    controls=[]
    for i in range(width):
        values=deepcopy(case['args'])
        for index in vector_positions:values[index]['m0']['value']=values[index]['m0']['value'][i]
        controls.append(clean(getattr(measures(),case['method'])(*values,**case['kwargs'])))
    case['scalar_controls']=controls
    case['deviation']='Preserve every vector result; compare each element with a separate CASA scalar call. CASA rejects or truncates the vector.'
args.out.write_text(json.dumps({'casatools':version_string(),'cases':cases},indent=2)+'\n')
print(len(cases),'cases')
