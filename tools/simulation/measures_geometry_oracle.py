#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6.14 baseline/uvw/magnetic tools and their record consumers."""
import argparse
from copy import deepcopy
import json
import math
from pathlib import Path
import resource
from casaconfig import config
config.data_auto_update=False
config.measures_auto_update=False
import numpy as np
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--out',type=Path,required=True)
p.add_argument('--measures-data', type=Path, required=True)
a=p.parse_args()
config.measurespath = str(a.measures_data.resolve())
config.datapath = [config.measurespath, *config.datapath]
from casatools import measures,quanta,version_string
assert version_string()=='6.7.6-14'

def clean(v):
    if isinstance(v,bytes):return {'__python_bytes__':list(v)}
    if isinstance(v,dict):return {k:clean(x) for k,x in v.items()}
    if isinstance(v,(list,tuple,np.ndarray)):return [clean(x) for x in v]
    if isinstance(v,np.generic):return clean(v.item())
    if isinstance(v,float) and not math.isfinite(v):return str(v)
    return v

cases=[]
def capture(name,method,args=None,kwargs=None,frame=None):
    args=[] if args is None else args;kwargs={} if kwargs is None else kwargs
    c={'name':name,'method':method,'args':clean(args),'kwargs':clean(kwargs),'initial_frame':'empty after done; existing owner decision'}
    tool=measures();tool.done()
    try:
        if frame:
            c['frame']=clean(frame)
            for element in frame:assert tool.doframe(element)
        result=getattr(tool,method)(*args,**kwargs)
        c['result']=clean(result)
        c['python_type']=type(result).__name__
    except Exception as e:c['error']={'type':type(e).__name__,'message':str(e)}
    finally:tool.done()
    cases.append(c)

m=measures()
frame=[m.epoch('UTC','2020/01/01'),m.observatory('ALMA'),m.direction('J2000','10deg','20deg')]
records={}
for method,default,unit in [('baseline','ITRF','m'),('uvw','ITRF','m'),('earthmagnetic','IGRF','nT')]:
    capture(method+'_default',method)
    valid=['3nT','10deg','20deg'] if method=='earthmagnetic' else ['1m','2m','3m']
    base=getattr(m,method)(default,*valid)
    records[method]=base
    capture(method+'_codes','listcodes',[base])
    refs=m.listcodes(base)['normal']
    for ref in [*refs,'bad','']:
        capture(method+'_ref_'+ref,method,[ref,*valid],frame=frame)
    values=[['1'+unit,'2'+unit,'3'+unit],['10deg','20deg','3'+unit],['3'+unit,'10deg','20deg'],[1.,2.,3.],[{'value':1.,'unit':unit},{'value':2.,'unit':unit},{'value':3.,'unit':unit}],['bad','2'+unit,'3'+unit],['1Hz','2Hz','3Hz'],[None,[],[]]]
    for i,args in enumerate(values):capture(method+'_values_'+str(i),method,[default,*args],frame=frame)
    if method!='earthmagnetic':
        for n in [1,2,3]:
            q=[{'value':[float(i+j+1) for i in range(n)],'unit':'m'} for j in range(3)]
            capture(method+'_vector_'+str(n),method,[default,*q],frame=frame)
        capture(method+'_bad_vector_lengths',method,[default,{'value':[1.,2.],'unit':'m'},'2m','3m'],frame=frame)
    for i,off in enumerate([base,dict(base,offset=deepcopy(base)),m.frequency('BARY','1GHz'),{'bad':1},{}]):
        capture(method+'_offset_'+str(i),method,[default,*valid,off],frame=frame)
    for i,rf in enumerate([default.encode(),None,3,default+'\0ignored']):
        capture(method+'_protocol_rf_'+str(i),method,[rf,*valid],frame=frame)

for kind,base in records.items():
    variants=[deepcopy(base),deepcopy(base),deepcopy(base)]
    for key in ['m0','m1','m2']:
        if key in variants[1]:variants[1][key]['value']=[base[key]['value'],base[key]['value']*2]
    variants[2]['offset']=deepcopy(base)
    for i,v in enumerate(variants):
        for method in ['getref','gettype','getvalue','ismeasure','show','dirshow','listcodes','getoffset','addxvalue']:
            capture(kind+'_inspect_'+str(i)+'_'+method,method,[v],frame=frame)
        target='J2000' if kind!='earthmagnetic' else 'ITRF'
        capture(kind+'_measure_'+str(i),'measure',[v,target],frame=frame)
        capture(kind+'_touvw_'+str(i),'touvw',[v],frame=frame)
        capture(kind+'_expand_'+str(i),'expand',[v],frame=frame)

for kind in ['baseline','uvw','earthmagnetic']:
    base=deepcopy(records[kind])
    if kind=='earthmagnetic':
        base = {
            'type': kind, 'refer': 'J2000',
            **{f'm{i}': {'value': value, 'unit': 'nT'}
               for i, value in enumerate((10000., 20000., 30000.))},
        }
    codes=m.listcodes(base)['normal']
    for ref in codes:
        capture(kind+'_convert_'+ref,'measure',[base,ref],frame=frame)

position=m.position('WGS84','10deg','20deg','100m')
vector=deepcopy(position)
for key in ['m0','m1','m2']:vector[key]['value']=[position[key]['value'],position[key]['value']*1.1]
for i,record in enumerate([position,vector,records['baseline'],records['uvw'],m.direction('J2000','10deg','20deg'),{},dict(position,offset=deepcopy(position))]):
    capture('asbaseline_'+str(i),'asbaseline',[record],frame=frame)
for method in ['touvw','expand']:
    capture(method+'_default',method)
    capture(method+'_empty_record',method,[{}])
    capture(method+'_missing_frame',method,[records['baseline']])
    for i,value in enumerate([None,3,[]]):capture(method+'_protocol_'+str(i),method,[value])
    for n in [2,3,4]:
        v=deepcopy(records['baseline'])
        for j,key in enumerate(['m0','m1','m2']):v[key]['value']=[v[key]['value']*(i+1)+j*.01 for i in range(n)]
        capture(method+'_baseline_vector_'+str(n),method,[v],frame=frame)
# Independent controls for the direct FK4 path and the HADEC handedness
# boundary, including directions in which the neighboring AZEL hop differs.
for kind in ['baseline', 'uvw']:
    for source, target in [('B1950','APP'), ('APP','B1950'), ('HADEC','ITRF'),
                           ('ITRF','HADEC'), ('HADEC','AZEL'), ('AZEL','HADEC')]:
        value = getattr(m, kind)(source, '1m', '2m', '3m')
        capture(f'{kind}_hop_{source}_{target}', 'measure', [value, target], frame=frame)
# An ordinary vector may happen to share the encoded radius of a
# negative geodetic height. Baseline and UVW records must preserve it.
for kind in ['baseline', 'uvw']:
    capture(f'{kind}_position_sentinel_radius', kind, ['ITRF', '743.5685m', '0m', '0m'])
    value = getattr(m, kind)('ITRF', '743.5685m', '0m', '0m')
    capture(f'{kind}_position_sentinel_show', 'show', [value])
    capture(f'{kind}_position_sentinel_cartesian', 'addxvalue', [value])

for source, target in [('ITRF', 'HADEC'), ('HADEC', 'ITRF'),
                       ('AZEL', 'HADEC'), ('AZEL', 'AZELSW')]:
    value = m.uvw(source, '1m', '2m', '3m')
    terrestrial = [frame[1], m.direction(source, '10deg', '20deg')]
    capture(f'uvw_hop_{source}_{target}_epoch_required', 'measure', [value, target], frame=terrestrial)
    capture(f'uvw_hop_{source}_{target}_terrestrial_phase', 'measure', [value, target],
            frame=[frame[0], *terrestrial])

# Independent CASA direction conversions normalize the input to each
# aberration hop. MVEarthMagnetic skips that normalization and changes
# the field strength, especially on the direct FK4 APP/B1950 route.
xyz = [10000., 20000., 30000.]
length = math.hypot(*xyz)
assert all(m.doframe(element) for element in frame)


def direction_control(case, destination, magnitude, explanation):
    source = case['args'][0]['refer']
    direction = m.direction(source, {'value': math.atan2(xyz[1], xyz[0]), 'unit': 'rad'},
                            {'value': math.asin(xyz[2] / length), 'unit': 'rad'})
    reference = m.measure(direction, destination)
    lon, lat = (reference[f'm{i}']['value'] for i in range(2))
    unit = [math.cos(lat) * math.cos(lon), math.cos(lat) * math.sin(lon), math.sin(lat)]
    case['direction_control'] = clean(reference)
    case['native_result'] = {
        'type': 'earthmagnetic', 'refer': case['result']['refer'],
        **{f'm{i}': {'value': v * magnitude, 'unit': 'nT'} for i, v in enumerate(unit)},
    }
    case['deviation'] = explanation


horizon_refs = {'HADEC', 'AZEL', 'AZELSW', 'AZELNE', 'AZELGEO',
                'AZELSWGEO', 'AZELNEGEO', 'ITRF', 'IGRF'}
for case in cases:
    if case['name'].startswith('earthmagnetic_convert_') and case['args'][1] in horizon_refs:
        target = case['args'][1]
        direction_control(case, 'ITRF' if target == 'IGRF' else target, length,
                          'Preserve field strength through diurnal aberration. '
                          'CASA changes the norm; its separate direction conversion supplies the control.')

for source, target in [('B1950', 'APP'), ('APP', 'B1950')]:
    for scale in [1e-4, 1.]:
        field = {
            'type': 'earthmagnetic', 'refer': source,
            **{f'm{i}': {'value': v * scale, 'unit': 'nT'} for i, v in enumerate(xyz)},
        }
        capture(f'earthmagnetic_fk4_{source}_{target}_{scale:g}', 'measure', [field, target], frame=frame)
        direction_control(cases[-1], target, length * scale,
                          'Use the direct FK4 transformation on a unit vector and preserve '
                          'field strength. CASA changes the norm or produces NaNs; the '
                          'separate direction conversion supplies the control.')
m.done()
# Qualify the observed CASA defects with independent quantity/record paths.
qa=quanta()
for case in cases:
    result=case.get('result')
    if case['method'] in ['touvw','expand'] and isinstance(result,list) and result and result[0]=={}:
        case['native_result']=[{} for _ in result]
        case['deviation']='A failed conversion returns empty auxiliary records; CASA retains stale auxiliary values from an earlier call.'
    if case['name'] in ['baseline_values_2','uvw_values_2']:
        ref, radius, lon, lat = case['args']
        case['native_result'] = clean(getattr(m, case['method'])(ref, lon, lat, radius))
        case['deviation'] = 'Keep the length and both angles in a length-first field. CASA wrongly reorders this form to an invalid quantity triple and emits a zero vector; its angle-first form supplies the control.'
    if case['method'] == 'expand' and case['args'] and isinstance(case['args'][0], dict) and case['args'][0].get('type') == 'earthmagnetic':
        case['native_result'] = [{}, {}]
        case['deviation'] = 'Refuse a magnetic field in pairwise geometric expansion. CASA relabels it as uvw and emits length data with the original magnetic type.'
    if case['name'] in ['baseline_values_6','uvw_values_6']:
        case['unit_controls']=[qa.compare(qa.quantity(v),qa.quantity('1m')) for v in ['1Hz','2Hz','3Hz']]
        assert not any(case['unit_controls'])
        case['native_result']={}
        case['deviation']='Refuse incompatible dimensions rather than silently replacing invalid frequency-valued baselines with zero.'
    if case['method']=='earthmagnetic' and isinstance(result,dict) and result and len(case['args'])>=4:
        try:
            q=[qa.quantity(v) for v in case['args'][1:4]]
            if not qa.compare(q[0],qa.quantity('1nT')):continue
            if not all(qa.compare(v,qa.quantity('1rad')) for v in q[1:]):continue
            length=float(qa.convert(q[0],'nT')['value'])
            lon,lat=[float(qa.convert(v,'rad')['value']) for v in q[1:]]
            if length==0.:continue
            xyz=[length*math.cos(lon)*math.cos(lat),length*math.sin(lon)*math.cos(lat),length*math.sin(lat)]
            record=dict(type='earthmagnetic',refer=result['refer'],**{'m'+str(i):{'unit':'nT','value':v} for i,v in enumerate(xyz)})
            control=m.measure(record,result['refer'])
            case['record_control']=clean(control)
            if 'offset' in result:control['offset']=deepcopy(result['offset'])
            case['native_result']=clean(control)
            case['deviation']='Correct the spherical magnetic constructor SI-to-nT mismatch; expected values use CASA quantity conversions and a separate explicit Cartesian measure record.'
        except Exception as error:raise RuntimeError(case['name']) from error
    if case['method']=='uvw' and len(case['args'])==5 and case['args'][4].get('type')=='uvw' and isinstance(result,dict) and result:
        absolute=m.uvw(*case['args'][:4])
        case['native_result']=clean(m.measure(absolute,case['args'][0],case['args'][4]))
        case['deviation']='Retain a valid UVW reference offset; CASA constructor discards it. The independent generic measure operation supplies the control.'
    if case['method']=='addxvalue' and case['args'][0].get('type')=='earthmagnetic' and isinstance(result,dict) and result:
        values=[float(np.asarray(qa.convert(case['args'][0]['m'+str(i)],'nT')['value']).reshape(-1)[0]) for i in range(3)]
        case['native_result']={'unit':'nT','value':values}
        case['deviation']='Magnetic Cartesian extras have nT units; CASA inherits the metre label from MVPosition.'
for kind,base in [('direction',m.direction('J2000','10deg','20deg')),('position',m.position('ITRF','1m','2m','3m'))]:
    vector=deepcopy(base)
    for name,value in vector.items():
        if name.startswith('m'):value['value']=[value['value'],value['value']*2]
    capture('addxvalue_vector_'+kind,'addxvalue',[vector])
m.done()
a.out.write_text(json.dumps({'casatools':version_string(),'measures_snapshot': json.loads((a.measures_data / '.casa-rs-measures-provenance.json').read_text()),'cases':cases},indent=2)+'\n')
print(len(cases),'cases')
