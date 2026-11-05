import json
from casaconfig import config
config.data_auto_update=config.measures_auto_update=False
from casatools import quanta,version_string
q=quanta()
values=['bad',None,True,False,[],{},[1.0],[1.0,2.0],[[1.0,2.0]],1,1.5]
results=[]
for value in values:
 for method in ('quantity','getunit','add'):
  args=[{'value':value,'unit':'m'}]+([1.] if method=='add' else [])
  case={'method':method,'args':args}
  try:
   result=getattr(q,method)(*args)
   if isinstance(result,dict) and hasattr(result.get('value'), 'tolist'): result['value']=result['value'].tolist()
   case['result']=result
  except Exception as e:case['error']={'type':type(e).__name__,'message':str(e)}
  results.append(case)
print('CASA_RESULT='+json.dumps({'casatools':version_string(),'cases':results}))
