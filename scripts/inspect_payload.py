import json
import runpy

mod = runpy.run_path('app.py')
ds = mod['datastore']
props = ds.get_properties()
with_coords = [p for p in props if p.get('hasCoordinates')]
print('PROPS_TOTAL', len(props))
print('PROPS_WITH_COORDS', len(with_coords))
sample = next((p for p in with_coords if isinstance(p.get('latitude'), (int, float)) and isinstance(p.get('longitude'), (int, float))), None)
print('SAMPLE_WITH_COORDS:', json.dumps(sample or {}, ensure_ascii=False)[:800])
