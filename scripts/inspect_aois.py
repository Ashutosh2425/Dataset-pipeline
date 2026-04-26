import json
aois = json.load(open('data/aoi_list.json'))
event_types = {}
for a in aois:
    eid = a['event_id']
    et = a.get('event_type', '?')
    event_types[eid] = et
for eid, et in sorted(event_types.items()):
    print(f'{eid}: {et}')
