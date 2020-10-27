import pandas as pd

from pymongo import MongoClient
client = MongoClient('localhost', 27017)

def get_season(x):
    if x.month < 9:
        return (x.year - 1)
    else:
        return x.year

db = client.hkjc
hkrace = db.hkrace
df = pd.DataFrame(list(hkrace.find()))

columns = ['race_date', 'venue', 'race_code', 'race_no', 'season',
       'track', 'config', 'condition', 'race_class', 'distance', 'result',
       'horse_number', 'horse_name', 'horse_id', 'jockey', 'trainer',
       'actual_weight', 'declared_weight', 'draw', 'LBW', 'finish_time',
       'win_odds','running_pos_1', 'running_pos_2',
       'running_pos_3', 'running_pos_4', 'running_pos_5', 'running_pos_6']

df['race_date'] = pd.to_datetime(df['race_date'], format='%d/%m/%Y')
df = df.sort_values(['race_date','race_no']).reset_index(drop=True)
df['global_race_id'] = df.race_date.astype(str)+' '+df.race_no.astype(str)

df = df[df.result != '']
df = df[df.finish_time != '---']

df['result'] = df['result'].apply(lambda x: str(x).replace(' DH',''))
df['result'] = df['result'].str.replace('99','DNF')
df['result'] = df['result'].str.replace('nan','DNF')
df['result'] = df['result'].fillna("REFUND")
df['result'] = df['result'].apply(lambda x: str(x).replace('47','7'))

special_incident_list = ['WV-A', 'PU', 'WV', 'WX', 'VOID', 'TNP', 'WX-A', 'FE', 'DISQ','UR', 'DNF', 'poor', 'WXNR', 'REFUND']
df['special_incident'] = df['result'].apply(lambda x: x if x in special_incident_list else '0')

df['result'] = pd.to_numeric(df['result'], errors='coerce')

#df = df.drop(df[(df['result'].isna()) & (df.special_incident == '0')].index)

df['season'] = df.race_date.apply(lambda x: get_season(x))

df = df[df.finish_time.notnull()]
df = df[df.venue != 'Conghua']
venue_dict = {"Happy Valley":"HV","Sha Tin":"ST"}
df['venue'] = df['venue'].map(venue_dict)


df["finish_time"] = df["finish_time"].str.replace('.', ':')
df['finish_time'] = df['finish_time'].str.split(':').apply(lambda x: int(x[0]) * 3600 + int(x[1]) * 60 + int(x[2]))

df['race_no'] = df['race_no'].apply(lambda x:int(x))
df['distance'] = df['distance'].apply(lambda x:int(x))
df['draw'] = pd.to_numeric(df['draw'], errors='coerce')
df['declared_weight'] = pd.to_numeric(df['declared_weight'], errors='coerce')
df['actual_weight'] = pd.to_numeric(df['actual_weight'], errors='coerce')
df['win_odds'] = pd.to_numeric(df['win_odds'], errors='coerce')

df = df.sort_values(['race_date','race_no','horse_number'],ascending=[True,True,True])

df = df[columns]

df = df[df.win_odds.notnull()]
df = df[df.win_odds != 0]

df = df.reset_index(drop=True)

df.to_csv('../tony/hkjc_model/hkrace_cleaned.csv', index = False)
