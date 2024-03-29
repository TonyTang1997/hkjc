import pandas as pd
import numpy as np

def get_season(x):
    if x.month < 9:
        return (x.year - 1)
    else:
        return x.year

df = pd.read_csv('hkrace_cleaned.csv')

racecard = pd.read_json('racecard.json', lines=True)

racecard['race_date'] = pd.to_datetime(racecard['race_date'].astype(str), format='%Y%m%d')
racecard['race_date'] = racecard['race_date'].apply(lambda x: str(x).split(" ")[0])

racecard['result'] = np.nan
racecard['LBW'] = np.nan
racecard['special_incident'] = '0'
racecard['season'] = 2019

venue_dict = {"HappyValley":"HV","ShaTin":"ST"}
racecard['venue'] = racecard['venue'].map(venue_dict)

racecard['track'] = racecard['track'].str.upper()
racecard['config'] = racecard['config'].str.upper()

racecard['race_no'] = racecard['race_no'].apply(lambda x:int(x))
racecard['distance'] = racecard['distance'].apply(lambda x:int(x))
racecard['draw'] = pd.to_numeric(racecard['draw'], errors='coerce')
racecard['declared_weight'] = pd.to_numeric(racecard['declared_weight'], errors='coerce')
racecard['actual_weight'] = pd.to_numeric(racecard['actual_weight'], errors='coerce')

racecard = racecard.reset_index(drop=True)

combined_df = pd.concat([df,racecard])

combined_df = combined_df.sort_values(['race_date','race_no','finish_time','horse_number'],ascending=[True,True,True,True])

combined_df = combined_df[df.columns]

combined_df.to_csv("hkrace_cleaned_racecard.csv", idnex=False)