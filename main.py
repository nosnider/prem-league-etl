import requests
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime


# define bigquery project and dataset name
bq_project = 'fantasy-pl-etl'
bq_dataset = 'raw'

# auth
credentials = service_account.Credentials.from_service_account_file(
    'creds.json',
)


# request data from fantasy pl api
r = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
data = r.json()

# save response objects to df
loaded_at = datetime.now()
gameweeks = pd.DataFrame(data['events'])
gameweeks['loaded_at'] = loaded_at
teams = pd.DataFrame(data['teams'])
teams['loaded_at'] = loaded_at
players = pd.DataFrame(data['elements'])
players['loaded_at'] = loaded_at
player_stats = pd.DataFrame(data['element_stats'])
player_stats['loaded_at'] = loaded_at
player_types = pd.DataFrame(data['element_types'])
player_types['loaded_at'] = loaded_at

if __name__ == “__main__”:
    # load to bigquery tables
    gameweeks.to_gbq('raw.gameweeks', project_id=bq_project, if_exists='replace', credentials=credentials)
    teams.to_gbq('raw.teams', project_id=bq_project, if_exists='replace', credentials=credentials)
    players.to_gbq('raw.players', project_id=bq_project, if_exists='replace', credentials=credentials)
    player_stats.to_gbq('raw.player_stats', project_id=bq_project, if_exists='replace', credentials=credentials)
    player_types.to_gbq('raw.player_types', project_id=bq_project, if_exists='replace', credentials=credentials)
