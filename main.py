import requests
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime
import logging
from string import Template
import config
from google.cloud import bigquery


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

def main(data, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        current_time = datetime.utcnow()
        log_message = Template('Cloud Function was triggered on $time')
        logging.info(log_message.safe_substitute(time=current_time))

        try:
            gameweeks.to_gbq('raw.gameweeks', project_id=bq_project, if_exists='replace', credentials=credentials)
            teams.to_gbq('raw.teams', project_id=bq_project, if_exists='replace', credentials=credentials)
            players.to_gbq('raw.players', project_id=bq_project, if_exists='append', credentials=credentials)
            player_stats.to_gbq('raw.player_stats', project_id=bq_project, if_exists='replace', credentials=credentials)
            player_types.to_gbq('raw.player_types', project_id=bq_project, if_exists='replace', credentials=credentials)

        except Exception as error:
            log_message = Template('Query failed due to '
                                   '$message.')
            logging.error(log_message.safe_substitute(message=error))

    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)

if __name__ == "__main__":
   main('data','context')
