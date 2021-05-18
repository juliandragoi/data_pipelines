from googleapiclient import discovery
import json
import yaml
from pathlib import Path
import os
import pandas as pd
import time

script_location = Path(__file__).absolute().parent
head, tail = os.path.split(script_location)
file_location = os.path.join(head,'utils','config.yaml')

with open(file_location, 'r') as stream:
    creds = yaml.safe_load(stream)
    persp_creds = creds['perspective_api']


client = discovery.build(
  "commentanalyzer",
  "v1alpha1",
  developerKey=persp_creds['api_key'],
  discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
  static_discovery=False
)


def make_request(data_string):

  analyze_request = {
  'comment': {'text': data_string},
  'requestedAttributes': {'TOXICITY': {}, 'SEVERE_TOXICITY': {}, 'IDENTITY_ATTACK': {},'INSULT': {},'THREAT': {}
    ,'PROFANITY': {},'OBSCENE': {},'INFLAMMATORY': {}
                          }
  }

  response = client.comments().analyze(body=analyze_request).execute()

  TOXICITY = response['attributeScores']['TOXICITY']['summaryScore']['value']
  SEVERE_TOXICITY = response['attributeScores']['SEVERE_TOXICITY']['summaryScore']['value']
  IDENTITY_ATTACK = response['attributeScores']['IDENTITY_ATTACK']['summaryScore']['value']
  INSULT = response['attributeScores']['INSULT']['summaryScore']['value']
  THREAT = response['attributeScores']['THREAT']['summaryScore']['value']
  PROFANITY = response['attributeScores']['PROFANITY']['summaryScore']['value']
  OBSCENE = response['attributeScores']['OBSCENE']['summaryScore']['value']
  INFLAMMATORY = response['attributeScores']['INFLAMMATORY']['summaryScore']['value']


  data = {'data_string':data_string, 'TOXICITY':TOXICITY, 'SEVERE_TOXICITY':SEVERE_TOXICITY
    , 'IDENTITY_ATTACK':IDENTITY_ATTACK, 'INSULT':INSULT, 'THREAT':THREAT, 'PROFANITY':PROFANITY
    , 'OBSCENE':OBSCENE, 'INFLAMMATORY':INFLAMMATORY}

  return data


if __name__ == '__main__':

  file = '/Users/juliandragoi/newsnlp-data-science-pipelines/scrape/test.csv'

  df = pd.read_csv(file)
  count = 0

  for index, row in df.iterrows():
    count += 1
    if count == 60:
      count = 0
      time.sleep(70)
    print(row)
    try:
        res = make_request(row['text'])
        print(json.dumps(res, indent=2))
        # print(res)
    except:
      pass