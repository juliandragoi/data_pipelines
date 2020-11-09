
from companies_house.api import CompaniesHouseAPI

#
api_key = '1KlWDXHwUv8lJ5ePp0B-N_9M2gTvI05AxaoQN5BH'

ch = CompaniesHouseAPI(api_key)



x = ch.get(query='tesco')

print(x)

# import requests
#
# r = requests.get('https://api.companieshouse.gov.uk/search/fashion', auth=('1KlWDXHwUv8lJ5ePp0B-N_9M2gTvI05AxaoQN5BH', ''))
# print(r.text)