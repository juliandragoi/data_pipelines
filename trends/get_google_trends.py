import pandas as pd
from pytrends.request import TrendReq
from datetime import datetime
from utils.helpers import get_engine

pytrend = TrendReq()


# Keyword by region breakdown

# pytrend.build_payload(kw_list=["Taylor Swift"])
# # Interest by Region
# df = pytrend.interest_by_region()
# print(df)


def get_trend_searches_in_region(region):

    df = pytrend.trending_searches(pn=region)

    return df


def get_todays_searches():
    todays_trends = pytrend.today_searches()

    todays_keywords = todays_trends.to_list()
    df = pd.DataFrame(todays_keywords, columns=['keywords'])

    df['captured_at'] = str(datetime.now().strftime("%Y-%m-%d_%H:00"))

    return df


if __name__ == '__main__':

    trends = get_todays_searches()

    print(trends)

    trends.to_sql(schema='staging', name='google_trends_today', con=get_engine(), if_exists='replace')



