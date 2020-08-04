import pandas as pd
from pytrends.request import TrendReq
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

    df = pytrend.today_searches()

    return df




if __name__ == '__main__':


    pass

