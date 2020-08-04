import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup, NavigableString, Tag
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import preprocessor as p
from webdriver_manager.chrome import ChromeDriverManager

## Construct your search criteria using twitters advanced search (link below) then copy/paste the url
# https://twitter.com/search-advanced?lang=en-gb

# url = "https://twitter.com/search?q=(%23barclays%20OR%20%23HSBC%20OR%20%23Lloyds%20OR%20%23Lloydsbank%20OR%20%23RBS%20OR%20%23RoyalBankOfScotland%20OR%20%23bank)%20(%40barclays%20OR%20%40HSBC%20OR%20%40HSBC_UK%20OR%20%40AskLloydsBank%20OR%20%40AskNationwide%20OR%20%40NatWestBusiness%20OR%20%40NatWest_Help%20OR%20%40BarclaysUK%20OR%20%40LBGplc%20OR%20%40AskHalifaxBank%20OR%20%40RBS%20OR%20%40RBS_Help%20OR%20%40TSB%20OR%20%40santanderukhelp%20OR%20%40AskBankOfScot)%20-filter%3Aretweets%20AND%20-filter%3Areplies%20AND%20-https%3A%2F%2F%27&src=typed_query&f=live"
# url = "https://twitter.com/search?lang=en-gb&q=(%23albanian%20OR%20%23albania%20OR%20%23shqiperi)&src=typed_query"
# url = "https://twitter.com/search?q=(%23goodwood%20OR%20%23goodwoodfestivalofspeed)&src=typed_query"
url = "https://twitter.com/search?q=(covid19 OR covid OR coronavirus)&src=typed_query"


browser = webdriver.Chrome(ChromeDriverManager().install())

browser.get(url)
time.sleep(1)

elem = browser.find_element_by_tag_name("body")

# need to scroll the page down to load more data
no_of_pagedowns = 200

while no_of_pagedowns:
    elem.send_keys(Keys.PAGE_DOWN)
    time.sleep(0.2)
    no_of_pagedowns -= 1

html = browser.page_source
# print(html)

soup = BeautifulSoup(html, "lxml")
browser.quit()

tweet_soup_list = soup.find_all("div", {"class": "original-tweet"})


def prettify_tweet_text_bs_element(tweet_text_bs_element):
    tweet_text = ''
    for child in tweet_text_bs_element.children:
        if isinstance(child, NavigableString):
            tweet_text += child + " "
        elif isinstance(child, Tag):
            try:
                tag_class = child['class'][0]
                if tag_class == "twee-atreply":
                    mention = ''.join([i.string for i in child.contents])
                    tweet_text += mention + " "
                elif tag_class == "twee-hashtag":
                    hashtag = ''.join([i.string for i in child.contents])
                    tweet_text += hashtag + " "
                elif tag_class == "twee-timeline-link":
                    if isinstance(child["href"], str):
                        tweet_text += child["href"] + " "
            except:
                if isinstance(child.string, str):
                    tweet_text += child.string + " "
    return " ".join(tweet_text.split())


tweet_dict = {'tweet_id': []
    , 'author_name': []
    , 'author_handle': []
    , 'author_id': []
    , 'author_href': []
    , 'tweet_permalink': []
    , 'tweet_text': []
    , 'tweet_language': []
    , 'tweet_time': []
    , 'tweet_timestamp': []
    , 'retweets': []
    , 'favorites': []
              }
for tweet_soup in tweet_soup_list:
    print(tweet_soup)
    tweet_dict["tweet_id"].append(int(tweet_soup["data-tweet-id"]))
    tweet_dict["author_name"].append(tweet_soup["data-name"])
    tweet_dict["author_handle"].append(tweet_soup["data-screen-name"])
    tweet_dict["author_id"].append(int(tweet_soup["data-user-id"]))
    tweet_dict["author_href"].append(tweet_soup.find(
        "a", {"class": "account-group"})["href"])
    tweet_dict["tweet_permalink"].append(tweet_soup["data-permalink-path"])
    tweet_dict["tweet_text"].append(prettify_tweet_text_bs_element(
        tweet_soup.find("p", {"class": "tweet-text"})))
    tweet_dict["tweet_language"].append(tweet_soup.find(
        "p", {"class": "tweet-text"})['lang'])
    tweet_dict["tweet_time"].append(tweet_soup.find(
        "a", {"class": "tweet-timestamp"})["title"])
    tweet_dict["tweet_timestamp"].append(tweet_soup.find(
        "span", {"class": "_timestamp"})["data-time-ms"])
    tweet_dict["retweets"].append(int(tweet_soup.find(
        "span", {"class": "ProfileTweet-action--retweet"}).find(
        "span", {"class": "ProfileTweet-actionCount"})['data-tweet-stat-count']))
    tweet_dict["favorites"].append(int(tweet_soup.find(
        "span", {"class": "ProfileTweet-action--favorite"}).find(
        "span", {"class": "ProfileTweet-actionCount"})['data-tweet-stat-count']))

tweet_df = pd.DataFrame(tweet_dict)

print(tweet_df)


# engine = create_engine(
#     'postgresql://user:password@nonprod-dsol-prototyping-db.ctolc6xouppg.eu-west-1.rds.amazonaws.com:5432/dev')
#
# tweet_df.to_sql('TG_STG_Twitter_data', engine, schema='prototyping')
#
# tweet_df = pd.read_sql('''
#             select * from prototyping."TG_STG_Twitter_data"''', engine)
#
# tweet_df['tweet_text_clean'] = tweet_df['tweet_text'].apply(p.clean)
# tweet_df['tweet_text_tokenize'] = tweet_df['tweet_text'].apply(p.tokenize)
# p.set_options(p.OPT.URL, p.OPT.EMOJI)
# tweet_df['tweet_text_clean_with_hashtag'] = tweet_df['tweet_text'].apply(p.clean)
