import datetime
import re
from pandas import DataFrame

# assign max score to be the sentiment
def max_sentiment(neg, neu, pos):
    sentiment = ''
    if neu >= neg and neu >= pos:
        sentiment = 'Neutral'
    elif pos >= neu and pos >= neg:
        sentiment = 'Positive'
    elif neg >= pos and neg >= neu:
        sentiment = 'Negative'
    return sentiment

# parse crimson hexagon posts endpoint
def parser(json_data, monitor):
    result_df = DataFrame()
    # initialize sentiment
    neg = 0
    neu = 0
    pos = 0
    # traverse json
    for node in json_data["posts"]:
        try:
            author = re.sub('\s+', ' ', node["author"])
        except KeyError:
            author = ''
        try:
            # input format 2012-07-19T19:50:38
            post_dt = datetime.datetime.strptime(node["date"][:10], "%Y-%m-%d").date()
        except KeyError:
            post_dt = ''
        try:
            url = node["url"]
        except KeyError:
            url = ''
        try:
            post_title = re.sub('\s+', ' ', node["title"])
        except KeyError:
            post_title = ''
        try:
            network = node["type"]
        except KeyError:
            network = ''
        try:
            cat = node["categoryScores"]
            # there are always three ill defined categories that can change order that correspond to neg, pos, neu posts
            # inspect 1st node
            if 'Negative' in cat[0]["categoryName"]:
                neg = cat[0]["score"]
            elif 'Positive' in cat[0]["categoryName"]:
                pos = cat[0]["score"]
            elif 'Neutral' in cat[0]["categoryName"]:
                neu = cat[0]["score"]
            # inspect 2nd node
            if 'Negative' in cat[1]["categoryName"]:
                neg = cat[1]["score"]
            elif 'Positive' in cat[1]["categoryName"]:
                pos = cat[1]["score"]
            elif 'Neutral' in cat[1]["categoryName"]:
                neu = cat[1]["score"]
            # inspect 3rd node
            if 'Negative' in cat[2]["categoryName"]:
                neg = cat[2]["score"]
            elif 'Positive' in cat[2]["categoryName"]:
                pos = cat[2]["score"]
            elif 'Neutral' in cat[2]["categoryName"]:
                neu = cat[2]["score"]
            # assign max score to be the sentiment
            sentiment = max_sentiment(neg, neu, pos)
        except KeyError:
            sentiment = ''
        # create data frame and append to full frame
        df = DataFrame([[monitor, author, post_dt, url, post_title, network, sentiment]],
                       columns=['MonitorID', 'Author', 'PostDate', 'URL', 'PostTitle', 'Network', 'Sentiment'])
        result_df = result_df.append(df)
    return result_df
