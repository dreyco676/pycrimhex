import datetime
from pandas import DataFrame

# parse crimson hexagon content sources endpoint
def parser(json_data, monitor):
    result_df = DataFrame()
    if len(json_data["contentSources"]) != 0:
        for node in json_data["contentSources"]:
            try:
                src_dt = datetime.datetime.strptime(node["startDate"][:10], "%Y-%m-%d").date()
            except KeyError:
                src_dt = ''
            try:
                src = node["sources"]
            except KeyError:
                src = None
            try:
                comments = src["Comments"]
            except KeyError:
                comments = 0
            try:
                reviews = src["Reviews"]
            except KeyError:
                reviews = 0
            try:
                blogs = src["Blogs"]
            except KeyError:
                blogs = 0
            try:
                facebook = src["Facebook"]
            except KeyError:
                facebook = 0
            try:
                youtube = src["YouTube"]
            except KeyError:
                youtube = 0
            try:
                forums = src["Forums"]
            except KeyError:
                forums = 0
            try:
                twitter = src["Twitter"]
            except KeyError:
                twitter = 0
            try:
                news = src["News"]
            except KeyError:
                news = 0
            # create data frame and append to full frame
            df = DataFrame([[monitor, src_dt, src, comments, reviews, blogs, facebook, youtube, forums, twitter, news]],
                           columns=['MonitorID', 'SourcesDate', 'Comments', 'Reviews', 'Blogs', 'Facebook', 'Youtube',
                                    'Forums', 'Twitter', 'News'])
            result_df = result_df.append(df)
    return result_df
