import datetime
from pandas import DataFrame

def parser(json_data, monitor):
    result_df = DataFrame()
    neg = 0
    pos = 0
    neu = 0
    for node in json_data["results"]:
        try:
            results_dt = datetime.datetime.strptime(node["startDate"][:10], "%Y-%m-%d").date()
        except KeyError:
            results_dt = ''
        try:
            num_docs = str(node["numberOfDocuments"])
        except KeyError:
            num_docs = 0
        try:
            cat = node["categories"]
            if 'Negative' in cat[0]["category"]:
                neg = cat[0]["volume"]
            elif 'Positive' in cat[0]["category"]:
                pos = cat[0]["volume"]
            elif 'Neutral' in cat[0]["category"]:
                neu = cat[0]["volume"]

            if 'Negative' in cat[1]["category"]:
                neg = cat[1]["volume"]
            elif 'Positive' in cat[1]["category"]:
                pos = cat[1]["volume"]
            elif 'Neutral' in cat[1]["category"]:
                neu = cat[1]["volume"]

            if 'Negative' in cat[2]["category"]:
                neg = cat[2]["volume"]
            elif 'Positive' in cat[2]["category"]:
                pos = cat[2]["volume"]
            elif 'Neutral' in cat[2]["category"]:
                neu = cat[2]["volume"]
        except KeyError:
            neg = 0
            pos = 0
            neu = 0
        # create data frame and append to full frame
        df = DataFrame([[monitor, results_dt, num_docs, pos, neg, neu]],
                       columns=['MonitorID', 'ResultsDate', 'NumberOfDocuments', 'BasicPositive',
                                'BasicNegative', 'BasicNeutral'])
        result_df = result_df.append(df)
    return result_df
