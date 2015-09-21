from time import sleep
from datetime import timedelta
from datetime import datetime
import requests
import math
from pandas import DataFrame
from parsers import sources, posts, results


class CrimsonHexagonClient(object):
    REQUEST_LATENCY = 0.2

    def __init__(self, endpoint, monitor, start_dt, end_dt, username, password):
        self.start_time = datetime.now()
        self.rate_window = 60
        self.max_results = 10000
        self._auth()
        self.endpoint = endpoint
        self.monitor = monitor
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.chunk_start = start_dt
        self.chunk_end = end_dt
        self.username = username
        self.password = password
        # perhaps someday CH will make endpoint name the first node in contentsources
        if endpoint == 'posts':
            self.json_start = 'posts'
        elif endpoint == 'sources':
            self.json_start = 'contentSources'
        elif endpoint == 'results':
            self.json_start = 'results'
        else:
            print('Endpoint not implemented')

    def _auth(self):
        url = "https://forsight.crimsonhexagon.com/api/authenticate?"
        payload = {'username': self.username, 'password': self.password}
        auth_req = requests.get(url, params=payload)
        auth_res = auth_req.json()
        self.auth_token = auth_res["auth"]
        return

    # make the REST request
    def _make_req(self, start, end):
        start_str = start.strftime('%Y-%m-%d')
        end_str = end.strftime('%Y-%m-%d')
        url = 'https://forsight.crimsonhexagon.com/api/monitor/' + self.endpoint + '?'
        # set REST parameters
        payload = {'start': start_str, 'end': end_str, 'extendLimit': 'True', 'auth': self.auth_token, 'id': self.monitor}
        self.ch_req = requests.get(url, params=payload)
        # find the number of
        res_len = len(self.ch_req.json()[self.json_start])
        print(self.ch_req.url)
        return res_len

    def get_endpoint_timeframe(self):
        result_df = DataFrame()
        res_len = self._make_req(self.chunk_start, self.chunk_end)
        self._wait_for_rate_limit()
        # check to see if there are possibly more results to get if close to max_result
        # this will make additional requests until either the results are smaller than 9k or the timeframe is 1day
        if res_len / self.max_results > .90:
            delta = self.chunk_end - self.chunk_start
            step_size = math.floor(delta.days / 2)
            self.chunk_end = self.chunk_start + timedelta(days=step_size)
            # if step is greater than a day make request
            if self.chunk_start != self.chunk_end:
                self.get_endpoint_timeframe()
            # if no step save data and just increment another day
            else:
                self.chunk_start = self.chunk_end + timedelta(days=1)
                self.chunk_end = self.chunk_end + timedelta(days=1)
                self.get_endpoint_timeframe()
                # parse & append results to dataframe
                df = self._parse_json()
                result_df = result_df.append(df)

        # pick up where we left off from chunking
        elif self.chunk_end != self.end_dt:
            self.chunk_start = self.chunk_end
            self.chunk_end = self.end_dt
            self.get_endpoint_timeframe()
            # parse & append results to dataframe
            df = self._parse_json()
            result_df = result_df.append(df)
        return result_df

    # chooses the parser for the endpoint
    def _parse_json(self):
        df = None
        if self.endpoint == 'posts':
            df = posts.parser(self.ch_req.json(), self.monitor)
        elif self.endpoint == 'sources':
            df = sources.parser(self.ch_req.json(), self.monitor)
        elif self.endpoint == 'results':
            df = results.parser(self.ch_req.json(), self.monitor)
        else:
            print('End Point', self.endpoint, 'not yet implemented.')
        return df

    # perhaps in the future Crimson Hexagon will use a X-Rate-Limit-Reset header but for now we must keep track
    # currently 120 requests a minute
    def _wait_for_rate_limit(self):
        remaining = int(self.ch_req.headers['X-RateLimit-Remaining'])
        total_avail = int(self.ch_req.headers['X-RateLimit-Limit'])
        while remaining >= total_avail:
            time_elapsed = datetime.now() - self.start_time
            if time_elapsed < self.rate_window:
                time_remaining = time_elapsed - timedelta(seconds=self.rate_window)
                sleep(time_remaining)
                self.start_time = datetime.now()
