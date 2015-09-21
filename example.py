import ch_request as req
from datetime import datetime

# crimson hexagon credentials
ch_user = "an_email@gmail.com"
ch_pw = "password"
# crimson hexagon monitor id
ch_id = '123456789'

start_dt = datetime.strptime('2015-09-01', '%Y-%m-%d')
end_dt = datetime.strptime('2015-09-10', '%Y-%m-%d')


crimhex = req.CrimsonHexagonClient('posts', ch_id, start_dt, end_dt, ch_user, ch_pw)
# returns a dataframe
posts_df = crimhex.get_endpoint_timeframe()

crimhex = req.CrimsonHexagonClient('sources', ch_id, start_dt, end_dt, ch_user, ch_pw)
# returns a dataframe
sources_df = crimhex.get_endpoint_timeframe()

crimhex = req.CrimsonHexagonClient('results', ch_id, start_dt, end_dt, ch_user, ch_pw)
# returns a dataframe
results_df = crimhex.get_endpoint_timeframe()
