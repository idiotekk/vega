import os
import sys
import pandas as pd
sys.path.insert(0, "../lib")
from vega.apps.tables import TokenInfo, event_archive_factory, event_archive_parser

if __name__ == "__main__":

    parser = event_archive_parser()
    args = parser.parse_args()
    ea = event_archive_factory("token_transfer")
    if args.fetch_new:
        ea.fetch_new(batch_freq="5min")
    else:
        stime = pd.to_datetime(args.stime, utc=True)
        etime = pd.to_datetime(args.etime, utc=True)
        ea.fetch_range(
            stime=stime,
            etime=etime,
            batch_freq="5min")
