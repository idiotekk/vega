import os
import sys
import pandas as pd
sys.path.insert(0, "../lib")
from vega.apps.tables import TokenInfo, event_archive_factory, event_archive_parser

if __name__ == "__main__":

    parser = event_archive_parser()
    parser.add_argument(
        "--name",
        type=str,
        choices=[
            "token_transfer",
            "weth_deposit",
            "weth_withdrawal",
            "uniswap_v2_swap",
        ],
    )
    parser.add_argument(
        "--batch-freq",
        type=str,
        choices=[
            "5min",
            "30min",
            "1h",
        ],
        default="1h",
    )

    args = parser.parse_args()
    ea = event_archive_factory(args.name)
    batch_freq = args.batch_freq

    if args.fetch_new:
        ea.fetch_new(batch_freq=batch_freq)
    else:
        stime = pd.to_datetime(args.stime, utc=True)
        etime = pd.to_datetime(args.etime, utc=True)
        ea.fetch_range(
            stime=stime,
            etime=etime,
            batch_freq=batch_freq)
