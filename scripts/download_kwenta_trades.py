import os
import asyncio
import pandas as pd
from kwenta import Kwenta
from datetime import datetime

# get env variables
INFURA_API_KEY = os.getenv("INFURA_API_KEY")
PROVIDER_RPC_URL = f"https://optimism-mainnet.infura.io/v3/{INFURA_API_KEY}"
WALLET_ADDRESS = "0x000741dEf5c59bEAD2B2F6be2d35fC4145e39E6b" # dummy address
PRIVATE_KEY = ""

# configure an instance of the kwenta sdk
kwenta = Kwenta(
    provider_rpc=PROVIDER_RPC_URL,  # OP mainnet or OP Goerli testnet
    wallet_address=WALLET_ADDRESS,
    private_key=PRIVATE_KEY,  # required if you want to sign transactions
    network_id=10  # 420 for OP goerli testnet
)

async def get_positions() -> pd.DataFrame:
    df = await kwenta.queries.positions()
    return df

df = asyncio.run(get_positions())
print(df.describe())
f_ = os.path.expandvars("$HOME/data/kwenta_trades.csv")
print(f_)
df.sort_values("close_timestamp").to_csv(f_, index=False)