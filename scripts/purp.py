import os
import sys
import asyncio
import plotly.graph_objects as go
from dateutil import tz
from functools import lru_cache
from dash import Dash
from dash import html, dcc, callback, Output, Input
from dash_table import DataTable, FormatTemplate
from dash.dash_table.Format import Format, Scheme, Trim
import plotly.express as px
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import pandas as pd
from kwenta import Kwenta
import numpy as np
from pprint import pprint

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.SLATE],# of course i need a dark mode
)

# get env variables
INFURA_API_KEY = os.getenv("INFURA_API_KEY")
PROVIDER_RPC_URL = f"https://optimism-mainnet.infura.io/v3/{INFURA_API_KEY}"
WALLET_ADDRESS = "0x000741dEf5c59bEAD2B2F6be2d35fC4145e39E6b"
PRIVATE_KEY = ""

# configure an instance of the kwenta sdk
kwenta = Kwenta(
    provider_rpc=PROVIDER_RPC_URL,  # OP mainnet or OP Goerli testnet
    wallet_address=WALLET_ADDRESS,
    private_key=PRIVATE_KEY,  # required if you want to sign transactions
    network_id=10  # 420 for OP goerli testnet
)

app.layout = html.Div([
    html.H1(
        children=" ".join(['PURP DASHBOARD']),
        style={'textAlign':'center'},
    ),
    dcc.Input(
        id='input-token-addr',
        style={"margin": "3% 0% 3% 0%", "width": "100%"},
        placeholder="input trader address",
    ),
    dcc.Graph(
        id='pnl-chart',
        responsive=True,
        style={"margin": "3% 0% 3% 0%"},
    ),
    html.Div(
        DataTable(
            id='tx-table',
            data=[],
            page_size=10,
            sort_action="native",
            style_cell={
                'textAlign': 'left'
            },
            style_header={
                'backgroundColor': 'rgb(30, 30, 30)',
                'color': 'white'
            },
            style_data={
                'backgroundColor': 'rgb(50, 50, 50)',
                'color': 'white'
            },
        ),
        style={"overflow": "scroll", "margin": "3% 0% 3% 0%"},
    )
],
style={"margin": "10% 20% 10% 20%"},
)


@lru_cache(maxsize=128)
def get_positions_for_account(account):
    df = asyncio.run(kwenta.queries.positions_for_account(account=account))
    df["time"] = pd.to_datetime(df["timestamp"], unit="s")
    df["open_time"] = pd.to_datetime(df["open_timestamp"], unit="s", utc=True)
    df["close_time"] = pd.to_datetime(df["close_timestamp"], unit="s", utc=True)
    df["duration"] = (df["close_time"] - df["open_time"]).astype(str)
    df["asset"] = df["market_key"].apply(lambda x: x[1:-4])
    df = df.sort_values("timestamp")
    return df


@callback(
    Output('pnl-chart', 'figure'),
    Input('input-token-addr', 'value')
)
def update_graph(addr: str) -> go.Figure:
    """
    Plot price chart for `addr`.
    """
    df = get_positions_for_account(account=addr)
    df["pnl"] = df["pnl_with_fees_paid"].cumsum()
    
    pnl = df[["time", "pnl"]].copy()
    min_time = pnl["time"].iloc[0]
    pnl = pd.concat([
        pd.DataFrame({"time": [min_time - pd.Timedelta("1d")], "pnl": 0, }),
        pnl,
        pd.DataFrame({"time": [pd.Timestamp.utcnow()], "pnl": pnl["pnl"].iloc[-1],})
    ])

    fig = px.line(
        pnl,
        x="time",
        line_shape="vh",
        y="pnl")
    fig.add_hline(
        y=0, opacity=0.5,
        line_dash="dash",
        )
    fig.update_layout(
        template="plotly_dark",
    )
    return fig


@app.callback(
    [Output("tx-table", "data"), Output('tx-table', 'columns')],
    Input('input-token-addr', 'value')
)
def update_table(addr: str):
    
    df = get_positions_for_account(account=addr)
    df = df.sort_values("timestamp", ascending=False)
    pprint(df.iloc[-1].to_dict())
    # format
    columns = [
        {"name": "time"},
        {"name": "pnl_with_fees_paid", "type": "numeric", "format": FormatTemplate.money(2)},
        {"name": "asset"},
        {"name": "duration"},
    ]
    [_.update({"id": _["name"]}) for _ in columns]
    display_cols = [_["name"] for _ in columns]
    return [df[display_cols].to_dict("records"), columns]

if __name__ == '__main__':
    app.run(debug=True)