import os
import sys
import plotly.graph_objects as go
from dateutil import tz
from dash import Dash
from dash import html, dcc, callback, Output, Input
from dash_table import DataTable
from dash.dash_table.Format import Format, Scheme
import plotly.express as px
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import pandas as pd
import numpy as np
from typing import Tuple
from pprint import pprint
import cachetools.func

sys.path.insert(0, "../lib/")
from vega.apps.tables import event_archive_factory, TokenInfo
from vega.evm.web3 import ERC20TokenTracker

p = ERC20TokenTracker()
p.init()
ti = TokenInfo()
swaps = event_archive_factory("uniswap_v2_swap")
place_holder = "0xE0f63A424a4439cBE457D80E4f4b51aD25b2c56C"



app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.SLATE],# of course i need a dark mode
)

app.layout = html.Div([
    html.H1(
        children='Token Dashboard',
        style={'textAlign':'center'},
    ),

    html.Div(children=[

        html.Div(
            children=[
            dcc.Input(
                id='input-addr',
                style={
                    "width": "100%",
                    "margin": "3% 0% 3% 0%"
                },
                placeholder="input token address",
            ),
            DataTable(
                id='info-table',
                data=[],
                page_size=100,
                sort_action="native",
                style_header={
                    'backgroundColor': 'rgb(30, 30, 30)',
                    'color': 'white'
                },
                style_data={
                    'backgroundColor': 'rgb(50, 50, 50)',
                    'color': 'white'
                },
            ),
            ],
            style={"overflow": "scroll", "margin": "3% 3% 3% 3%"},
        )
    ], style={
        "display": "inline-block",
        "width": "30%",
        "verticalAlign": "top",
    }),

    html.Div(children=[
    dcc.Graph(
        id='price-chart',
        responsive=True,
        style={"margin": "3% 3% 3% 3%"},
    ),
    html.Div(
        DataTable(
            id='tx-table',
            data=[],
            page_size=10,
            sort_action="native",
            style_header={
                'backgroundColor': 'rgb(30, 30, 30)',
                'color': 'white'
            },
            style_data={
                'backgroundColor': 'rgb(50, 50, 50)',
                'color': 'white'
            },
        ),
        style={"overflow": "scroll", "margin": "3% 3% 3% 3%"},
    )
    ], style={
        "display": "inline-block",
        "width": "60%"
    }),
],
style={"margin": "10% 10% 10% 10%"},
)

@callback(
    Output('price-chart', 'figure'),
    Input('input-addr', 'value')
)
def update_graph(token_addr: str) -> go.Figure:
    """
    Plot price chart for `token_addr`.
    """
    if token_addr is None: token_addr = place_holder
    token_addr = p.web3.to_checksum_address(token_addr)
    df, info = load_token_data(token_addr=token_addr)
    freq = "5min"
    price_var = "price"
    df["timestamp_freq"] = df["timestamp_"].round(freq)
    df_freq = df.groupby("timestamp_freq").apply(
        lambda x: pd.Series({
            "open": x[price_var].iloc[0],
            "close": x[price_var].iloc[-1],
            "high": x[price_var].max(),
            "low": x[price_var].min(),
        })).reset_index()
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df_freq["timestamp_freq"],
                open=df_freq['open'],
                high=df_freq['high'],
                low=df_freq['low'],
                close=df_freq['close'],
            )],
    )
    fig.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        width=1000, height=618,
        yaxis={'tickformat': 'g2'},
        xaxis_rangeslider_visible=False,
        hovermode='x unified',
        template="plotly_dark",
        title=f"{token_addr} | {info['symbol']}",
        title_y=0.95,
    )
    fig.add_hline(y=0, line_width=3, line_dash="dash", line_color="green", opacity=0.03)
    return fig


@app.callback(
    [Output("tx-table", "data"), Output('tx-table', 'columns')],
    Input('input-addr', 'value')
)
def update_swap_table(token_addr: str):
    # load
    if token_addr is None: token_addr = place_holder
    token_addr = p.web3.to_checksum_address(token_addr)
    df, _ = load_token_data(token_addr=token_addr)
    # format
    columns = [
        {"name": "tx", "presentation": "markdown"},
        {"name": "price", "type": "numeric", "format": Format(precision=2, scheme=Scheme.decimal_or_exponent)},
        {"name": "timestamp"},
    ]
    [_.update({"id": _["name"]}) for _ in columns]
    display_cols = [_["name"] for _ in columns]
    return [df[display_cols].to_dict("records"), columns]


@app.callback(
    [Output("info-table", "data"), Output('info-table', 'columns')],
    Input('input-addr', 'value')
)
def update_info_table(token_addr: str):
    # load
    if token_addr is None: token_addr = place_holder
    token_addr = p.web3.to_checksum_address(token_addr)
    _, df = load_token_data(token_addr=token_addr)
    df["dexscreener"] = f"https://dexscreener.com/ethereum/{token_addr}"
    df["dextools"] = f"https://www.dextools.io/app/en/ether/pair-explorer/{token_addr}"
    df = pd.Series(df).astype(str)
    df = pd.DataFrame({
        "field": df.index,
        "value": df.values,
    })
    def to_link(x):
        if x.startswith("0x"):
            return f"[{x[:6]}...{x[-4:]}](https://etherscan.io/address/{x})"
        elif x.startswith("http"):
            return f"[link]({x})"
        else:
            return x

    df["value"] = df["value"].apply(to_link)
    # format
    columns = [
        {"name": "field", "id": "field"},
        {"name": "value", "id": "value", "presentation": "markdown"},
    ]
    return [df.to_dict("records"), columns]


@cachetools.func.ttl_cache(maxsize=128, ttl= 60)
def load_token_data(token_addr: str) -> Tuple[pd.DataFrame, dict]:
    ti.touch(token_addr)
    info = ti.get_token_info(token_addr)
    pool = p.get_univswap_v2_pair(token_addr)
    df = swaps.db.read_sql(f"SELECT * from {swaps.table_name} where address = '{pool}'")
    df["timestamp_"] = p.get_timestamp_from_block_number(df["blockNumber"])
    df["timestamp"] = df["timestamp_"].dt.tz_convert(tz.tzlocal()).dt.strftime("%Y%m%d-%H:%M:%S")
    df["tx"] = df["transactionHash"].apply(lambda tx_hash: f"""[tx](https://etherscan.io/tx/{tx_hash})""")
    df["price"] = -df["amount0"].apply(lambda x: int(x))  / df["amount1"].apply(lambda x: int(x))
    return df, info


if __name__ == '__main__':
    app.run(debug=True)