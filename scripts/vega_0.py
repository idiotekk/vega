import os
import sys
import plotly.graph_objects as go
from dateutil import tz
from dash import Dash
from dash import html, dcc, callback, Output, Input
from dash_table import DataTable
from dash.dash_table.Format import Format, Scheme, Trim
import plotly.express as px
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import pandas as pd
import numpy as np
from pprint import pprint

sys.path.insert(0, "../lib/")
from vega.dsh.db import CSVDataBase

db = CSVDataBase()
db.init(os.path.expandvars("$HOME/vega/data/"))
token_addr_list = [_.replace(".csv", "") for _ in os.listdir(f"{db.root_dir}/token/swap/")]

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.SLATE],# of course i need a dark mode
)

app.layout = html.Div([
    html.H1(
        children='Dashboard',
        style={'textAlign':'center'},
    ),
    dcc.Dropdown(
        token_addr_list,
        token_addr_list[-1],
        id='dropdown-selection',
        style={"margin": "3% 0% 3% 0%"},
    ),
    dcc.Graph(
        id='price-chart',
        responsive=True,
        style={"margin": "3% 0% 3% 0%"},
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
        style={"overflow": "scroll", "margin": "3% 0% 3% 0%"},
    )
],
style={"margin": "10% 10% 10% 10%"},
)

@callback(
    Output('price-chart', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(token_addr: str) -> go.Figure:
    """
    Plot price chart for `token_addr`.
    """
    df = db.read_table(f"token/swap/{token_addr}.csv")
    freq = "5min"
    price_var = "price_weth"
    df["timestamp_freq"] = pd.to_datetime(df["timestamp"], format='ISO8601').round(freq)
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
        title=f"{token_addr} <a href=https://etherscan.io/address/{token_addr}>etherscan</a>",
        title_y=0.95,
    )
    fig.add_hline(y=0, line_width=3, line_dash="dash", line_color="green", opacity=0.03)
    return fig


@app.callback(
    [Output("tx-table", "data"), Output('tx-table', 'columns')],
    Input('dropdown-selection', 'value')
)
def update_table(token_addr: str):
    # load
    df = db.read_table(f"token/swap/{token_addr}.csv").sort_values("timestamp", ascending=False)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601").dt.tz_convert(tz.tzlocal()).dt.strftime("%Y%m%d-%H:%M:%S")
    df["tx"] = df["transactionHash"].apply(lambda tx_hash: f"""[tx](https://etherscan.io/tx/{tx_hash})""")
    df["trader"] = df["trader"].apply(lambda addr: f"[{addr[:6]}...{addr[-4:]}](https://etherscan.io/address/{addr})")
    df["side"] = np.where(df["side"] < 0, "SELL", "BUY")
    pprint(df.iloc[-1].to_dict())
    # format
    columns = [
        {"name": "timestamp"},
        {"name": "side"},
        {"name": "price_weth", "type": "numeric", "format": Format(precision=2, scheme=Scheme.decimal_or_exponent)},
        {"name": "token_amount", "type": "numeric", "format": Format(precision=2, scheme=Scheme.decimal_or_exponent)},
        {"name": "weth_amount", "type": "numeric", "format": Format(precision=2, scheme=Scheme.decimal_or_exponent)},
        {"name": "trader", "presentation": "markdown"},
        {"name": "tx", "presentation": "markdown"},
    ]
    [_.update({"id": _["name"]}) for _ in columns]
    display_cols = [_["name"] for _ in columns]
    return [df[display_cols].to_dict("records"), columns]

if __name__ == '__main__':
    app.run(debug=True)