import os
import sys
import plotly.graph_objects as go
from dash import Dash
from dash import html, dcc, callback, Output, Input
import plotly.express as px
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import pandas as pd

load_figure_template('DARKLY')

data_root_dir = os.path.expandvars("$HOME/vega/data/")
token_addr_list = [_.replace(".csv", "") for _ in os.listdir(f"{data_root_dir}/token/swap/")]

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],# of course i need a dark mode
)

app.layout = html.Div([
    html.H1(children='Dashboard', style={'textAlign':'center'}),
    dcc.Dropdown(token_addr_list, token_addr_list[0], id='dropdown-selection'),
    dcc.Graph(id='graph-content', responsive=True)
], style={"margin": "10% 10% 10% 10%"})

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(token_addr: str) -> go.Figure:
    """
    Plot price chart for `token_addr`.
    """
    df = pd.read_csv(f"{data_root_dir}/token/swap/{token_addr}.csv")
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
            go.Candlestick(x=df_freq["timestamp_freq"],
            open=df_freq['open'],
            high=df_freq['high'],
            low=df_freq['low'],
            close=df_freq['close'])])
    fig.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        width=1000, height=618,
        yaxis={'tickformat': 'g2'},
        xaxis_rangeslider_visible=False,
        hovermode='x unified',
    )
    fig.add_hline(y=0, line_width=3, line_dash="dash", line_color="green", opacity=0.03)
    return fig

if __name__ == '__main__':
    app.run(debug=True)