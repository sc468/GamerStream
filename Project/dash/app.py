# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly
from dash.dependencies import Input, Output

#######################################################
# This script is for reading from  a table in cassandra #
#######################################################
print ('CHECKKKKKKKKK!!!!!!!')

from cassandra.cluster import Cluster
#from cassandra-driver import Cluster
#import config

CASSANDRA_NAMESPACE = 'PlayerKills'

cluster = Cluster(['54.214.213.178', '52.88.247.214', '54.190.18.13', '52.41.141.29'])  #config.CASSANDRA
session = cluster.connect()

session.execute('USE ' + CASSANDRA_NAMESPACE)

#######################################################
# Setup Website with Dash #
#######################################################

app = dash.Dash()

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

app = dash.Dash(__name__)
app.layout = html.Div(
    html.Div([
        html.H4('PlayerStream'),
        dcc.Graph(id='live-update-graph'),
        dcc.Interval(
            id='interval-component',
            interval=2*1000, # in milliseconds
            n_intervals=0
        )
    ])
)

# Multiple components can update everytime interval gets fired.
@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):

    maxTimeInTable = 0
    tableToDash = [['kills'], ['time']]

    
    #Grab data from Cassandra
    result = session.execute('SELECT kills, time  FROM data WHERE hero = 14'  )
    for row in result:
        tableToDash[0].append(row.kills)
        tableToDash[1].append(row.time)
    #print (dictFromCas)
    #Create table to send to plot in Dash
    #Fill in unknown values
#    tableToDash = [['kills'], ['time']]
#    for i in range (0, maxTimeInTable):
#        tableToDash[1].append(i)
#        if i in dictFromCas:
#            tableToDash[0].append(dictFromCas[i])


    # Create the graph with subplots
    fig = plotly.tools.make_subplots(rows=1, cols=1, vertical_spacing=0.2)
    fig['layout']['margin'] = {
        'l': 60, 'r': 60, 'b': 30, 't': 10
    }
    fig['layout']['legend'] = {'x': 0, 'y': 1, 'xanchor': 'left'}
    fig['layout']['xaxis'] = {'title':'Time (seconds)'}
    fig['layout']['yaxis']= {'title':'Kill Rate (players/second)'}
    fig.append_trace({
        'x': tableToDash[1],
        'y': tableToDash[0],
        'name': 'Kill Rate of Hero',
        'type': 'bar'
    }, 1, 1)

    return fig

if __name__ == '__main__':
#    app.run_server(debug=True)
    app.run_server(debug=True, host="0.0.0.0",port=80)
