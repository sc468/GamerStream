# -*- coding: utf-8 -*-

#heroDicionaries.py in same folder
#contains heroNames, heroPics
import heroDict

print (heroDict.Names['heroes'][0]['name']) 

import time

import dash
import dash_core_components as dcc
import dash_html_components as html
import base64
import plotly
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go

#######################################################
# This script is for reading from  a table in cassandra #
#######################################################
print ('ec2-35-155-176-164.us-west-2.compute.amazonaws.com')

from cassandra.cluster import Cluster

CASSANDRA_NAMESPACE = 'PlayerKills'

#cluster = Cluster(['54.214.213.178', '52.88.247.214', '54.190.18.13', '52.41.141.29'])  #config.CASSANDRA
cluster = Cluster(['52.11.210.69', '50.112.90.110', '54.149.158.21'])
session = cluster.connect()

session.execute('USE ' + CASSANDRA_NAMESPACE)

#######################################################
# Setup Website with Dash #
#######################################################

#streamStarted = False
#windowSize = 20
#windowStart = 0
#bufferTime = 2
#tableToDash = [['kills'], ['time']]


heroListDict = [{'label': 'All Heroes', 'value': 0}]
heroImageDict = {0:'https://cdn-images-1.medium.com/max/1191/0*vbw4wQW_Xq2_3eOo.jpg'}
heroNameDict = {0: heroListDict[0]['label']}
for hero  in heroDict.Names['heroes']:
    name = hero['localized_name']
    id = hero['id']
    heroListDict.append( {'label':hero['localized_name'], 'value': hero['id']})
    formatHeroName = hero['name']
    heroImageDict[hero['id']]=  'http://cdn.dota2.com/apps/dota2/images/heroes/' + formatHeroName + '_full.png'  
    heroNameDict [hero['id']] = hero['localized_name'] 
    

#Clear log for recording read times
with open('outputReadTime.txt', 'w') as timelog:    
    timelog.write('Read Time (s)\n')

app = dash.Dash()

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}


app = dash.Dash(__name__)
app.layout = html.Div([
#Header image and title
    html.Div([
        html.Img(src = 'https://cdn.shopify.com/s/files/1/0972/9846/products/Front_c32054d3-16f9-477b-b5de-b8ad1b681443_2048x2048.jpg?v=1523626312', 
            height =100, style = {'display': 'inline'} ),
        html.H1('GamerStream', style = {'display': 'inline'})
    ], style = {'display': 'block'}),
   
#Dropdown
    dcc.Dropdown(
        id='my-dropdown',
        options= heroListDict ,
        value=0
    ),
    html.Div(id='output-container'),

#Radio Buttons
    dcc.RadioItems(
        id='my-buttons',
        options=[
            {'label': 'Kills  ', 'value': 'killButton'},
            {'label': 'Deaths  ', 'value': 'deathButton'},
            {'label': 'Matchup  ', 'value': 'matchupButton'}
        ],
        value='killButton',
        labelStyle={'display': 'inline-block', 'font-size':20, 'margin-bottom':20, 'margin-top':20} ),

    html.Div([
        html.Img(id = 'side-hero-image', src = 'https://cdn-images-1.medium.com/max/1191/0*vbw4wQW_Xq2_3eOo.jpg',
             style = { 'width':'95%', 'height':'auto', 'display': 'inline-block', 'border-radius':'2%', 'margin-bottom':2} ),
        html.H2(id = 'side-hero-label', style = {'text-align': 'center', 'margin-top':2})
    ], style = {'float':'left', 'width': '30%'}),

    dcc.Graph(id='live-update-graph', animate = True, style = {'width':'69%','display': 'inline-block', 'float': 'left'}), 

    dcc.Interval(
        id='interval-component',
        interval=2000, # in milliseconds
        n_intervals=0
    )

])

@app.callback(
    Output('side-hero-image', 'src'),
    [Input('my-dropdown', 'value')])
def update_hero_image(value):
    return  heroImageDict[value]

@app.callback(
    Output('side-hero-label', 'children'),
    [Input('my-dropdown', 'value')])
def update_hero_label(value):
    return  heroNameDict[value]

def plotBarGraph(cassandraCommand, windowSize, newestTime):

    tableToDash = [['kills'], ['hero']]
    #Grab data from Cassandra
    print ('Cassandra Command:')
    print (cassandraCommand)
    starttime = time.time()
    result = session.execute(cassandraCommand)
    elapsedtime = time.time() - starttime
    print ('Result:', result)

    maxTime = newestTime

    dictFromCas = {}
    listFromCas = []

    # Create the graph with subplots
    fig = plotly.tools.make_subplots(rows=1, cols=1, vertical_spacing=0.2)

    for row in result:
        dictFromCas[row.victimhero] = row.kills
        listFromCas.append(row.kills)
    for i in range (1,112):
        if i in dictFromCas:
            yPoints = [0,dictFromCas[i]]
            xPoints = [i,i] 
            fig.append_trace(go.Scatter(
                x= xPoints,
                y= yPoints,
                mode = 'lines+markers'
            ), 1, 1)
        else:
            xPoints = [i]
            yPoints = [0]
            fig.append_trace(go.Scatter(
                x= xPoints,
                y= yPoints,
                mode = 'markers'
             ), 1, 1)

    print (tableToDash)

    #Find largest y value to scale graph
    try:
        maxY = max(listFromCas)
        maxYDigits = len(str(maxY))
        yAxisMax = 10**(maxYDigits )
    except:
        yAxisMax = 10

    fig['layout']['margin'] = {
        'l': 60, 'r': 60, 'b': 30, 't': 10
    }
    fig['layout']['xaxis'] = {'title':'Victim Hero', 'ticks': 'outside', 'dtick':1, 'range':[0,112]}
    fig['layout']['yaxis']= {'title':'Kill Rate (players/second)', 'range': [0,yAxisMax]}
    fig['layout']['width'] = 6000
    return fig

def plotLineGraph(cassandraCommand, windowSize, newestTime):

    tableToDash = [['kills'], ['time']]
    #Grab data from Cassandra
    print ('Cassandra Command:')
    print (cassandraCommand)
    starttime = time.time()
    result = session.execute(cassandraCommand)
    elapsedtime = time.time() - starttime
    print ('Result:', result)
    maxTime = newestTime   

    dictFromCas = {}
    listFromCas = []
    for row in result:
        dictFromCas[row.time] = row.system_sum_kills
        listFromCas.append(row.system_sum_kills)
    for i in range (maxTime-windowSize+1, maxTime+1):
        tableToDash[1].append(i)
        if i in dictFromCas:
            tableToDash[0].append(dictFromCas[i])
        else:
            tableToDash[0].append(0)
    print (tableToDash)

    #Find largest y value to scale graph
    try:
        maxY = max(listFromCas)
        maxYDigits = len(str(maxY))
        yAxisMax = 10**(maxYDigits )
    except:
        yAxisMax = 10000

    # Create the graph with subplots
    fig = plotly.tools.make_subplots(rows=1, cols=1, vertical_spacing=0.2)
    fig['layout']['margin'] = {
        'l': 60, 'r': 60, 'b': 70, 't': 10
    }
    fig['layout']['xaxis'] = {'title':'Time (seconds)', 'titlefont': {'size':22, 'color' : 'purple'}, 'range': [maxTime-windowSize+1, maxTime+1], 'ticks': 'outside', 'dtick':1}
    fig['layout']['yaxis']= {'title':'Kill Rate (players/second)', 'titlefont': {'size':22, 'color' : 'purple'}, 'range': [0,yAxisMax]}
    fig['layout']['width'] = 1000
    fig.append_trace(go.Scatter(
        x= tableToDash[1],
        y= tableToDash[0],
        name= 'Kill Rate of Hero',
        mode= 'lines+markers'
    ), 1, 1)

    return fig


# Multiple components can update everytime interval gets fired.
@app.callback(Output('live-update-graph', 'figure'),
              [ Input('interval-component', 'n_intervals') ],
            [State('my-dropdown', 'value'), State('my-buttons', 'value')])
def update_graph_live( n, heroNum,buttonState):
    #Grab max time
    cassandraCommand = 'SELECT time  FROM killerstats LIMIT 1' 
    newestTime = session.execute(cassandraCommand)
    try:
        newestTime = newestTime[0].time
    except:
        newestTime = 0

    windowSize = 20   
    cassandraCommand = ''
    if buttonState == 'killButton': 
        cassandraCommand = 'SELECT SUM(kills), time  FROM killerstats  WHERE killerhero = ' + str(heroNum) +' GROUP BY time LIMIT ' + str(windowSize)
        return plotLineGraph(cassandraCommand, windowSize, newestTime)
    elif buttonState == 'deathButton':
        cassandraCommand = 'SELECT SUM(kills), time  FROM victimstats  WHERE victimhero = ' + str(heroNum) +' GROUP BY time LIMIT ' + str(windowSize)
        return plotLineGraph(cassandraCommand, windowSize, newestTime)
    else:
        cassandraCommand = 'SELECT kills, victimhero  FROM killerstats  WHERE time = ' + str(newestTime) + ' AND killerhero = ' + str(heroNum) 
        return plotBarGraph(cassandraCommand, windowSize, newestTime)

if __name__ == '__main__':
#    app.run_server(debug=True)
    app.run_server(debug=True, host="0.0.0.0",port=80)
