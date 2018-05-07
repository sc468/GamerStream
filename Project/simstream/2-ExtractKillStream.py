#HOW TO USE IN COMMAND LINE: 
#python3 2-ExtractKillStream.py  <Number of players to simulate>

########################################################
#INPUT VARIABLES
#NumPlayers = 50000
import sys

#Number of players to simulate in stream
NumPlayers = int(sys.argv[1])

InputFileName = '2-CleanedData'+str(NumPlayers)+'.txt'
OutputFileName = '3-UnsortedKillStream'+str(NumPlayers)+'.txt'
########################################################

##############################################################################
#Get dictionary of hero names to id numbers
#Source: https://github.com/kronusme/dota2-api/blob/master/data/heroes.json
import json
import smart_open
hero_dict = {}

InputHeroJson = "heroes.json"

with open(InputHeroJson) as json_data:
    d = json.load(json_data)
    for hero in d['heroes']:
        hero_dict[hero['name']] = hero['id']
##############################################################################


import csv
import time
#Extract first player kill log






KillStream = ''
with open(InputFileName, 'r') as InputMatchData, open (OutputFileName, 'w') as OutputData:
    MatchCSV = csv.reader(InputMatchData, quotechar = '"', doublequote = False)
    count = 0
    for row in MatchCSV:
        count += 1
        
        #Limit computation rate to prevent Jupyter from crashing
        if count%5000 ==0:
            time.sleep(1)
            
        match_id = row[0].split('\'')[1]
        player_id = row[1].split('\'')[1]
        player_hero = row[2].split('\'')[1]
        for kill_time, kill_victim in zip(row[3::2],row[4::2]):
            kill_time = kill_time.split('\'')[1].split(':')[1]
            kill_victim = (kill_victim.split('\'')[1].split(':')[1])[14:]
            try:
                output = match_id + ', ' + player_id + ', ' + player_hero + ', ' \
                    + kill_time + ', ' + str(hero_dict[kill_victim]) + '\n'
                OutputData.write(output)
            except KeyError:
                pass
                #output = 'Error with victim:' + kill_victim + ', not a DOTA2 Hero'
            KillStream = KillStream + output
print ('2- Created KillStream ' , OutputFileName)
#print (KillStream)
