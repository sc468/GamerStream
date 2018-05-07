#HOW TO USE IN COMMAND LINE: 
#python3 1-CleanPlayerData.py <Number of players to simulate>

##############################################################################
#INPUT VARIABLES
##############################################################################
import sys

#Number of players to simulate in stream
#NumPlayers = 50000
NumPlayers = int(sys.argv[1])
#print (NumPlayers)

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

def RemoveChars(InputString, FilterChars):
    transtable= str.maketrans('','',FilterChars)
    InputString=InputString.translate(transtable)
    InputString=InputString.split(',')
    return (InputString)


import csv

#Extract Columns from InputFileName, output to OutputFileName
def ExtractColumns (ReadType, InputFileName, OutputFileName, columns, rowLimit):
#    with open(InputFileName, 'r') as InputData,open (OutputFileName, 'w') as OutputData:
    with smart_open.smart_open("s3://steve-dota2/Dota2Data/PlayerMatchData100GB.txt", encoding = 'utf8') as InputData,open (OutputFileName, 'w') as OutputData:
        missedPlayers = 0
        if ReadType == 'PythonIO':
            rows = InputData
        elif ReadType == 'CSV':
            rows = csv.reader(InputData)
        for index, row in enumerate(rows):
            if index>rowLimit:
                break
            if ReadType == 'PythonIO':
                row = row.split(',')
            output = ''
            
            missingEntries = False
            for c in columns:
                try:
                    nextEntry = str(RemoveChars(row[c],'"{}\\\''))
                    output = output +  ',' + nextEntry 
                    if nextEntry == "['']":
                        missedPlayers += 1
                        rowLimit += 1
                        missingEntries = True
                        break
                except IndexError:
                    pass
            if not missingEntries:
                output= output[1:] + '\n'
                #print (output)
                OutputData.write(output)
        print ('1- Cleaned File')
        print ('Output File:', OutputFileName)        
        print ('Number of players read = ', index)
        print ('Players with missing data = ', missedPlayers)
        PlayersInStream = index - missedPlayers -1
        print ('Final Number of Players in Stream = ', PlayersInStream, '\n')        


#Extract Match ID, Player ID, Kill log
OutputFileName = '2-CleanedData'+str(NumPlayers)+'.txt'
ExtractColumns('CSV','PlayerMatchData500MB.txt', OutputFileName, [0,1,3,34], NumPlayers)
