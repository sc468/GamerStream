#GOAL: Limit kill stream to 5 minutes for testing purposes

#HOW TO USE IN COMMAND LINE: 
#python3 2-ExtractKillStream.py  <Input File Name, should start with 4>

########################################################
#INPUT VARIABLES
#NumPlayers = 50000
import sys

#Number of players to simulate in stream
InputFileName = sys.argv[1]

OutputFileName = '5-ShortenedTestStream.txt'

########################################################

with open(InputFileName, 'r') as InputFile,  open(OutputFileName, 'w') as OutputFile:
    for line in InputFile:	
        time = int(str(line.split(',')[0][1:]))
        if time > 300:
            break
        else:
            OutputFile.write(line)
