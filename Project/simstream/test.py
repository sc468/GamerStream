import smart_open
import csv

with smart_open.smart_open("s3://steve-dota2/Dota2Data/PlayerMatchData100GB.txt", encoding = 'utf8') as InputData:
    rows = csv.reader(InputData)
    for line in InputData:
        print(line)
        break
    for index, row in enumerate(rows):
        print (row)
        break
