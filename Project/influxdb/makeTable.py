
from influxdb import InfluxDBClient

killer = 100
def makeEvent(gametime, killer, victim, kills):
    kill_event = \
        {
           "measurement": "killsTable",
            "tags": {
                "killer": killer,
                "victim": victim,
                "gametime" : gametime,
            },
            "fields": {
                "kills": kills,
            }
        }
    return kill_event

def inDatabase (name):
    databases = client.get_list_database()
    for db in databases:
        if db['name'] == name:
            return True
    return False


client = InfluxDBClient('localhost', 8086, 'root', 'root', 'example')

if (not inDatabase('KillsDB')):
    print ('KillsDB not in database, making database...')
    client.create_database('KillsDB')

#client.write_points([makeEvent(1,2,3,1),\
#    makeEvent(2,2,2,2), \
#    makeEvent(2,2,4,3), \
#    makeEvent(2,3,2,4), \
#    makeEvent(2,3,2,5)], database = 'KillsDB')


a = client.query("select sum(kills) from killsTable where victim = '2' group by gametime",database = 'KillsDB')
for result in a:
    print (result)

print (a.series.tags)
