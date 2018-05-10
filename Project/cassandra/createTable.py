#######################################################
# This script is for creating a table in cassandra    #
#######################################################

from cassandra.cluster import Cluster
#from cassandra-driver import Cluster
import config

cluster = Cluster(config.CASSANDRA_SERVER)  #config.CASSANDRA
session = cluster.connect()

#Reset database
#Delete existing keyspace
session.execute('DROP KEYSPACE IF EXISTS '+config.CASSANDRA_NAMESPACE + ';')

session.execute('CREATE KEYSPACE ' + config.CASSANDRA_NAMESPACE  + ' WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 3};')
session.execute('USE ' + config.CASSANDRA_NAMESPACE)
session.execute('CREATE TABLE killerstats (time int, killerhero int, victimhero int, kills counter, PRIMARY KEY (killerhero, time, victimhero) )WITH CLUSTERING ORDER BY (time DESC);')
session.execute('CREATE TABLE victimstats (time int, killerhero int, victimhero int, kills counter, PRIMARY KEY (victimhero, time, killerhero) )WITH CLUSTERING ORDER BY (time DESC);')
#session.execute('CREATE TABLE killstats (time int, killerhero int, kills counter, PRIMARY KEY (killerhero, time) )WITH CLUSTERING ORDER BY (time DESC);')
#session.execute('CREATE TABLE deathstats (time int, victimhero int, kills counter, PRIMARY KEY (victimhero, time) )WITH CLUSTERING ORDER BY (time DESC);')
#session.execute('CREATE TABLE killerstats (time int, killerhero int, victimhero int, kills int, PRIMARY KEY (killerhero, time, kills) )WITH CLUSTERING ORDER BY (time DESC, kills DESC);')
#session.execute('CREATE TABLE data (time int, hero int, kills int, PRIMARY KEY (hero, time) )WITH CLUSTERING ORDER BY (time DESC);')

