#######################################################
# This script is for creating a table in cassandra    #
#######################################################

from cassandra.cluster import Cluster
import config

cluster = Cluster(config.CASSANDRA_SERVER)  #config.CASSANDRA
session = cluster.connect()

#Reset database
#Delete existing keyspace
session.execute('DROP KEYSPACE IF EXISTS '+config.CASSANDRA_NAMESPACE + ';')

#Create keyspace and tables
session.execute('CREATE KEYSPACE ' + config.CASSANDRA_NAMESPACE  + ' WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 3};')
session.execute('USE ' + config.CASSANDRA_NAMESPACE)
session.execute('CREATE TABLE killerstats (time int, killerhero int, victimhero int, kills counter, PRIMARY KEY (killerhero, time, victimhero) )WITH CLUSTERING ORDER BY (time DESC);')
session.execute('CREATE TABLE victimstats (time int, killerhero int, victimhero int, kills counter, PRIMARY KEY (victimhero, time, killerhero) )WITH CLUSTERING ORDER BY (time DESC);')
session.shutdown()
