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
session.execute('CREATE TABLE data (userid int, kills int, PRIMARY KEY (userid) );')
# WITH CLUSTERING ORDER BY (time DESC);')

