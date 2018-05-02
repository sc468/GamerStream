#######################################################
# This script is for creating a table in rethinkDB    #
#######################################################

import rethinkdb as r
import config

conn = r.connect(host=config.RETHINKDB_SERVER, \
                 port=28015, \
                   db=config.RETHINKDB_DB)

try:
    r.db(config.RETHINKDB_DB)\
     .table_drop(config.RETHINKDB_TABLE)\
     .run(conn)

except:
    pass


r.db(config.RETHINKDB_DB)\
 .table_create(config.RETHINKDB_TABLE, primary_key=config.RETHINKDB_PRIMARYKEY)\
 .run(conn)


