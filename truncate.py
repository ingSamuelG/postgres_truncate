import re
import delegator
from PostSqlConnect import PostSql
import os

POSG_HOST = "localhost"
POSG_DB = "testBest"
POSG_USER = "postgres"
POSG_PASS = "1234"
POSG_END = "UTF8"
POSG_DROP_TABLES = ['twitter_users', 'tweets', "twitter_master_users", "twitter_request_users", "twitter_requests", "inappropriate_videos", "video_browse_categories", "video_categories", "video_favs", "video_tags", "video_votes", "videos", "schema_migrations"]
POSG_TRUNCATE_TO_0_TABLES = ['dmcas']
POSG_TRUN_SIZE = 10


Postql = PostSql.PostSqlConnect(POSG_HOST, POSG_USER, POSG_PASS,POSG_DB, POSG_END)
table_names = [results[0] for results in Postql.get_table_names()]

child_parents_struct = {}
processed_table = []
for table in table_names:
        if table not in POSG_DROP_TABLES and  table not in processed_table:
                id = table.strip('s') + "_id"
                childs  = Postql.get_child_tables(id)
                if len(childs)>0:
                        for ch in childs:
                                if ch not in POSG_DROP_TABLES:
                                        if child_parents_struct.get(ch):
                                                if len(child_parents_struct.get(ch)) > 0: 
                                                        child_parents_struct[ch] = child_parents_struct.get(ch) + [table]
                                                else: 
                                                        child_parents_struct[ch] = [table]
                                        else:
                                                child_parents_struct[ch] = [table]
                else:
                        child_parents_struct[table] = None

tables_added = []
parent_created = []
ids_seeded = {}

for table_child_key, parents in child_parents_struct.items():
        table_child_id = table_child_key.strip('s') + "_id"
        if parents != None:
                query_parent_id = []
                for parent in parents:
                        parent_id = parent.strip('s') + "_id"
                        if parent not in parent_created:
                                Postql.drop_existing_table('nt_{}'.format(parent))
                                Postql.copy_tables_from_main(parent)
                                parent_created.append(parent)
                                print("Cloning table {}".format(parent))
                        
                        if not ids_seeded.get(parent):
                                source_table_rows = Postql.get_base_rows(parent, POSG_TRUN_SIZE)
                                if parent not in POSG_TRUNCATE_TO_0_TABLES:
                                        Postql.insert_from_old(source_table_rows, 'nt_{}'.format(parent))
                                        tables_added.append('nt_{}'.format(parent))
                                        ids_string = ",".join([str(row[0]) for row in source_table_rows])
                                        ids_seeded[parent] =  ids_string

                        query_parent_id.append("({} in ({}))".format(parent_id, ids_seeded[parent]))


                full_query = " and ".join(query_parent_id)

                if ids_seeded.get(table_child_key):
                        Postql.delete_not_in_the_parent(table_child_key, full_query)
                else:
                        Postql.drop_existing_table('nt_{}'.format(table_child_key))
                        Postql.copy_tables_from_main(table_child_key)
                        print("Cloning table {}".format(table_child_key))
                        if table_child_key not in POSG_TRUNCATE_TO_0_TABLES:
                                Postql.insert_childs_dependencies_rows_for_temps_v2(table_child_key, full_query)
                                tables_added.append('nt_{}'.format(table_child_key))
        else:
                Postql.drop_existing_table('nt_{}'.format(table_child_key))
                Postql.copy_tables_from_main(table_child_key)
                print("Cloning table {}".format(table_child_key))
                if table_child_key not in POSG_TRUNCATE_TO_0_TABLES:
                        source_table_rows = Postql.get_base_rows(table_child_key, POSG_TRUN_SIZE)
                        Postql.insert_from_old(source_table_rows, 'nt_{}'.format(table_child_key))
                        tables_added.append('nt_{}'.format(table_child_key))

nt_table_names = [results[0] for results in Postql.get_table_names() if re.search('^nt_*', results[0])]

os.environ["PGPASSWORD"] = POSG_PASS
# command = 'pg_dump -U {} -w  -t "^nt_*" -f ./dev_x3.sql {}'.format(POSG_USER, POSG_DB)
command = 'pg_dump -U {} -h {} -w  -t "^nt_*" {} > ./dev_x3.sql '.format(POSG_USER, POSG_HOST ,POSG_DB)

c = delegator.run(command)


for table_name in nt_table_names:
        Postql.drop_existing_table(table_name)


Postql.commit_all_transactions()

Postql.close_conections()
Postql.close_conections()