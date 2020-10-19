#!/usr/bin/env sysbench

require("table_migration_common")

function event()
   execute_select_pk_during_migration()
   execute_select_index_during_migration()
   execute_insert_during_migration()
   execute_delete_during_migration()
   execute_update_pk_during_migration()
   execute_update_index_during_migration()

   check_reconnect()
end
