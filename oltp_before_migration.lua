#!/usr/bin/env sysbench

require("table_migration_common")

function event()
   execute_select_pk_before_migration()
   execute_select_index_before_migration()
   execute_insert_before_migration()
   execute_delete_before_migration()
   execute_update_pk_before_migration()
   execute_update_index_before_migration()

   check_reconnect()
end
