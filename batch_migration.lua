#!/usr/bin/env sysbench

require("table_migration_common")

function event()
   execute_migration()

   check_reconnect()
end
