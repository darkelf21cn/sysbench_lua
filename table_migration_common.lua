--[[
This benchmark simulates a live table migration. Data are supposed
to be migrated by many small batches with many commits. To ensure
the data consistency, the SELECT, UPDATE, and DELETE statement is
modified slightly. When the data that you are going to process has
migrated to the new table, it will directly work on the new dataset.
Otherwise, it will fall back to the old table for fetching the data,
move it to the new table then process it. So the entire migration
will be lockless. But it does have overheads to the database. This
benchmark is to test how the performance impact to the OLTP service
during the migration.

INSERT - insert into the new table.
DELETE - delete from both old and new tables.
UPDATE - if the row exists in the new table, do update against the 
         new table; otherwise, fetch data from the old table and 
         insert into the new table then do the update.
SELECT - select from the new table first and fall back to the old
         table when the row doesn't exist.
]]

function init()
   assert(event ~= nil,
      "this script is meant to be included by other scripts and " ..
         "should not be called directly.")
end

-- Command line options
sysbench.cmdline.options = {
   table_size =
      {"Number of rows", 1000000},
   dup_rate =
      {"The rate of duplicate value of val column", 10},
   migrate_batch_size = 
      {"The batch size of each data movement", 1000},
   reconnect =
      {"Reconnect after every N events. The default (0) is to not reconnect", 0},
}

sysbench.cmdline.commands = {
   migrate = {cmd_migrate},
}

function cmd_migrate()
   -- callback
   migrate()
end

-- SQL commands that directly process against the old table
local stmt_before_migration = {
   select_pk = "SELECT * FROM tbl_old WHERE id = %d",
   select_index = "SELECT * FROM tbl_old WHERE val = %d",
   insert = "INSERT INTO tbl_old (val, write_counter, generated_at) VALUES (%d, 1, 'N')",
   delete = "DELETE FROM tbl_old WHERE id = %d",
   update_pk = "UPDATE tbl_old SET val = %d, write_counter = write_counter + 1 WHERE id = %d",
   update_index = "UPDATE tbl_old SET val = %d, write_counter = write_counter + 1 WHERE val = %d",
}

-- SQL commands that contains a fall back logical
local stmt_with_fallback = {
   start = "START TRANSACTION",
   commit = "COMMIT",
   select_pk = {[[
SELECT * FROM tbl_new WHERE id = %d
UNION ALL
SELECT tbl_old.*
FROM tbl_old LEFT JOIN tbl_new ON tbl_old.id = tbl_new.id 
WHERE tbl_old.id = %d AND tbl_new.id IS NULL]],
   },
   select_index = {[[
SELECT * FROM tbl_new WHERE val = %d
UNION ALL
SELECT tbl_old.* 
FROM tbl_old LEFT JOIN tbl_new ON tbl_old.id = tbl_new.id
WHERE tbl_old.val = %d AND tbl_new.id IS NULL]],
   },
   insert = {
"INSERT INTO tbl_new (val, write_counter, generated_at) VALUES (%d, 1, 'N')",
   },
   delete = {
"DELETE FROM tbl_old WHERE id = %d",
"DELETE FROM tbl_new WHERE id = %d",
   },
   update_pk = {
"INSERT INTO tbl_new SELECT tbl_old.* FROM tbl_old LEFT JOIN tbl_new ON tbl_old.id = tbl_new.id WHERE tbl_old.id = %d AND tbl_new.id IS NULL",
"UPDATE tbl_new SET val = %d, write_counter = write_counter + 1 WHERE id = %d",
   },
   update_index = {
"INSERT INTO tbl_new SELECT tbl_old.* FROM tbl_old LEFT JOIN tbl_new ON tbl_old.id = tbl_new.id WHERE tbl_old.val = %d AND tbl_new.id IS NULL",
"UPDATE tbl_new SET val = %d, write_counter = write_counter + 1 WHERE val = %d",
   },
}

local stmt_migrate_data = [[
INSERT INTO tbl_new
SELECT tbl_old.* FROM tbl_old LEFT JOIN tbl_new ON tbl_old.id = tbl_new.id
WHERE tbl_old.id >= %d AND tbl_old.id < %d AND tbl_new.id IS NULL]]

function prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   print("Creating tables")
   local query
   query = [[
CREATE TABLE tbl_old (
	id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
   val INT NULL,
   write_counter INT UNSIGNED NULL,
   generated_at CHAR(1),
   KEY ix_val(val)
)]]
   con:query(query)

   query = [[
CREATE TABLE tbl_new (
	id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
   val INT NULL,
   write_counter INT UNSIGNED NULL,
   generated_at CHAR(1),
   KEY ix_val(val)
)]]
   con:query(query)

   if (sysbench.opt.table_size > 0) then
      print(string.format("Inserting %d records into 'tbl_old'", sysbench.opt.table_size))
   end
   query = "INSERT INTO tbl_old (id, val, write_counter, generated_at) VALUES"
   con:bulk_insert_init(query)
   for i = 1, sysbench.opt.table_size do
      query = string.format("(%d, %d, 1, 'O')", i, sysbench.rand.default(1, sysbench.opt.table_size / 10))
      con:bulk_insert_next(query)
   end
   con:bulk_insert_done()
end

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   print("Dropping table 'tbl_old'...")
   con:query("DROP TABLE IF EXISTS tbl_old")
   print("Dropping table 'tbl_new'...")
   con:query("DROP TABLE IF EXISTS tbl_new")
end

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()

   stmt = {}
   param = {}
end

function thread_done()
   con:disconnect()
end

local function get_id()
   return sysbench.rand.default(1, sysbench.opt.table_size)
end

local function get_val()
   return sysbench.rand.default(1, sysbench.opt.table_size / sysbench.opt.dup_rate)
end

function execute_select_pk_before_migration()
   con:query(string.format(stmt_before_migration.select_pk, get_id()))
end

function execute_select_pk_during_migration()
   local id = get_id()
   con:query(string.format(stmt_with_fallback.select_pk[1], id, id))
end

function execute_select_index_before_migration()
   con:query(string.format(stmt_before_migration.select_index, get_val()))
end

function execute_select_index_during_migration()
   local val = get_val()
   con:query(string.format(stmt_with_fallback.select_index[1], val, val))
end

function execute_insert_before_migration()
   con:query(string.format(stmt_before_migration.insert, get_val()))
end

function execute_insert_during_migration()
   con:query(string.format(stmt_with_fallback.insert[1], get_val()))
end

function execute_delete_before_migration()
   con:query(string.format(stmt_before_migration.delete, get_id()))
end

function execute_delete_during_migration()
   local id = get_id()
   con:query(stmt_with_fallback.start)
   con:query(string.format(stmt_with_fallback.delete[1], id))
   con:query(string.format(stmt_with_fallback.delete[2], id))
   con:query(stmt_with_fallback.commit)
end

function execute_update_pk_before_migration()
   con:query(string.format(stmt_before_migration.update_pk, get_val(), get_id()))
end

function execute_update_pk_during_migration()
   local id = get_id()
   con:query(stmt_with_fallback.start)
   con:query(string.format(stmt_with_fallback.update_pk[1], id))
   con:query(string.format(stmt_with_fallback.update_pk[1], get_val(), id))
   con:query(stmt_with_fallback.commit)
end

function execute_update_index_before_migration()
   con:query(string.format(stmt_before_migration.update_index, get_val(), get_val()))
end

function execute_update_index_during_migration()
   local val = get_val()
   con:query(stmt_with_fallback.start)
   con:query(string.format(stmt_with_fallback.update_pk[1], val))
   con:query(string.format(stmt_with_fallback.update_pk[1], val, val))
   con:query(stmt_with_fallback.commit)
end

function execute_migration()
   for i = 0, sysbench.opt.table_size, sysbench.opt.migrate_batch_size do
      con:query(string.format(stmt_migrate_data, i, i + sysbench.opt.migrate_batch_size))
   end
end

-- Re-prepare statements if we have reconnected, which is possible when some of
-- the listed error codes are in the --mysql-ignore-errors list
function sysbench.hooks.before_restart_event(errdesc)
   if errdesc.sql_errno == 2013 or -- CR_SERVER_LOST
      errdesc.sql_errno == 2055 or -- CR_SERVER_LOST_EXTENDED
      errdesc.sql_errno == 2006 or -- CR_SERVER_GONE_ERROR
      errdesc.sql_errno == 2011    -- CR_TCP_CONNECTION
   then
      close_statements()
   end
end

function check_reconnect()
   if sysbench.opt.reconnect > 0 then
      transactions = (transactions or 0) + 1
      if transactions % sysbench.opt.reconnect == 0 then
         close_statements()
         con:reconnect()
      end
   end
end