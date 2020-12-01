--[[
   This is about to test the database performance under a highly flexible
   table structure. The table name and column name are no longer given a
   meaningful name. Instead, they follow a common naming rule, and columns
   in each table will have the same data type. We define schema definition
   somewhere else to explain the data. We only merge those data from many
   tables when they are needed.

   usage:
   1. prepare a configuration file and put shared parameters there.
      cat > ./sysbench.conf <<EOL
      mysql-host=localhost
      mysql-port=8022
      mysql-user=msandbox
      mysql-password=msandbox
      EOL

   2. run a cleanup incase you left something in the database
      sysbench ./data_frame_common.lua --config-file=sysbench.cnf --tables=5 cleanup

   3. run the prepare command to setup tables and insert data
      sysbench ./data_frame_common.lua --config-file=sysbench.cnf --tables=5 --threads=16 --rand-type=uniform prepare

   4. run the benchmark!
      sysbench ./data_frame_common.lua --config-file=sysbench.cnf --tables=5 --threads=32 --rand-type=uniform --bc_pk_seeks=4 --bc-updates=1 --bc-updates=1 run
]]

-- Command line options
sysbench.cmdline.options = {
   table_size =
      {"Number of rows", 10000},
   columns = 
      {"Number of columns in the table", 5},
   column_types =
      {"Number of data types to be tested. Split by comma {varchar, int, bigint, float, double, date, datetime, text, longtext, json}", {"varchar", "int", "bigint", "float", "double", "date", "datetime", "text", "longtext", "json"}},
   tables =
      {"Tables to be created for each column_type", 4},
   varchar_length =
      {"Length of varchar column", 128},
   max_range_interval =
      {"The max interval for doing a range scan", 100},
   bc_pk_seeks = 
      {"Benchmark: pk_seek per transaction", 0},
   bc_pk_ranges = 
      {"Benchmark: pk_range per transaction", 0},
   bc_index_seeks =
      {"Benchmark: index_seek per transaction", 0},
   bc_index_ranges =
      {"Benchmark: index_range per transaction", 0},
   bc_inserts =
      {"Benchmark: inserts per transaction", 0},
   bc_deletes =
      {"Benchmark: deletes per transaction", 0},
   bc_updates =
      {"Benchmark: updates per transaction", 0},
   reconnect =
      {"Reconnect after every N events. The default (0) is to not reconnect", 0},
}

local function get_table_name(column_type, table_id)
   return string.format("df_%s_x%d_%d", column_type, sysbench.opt.columns, table_id)
end

local function get_column_name(column_id)
   return string.format("c_%d", column_id)
end

local function get_rand_id(column_type, table_id)
   return sysbench.rand.default(1, max_table_rows[column_type .. table_id])
end

local function get_component_id(uid)
   return uid % 100
end

local function get_rand_date()
   local t = os.time() - sysbench.rand.default(0, sysbench.opt.table_size)
   return "'" .. os.date("%Y-%m-%d %H:%M:%S", t) .. "'"
end

local function get_rand_varchar()
   return "'" .. sysbench.rand.string(string.rep('@', sysbench.rand.default(1, sysbench.opt.varchar_length))) .. "'"
end

local function get_rand_int()
   return sysbench.rand.default(0, sysbench.opt.table_size)
end

local function get_rand_float()
   return sysbench.rand.default(0, sysbench.opt.table_size) / 1000
end

local function get_rand_json()
   local fmt = "'{\"@@@@@@@@@@\": \"@@@@@@@@@@\"}'"
   return sysbench.rand.string(fmt)
end

local rand_func_mapping = {
   varchar = get_rand_varchar,
   int = get_rand_int,
   bigint = get_rand_int,
   float = get_rand_float,
   double = get_rand_float,
   text = get_rand_varchar,
   longtext = get_rand_varchar,
   date = get_rand_date,
   datetime = get_rand_date,
   json = get_rand_json
}

local function get_rand(column_type)
   return rand_func_mapping[column_type]()
end

local function get_rand_table_id()
   return sysbench.rand.uniform(1, sysbench.opt.tables)
end

local function get_rand_column_id()
   return sysbench.rand.uniform(1, sysbench.opt.columns)
end

local function get_rand_column_type()
   local i = sysbench.rand.default(1,#sysbench.opt.column_types)
   return sysbench.opt.column_types[i]
end

local function get_stmt_key(column_type, table_id, column_id)
   if column_id == 0 then
      return string.format("%s__%s", "updates", get_table_name(column_type, table_id))
   else
      return string.format("%s__%s__%s", "updates", get_table_name(column_type, table_id), get_column_name(column_id))
   end
end

function init()
   --[[
      assert(event ~= nil,
          "this script is meant to be included by other OLTP scripts and " ..
             "should not be called directly.")
   ]]
end

if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: prepare, warmup, run, " ..
            "cleanup, help")
end

-- Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --tables > 1
function cmd_prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for table_id = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables, sysbench.opt.threads do
      for j, column_type in ipairs(sysbench.opt.column_types) do
         column_type = string.lower(column_type)
         create_table(con, table_id, column_type, sysbench.opt.columns)
         batch_insert(con, table_id, column_type, sysbench.opt.columns, sysbench.opt.table_size)
      end
   end
end

-- Implement parallel prepare and warmup commands, define 'prewarm' as an alias
-- for 'warmup'
sysbench.cmdline.commands = {
   prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND}
}

function create_table(con, table_id, column_type, columns)
   print(string.format("Creating table '%s'...", get_table_name(column_type, table_id)))

   local sql_template = [[
CREATE TABLE %s (
   component_id BIGINT UNSIGNED NOT NULL,
   id BIGINT UNSIGNED NOT NULL,
   version INT NOT NULL DEFAULT 0,%s
   insert_dt DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
   update_dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (component_id, id),%s
   KEY insert_dt (insert_dt),
   KEY update_dt (update_dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;]]
   
   local create_col_tmpl = "\n   %s %s DEFAULT NULL,"
   local create_idx_tmpl = "\n   KEY (c_%d),"
   if column_type == "varchar" then
      create_col_tmpl = string.format(create_col_tmpl, "c_%d", "VARCHAR(" .. sysbench.opt.varchar_length .. ")")
   else
      create_col_tmpl = string.format(create_col_tmpl, "c_%d", string.upper(column_type))
   end

   if column_type == "text" or
      column_type == "longtext" or 
      column_type == "json" then
         create_idx_tmpl = ""
   end
     
   local create_col = ""
   local create_idx = ""
   for i = 1, columns do
      create_col = create_col .. string.format(create_col_tmpl, i)
      create_idx = create_idx .. string.format(create_idx_tmpl, i)
   end 

   local query = string.format(sql_template, get_table_name(column_type, table_id), create_col, create_idx)
   con:query(query)
end

function batch_insert(con, table_id, column_type, columns, rows)
   if (rows == 0) then
      return
   end

   print(string.format("Inserting %d records into '%s'", rows, get_table_name(column_type, table_id)))
   
   local col_list = ""
   for column_id = 1, columns do
      col_list = col_list .. ", " .. get_column_name(column_id)
   end
   local query = ""
   query = string.format("INSERT INTO %s (id, component_id%s) VALUES", get_table_name(column_type, table_id), col_list)
   con:bulk_insert_init(query)

   for i = 1, rows do
      local val_list = ""
      for j = 1, columns do
         val_list = val_list .. ", " .. get_rand(column_type)
      end
      query = string.format("(%d, %d%s)",
         i,
         get_component_id(i),
         val_list)
      con:bulk_insert_next(query)
   end
   con:bulk_insert_done()
end

local t = sysbench.sql.type
local sql_type_mapping = {
   varchar = t.VARCHAR,
   int = t.INT,
   bigint = t.BIGINT,
   float = t.FLOAT,
   double = t.DOUBLE,
   text = t.VARCHAR,
   longtext = t.VARCHAR,
   date = t.DATE,
   datetime = t.DATETIME,
   json = t.VARCHAR   
}

function prepare_begin()
   stmt.begin = con:prepare("BEGIN")
end

function begin()
   stmt.begin:execute()
end

function prepare_commit()
   stmt.commit = con:prepare("COMMIT")
end

function commit()
   stmt.commit:execute()
end

function execute_pk_seeks()
   for i = 1, sysbench.opt.bc_pk_seeks do
      local column_type = get_rand_column_type()
      local table_id = get_rand_table_id()
      local id = get_rand_id(column_type, table_id)
      local sql = string.format(
         "SELECT * FROM %s WHERE component_id=%d AND id=%d", 
         get_table_name(column_type, table_id),
         get_component_id(id),
         id
      )
      con:query(sql)
   end
end

function execute_pk_ranges()
   for i = 1, sysbench.opt.bc_pk_ranges do
      local column_type = get_rand_column_type()
      local table_id = get_rand_table_id()
      local id = get_rand_id(column_type, table_id)
      local sql = string.format(
         "SELECT * FROM %s WHERE component_id=%d AND id BETWEEN %d AND %d", 
         get_table_name(column_type, table_id),
         get_component_id(id),
         id,
         id + sysbench.rand.default(1, sysbench.opt.max_range_interval)
      )
      con:query(sql)
   end
end

function execute_index_seeks()
   for i = 1, sysbench.opt.bc_index_seeks do
      local column_type = get_rand_column_type()
      if column_type ~= "text" and column_type ~= "longtext" and column_type ~= "json" then
         local table_id = get_rand_table_id()
         local id = get_rand_id(column_type, table_id)
         local sql = string.format(
            "SELECT id, version FROM %s WHERE component_id=%d AND %s=%s", 
            get_table_name(column_type, table_id),
            get_component_id(id),
            get_column_name(get_rand_column_id()),
            tostring(get_rand(column_type))
         )
         con:query(sql)
      end
   end
end

function execute_index_ranges()
   for i = 1, sysbench.opt.bc_index_ranges do
      local column_type = get_rand_column_type()
      if column_type ~= "varchar" and column_type ~= "text" and column_type ~= "longtext" and column_type ~= "json" and column_type ~= "date" and column_type ~= "datetime" then
         local table_id = get_rand_table_id()
         local id = get_rand_id(column_type, table_id)
         local sql = string.format(
            "SELECT id, version FROM %s WHERE component_id=%d AND %s BETWEEN %s AND %s", 
            get_table_name(column_type, table_id),
            get_component_id(id),
            get_column_name(get_rand_column_id()),
            tostring(get_rand(column_type)),
            tostring(get_rand(column_type) + sysbench.rand.default(0, sysbench.opt.max_range_interval))
         )
         con:query(sql)
      end
   end
end

function execute_inserts()
   for i = 1, sysbench.opt.bc_inserts do
      local column_type = get_rand_column_type()
      local table_id = get_rand_table_id()
      local id = max_table_rows[column_type .. table_id]
      max_table_rows[column_type .. table_id] = id + sysbench.opt.threads
      local column_list = ""
      local value_list = ""
      for column_id = 1, sysbench.opt.columns do
         column_list = column_list .. string.format(", %s", get_column_name(column_id))
         value_list = value_list .. string.format(", %s", tostring(get_rand(column_type)))
      end
      local sql = string.format(
         "INSERT INTO %s (component_id, id%s) VALUES (%d, %d%s)",
         get_table_name(column_type, table_id),
         column_list,
         get_component_id(id),
         id,
         value_list
      )
      --print(column_type .. table_id, "tid:"..sysbench.tid, "cid:"..id, "nid:"..max_table_rows[column_type .. table_id])
      con:query(sql)
   end
end

function execute_deletes()
   for i = 1, sysbench.opt.bc_deletes do
      local column_type = get_rand_column_type()
      local table_id = get_rand_table_id()
      local id = get_rand_id(column_type, table_id)
      local sql = string.format(
         "DELETE FROM %s WHERE component_id=%d AND id=%d", 
         get_table_name(column_type, table_id),
         get_component_id(id),
         id
      )
      con:query(sql)
   end
end

function execute_updates()
   for i = 1, sysbench.opt.bc_updates do
      local column_type = get_rand_column_type()
      local table_id = get_rand_table_id()
      local id = get_rand_id(column_type, table_id)
      local sql = string.format(
         "UPDATE %s SET %s = %s WHERE component_id=%d AND id=%d", 
         get_table_name(column_type, table_id),
         get_column_name(get_rand_column_id()),
         tostring(get_rand(column_type)),
         get_component_id(id),
         id
      )
      con:query(sql)
   end
end

--[[
local stmt_defs = {
   sum_ranges = {
      "SELECT SUM(c_%d) FROM %s WHERE component_id=? AND id BETWEEN ? AND ?",
      t.INT, t.INT, t.INT},
   order_ranges = {
      "SELECT c FROM sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c",
       t.INT, t.INT},
   distinct_ranges = {
      "SELECT DISTINCT c FROM sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c",
      t.INT, t.INT}
}
]]

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()

   -- This function is a 'callback' defined by individual benchmark scripts
   stmt = {}
   prepare_begin()
   prepare_commit()

   -- Cache max id in each table and set the next id for inserts. The offset is the thread_id + 1.
   max_table_rows = {}
   for i, column_type in ipairs(sysbench.opt.column_types) do
      for table_id = 1, sysbench.opt.tables do
         local res = con:query(string.format("SELECT MAX(id) FROM %s", get_table_name(column_type, table_id)))
         local id = res:fetch_row()[1]
         if id ~= nil then
            max_table_rows[column_type..table_id] = id + sysbench.tid + 1
         end
      end
   end
   con:query("SELECT SLEEP(2);")
end

-- Close prepared statements
function close_statements()
   for i, s in ipairs(stmt) do
      s:close()
   end
   if (stmt.begin ~= nil) then
      stmt.begin:close()
   end
   if (stmt.commit ~= nil) then
      stmt.commit:close()
   end
end

function thread_done()
   close_statements()
   con:disconnect()
end

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for table_id = 1, sysbench.opt.tables do
      for j, column_type in ipairs(sysbench.opt.column_types) do
         print(string.format("Dropping table '%s'...", get_table_name(column_type, table_id)))
         con:query("DROP TABLE IF EXISTS " .. get_table_name(column_type, table_id))
      end 
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

function event()
   begin()
   execute_pk_seeks()
   execute_pk_ranges()
   execute_index_seeks()
   execute_index_ranges()
   execute_inserts()
   execute_deletes()
   execute_updates()
   commit()
   check_reconnect()
end
