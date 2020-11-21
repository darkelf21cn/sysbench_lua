--[[
   This is about to test the database performance under a highly flexible
   table structure. Data are in the database are grouped by types with fixed
   column width and names and even indexes.
]]

-- Command line options
sysbench.cmdline.options = {
   table_size =
      {"Number of rows", 1000000},
   tables =
      {"Number of tables", 1},
   dup_rate =
      {"The rate of duplicate value of val column", 10},
   migrate_batch_size = 
      {"The batch size of each data movement", 1000},
   reconnect =
      {"Reconnect after every N events. The default (0) is to not reconnect", 0},
}

local sql_stmts = {
   create_table_int_x5 = [[
CREATE TABLE data_frame_int_x5_%d (
   id BIGINT UNSIGNED,
   component_id BIGINT UNSIGNED NOT NULL,
   version_ts DATETIME NOT NULL,
   value_1 INT DEFAULT NULL,
   value_2 INT DEFAULT NULL,
   value_3 INT DEFAULT NULL,
   value_4 INT DEFAULT NULL,
   value_5 INT DEFAULT NULL,
   insert_dt DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
   update_dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (id),
   INDEX (component_id, value_1, version_ts),
   INDEX (component_id, value_2, version_ts),
   INDEX (component_id, value_3, version_ts),
   INDEX (component_id, value_4, version_ts),
   INDEX (component_id, value_5, version_ts),
   INDEX (insert_dt),
   INDEX (update_dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;]]
   create_table_bigint_x5 = [[
CREATE TABLE data_frame_bitint_x5_%d (
   id BIGINT UNSIGNED,
   component_id BIGINT UNSIGNED NOT NULL,
   version_ts DATETIME NOT NULL,
   value_1 BIGINT DEFAULT NULL,
   value_2 BIGINT DEFAULT NULL,
   value_3 BIGINT DEFAULT NULL,
   value_4 BIGINT DEFAULT NULL,
   value_5 BIGINT DEFAULT NULL,
   insert_dt DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
   update_dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (id),
   INDEX (component_id, value_1, version_ts),
   INDEX (component_id, value_2, version_ts),
   INDEX (component_id, value_3, version_ts),
   INDEX (component_id, value_4, version_ts),
   INDEX (component_id, value_5, version_ts),
   INDEX (insert_dt),
   INDEX (update_dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;]]
   create_table_varchar32_x5 = [[
CREATE TABLE data_frame_varchar32_x5_%d (
   id BIGINT UNSIGNED,
   component_id BIGINT UNSIGNED NOT NULL,
   version_ts DATETIME NOT NULL,
   value_1 VARCHAR(32) DEFAULT NULL,
   value_2 VARCHAR(32) DEFAULT NULL,
   value_3 VARCHAR(32) DEFAULT NULL,
   value_4 VARCHAR(32) DEFAULT NULL,
   value_5 VARCHAR(32) DEFAULT NULL,
   insert_dt DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
   update_dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (id),
   INDEX (component_id, value_1, version_ts),
   INDEX (component_id, value_2, version_ts),
   INDEX (component_id, value_3, version_ts),
   INDEX (component_id, value_4, version_ts),
   INDEX (component_id, value_5, version_ts),
   INDEX (insert_dt),
   INDEX (update_dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;]]
   create_table_varchar64_x5 = [[
CREATE TABLE data_frame_varchar64_x5_%d (
   id BIGINT UNSIGNED,
   component_id BIGINT UNSIGNED NOT NULL,
   version_ts DATETIME NOT NULL,
   value_1 VARCHAR(64) DEFAULT NULL,
   value_2 VARCHAR(64) DEFAULT NULL,
   value_3 VARCHAR(64) DEFAULT NULL,
   value_4 VARCHAR(64) DEFAULT NULL,
   value_5 VARCHAR(64) DEFAULT NULL,
   insert_dt DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
   update_dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (id),
   INDEX (component_id, value_1, version_ts),
   INDEX (component_id, value_2, version_ts),
   INDEX (component_id, value_3, version_ts),
   INDEX (component_id, value_4, version_ts),
   INDEX (component_id, value_5, version_ts),
   INDEX (insert_dt),
   INDEX (update_dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;]]
   create_table_varchar128_x5 = [[
CREATE TABLE data_frame_varchar128_x5_%d (
   id BIGINT UNSIGNED,
   component_id BIGINT UNSIGNED NOT NULL,
   version_ts DATETIME NOT NULL,
   value_1 VARCHAR(128) DEFAULT NULL,
   value_2 VARCHAR(128) DEFAULT NULL,
   value_3 VARCHAR(128) DEFAULT NULL,
   value_4 VARCHAR(128) DEFAULT NULL,
   value_5 VARCHAR(128) DEFAULT NULL,
   insert_dt DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
   update_dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (id),
   INDEX (component_id, value_1, version_ts),
   INDEX (component_id, value_2, version_ts),
   INDEX (component_id, value_3, version_ts),
   INDEX (component_id, value_4, version_ts),
   INDEX (component_id, value_5, version_ts),
   INDEX (insert_dt),
   INDEX (update_dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;]]
   create_table_varchar256_x5 = [[
CREATE TABLE data_frame_varchar256_x5_%d (
   id BIGINT UNSIGNED,
   component_id BIGINT UNSIGNED NOT NULL,
   version_ts DATETIME NOT NULL,
   value_1 VARCHAR(256) DEFAULT NULL,
   value_2 VARCHAR(256) DEFAULT NULL,
   value_3 VARCHAR(256) DEFAULT NULL,
   value_4 VARCHAR(256) DEFAULT NULL,
   value_5 VARCHAR(256) DEFAULT NULL,
   insert_dt DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
   update_dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (id),
   INDEX (component_id, value_1, version_ts),
   INDEX (component_id, value_2, version_ts),
   INDEX (component_id, value_3, version_ts),
   INDEX (component_id, value_4, version_ts),
   INDEX (component_id, value_5, version_ts),
   INDEX (insert_dt),
   INDEX (update_dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;]]
   create_table_json_x1 = [[
CREATE TABLE data_frame_json_x1_%d (
   id BIGINT UNSIGNED,
   component_id BIGINT UNSIGNED NOT NULL,
   version_ts DATETIME NOT NULL,
   lookup_key VARCHAR(128) DEFAULT NULL,
   json_content JSON,
   insert_dt DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
   update_dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (id),
   INDEX (component_id, lookup_key, version_ts),
   INDEX (insert_dt),
   INDEX (update_dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;]]
   create_table_text_x1 = [[
CREATE TABLE data_frame_text_x1_%d (
   id BIGINT UNSIGNED,
   component_id BIGINT UNSIGNED NOT NULL,
   version_ts DATETIME NOT NULL,
   lookup_key VARCHAR(128) DEFAULT NULL,
   text_content TEXT,
   insert_dt DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
   update_dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (id),
   INDEX (component_id, lookup_key, version_ts),
   INDEX (insert_dt),
   INDEX (update_dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;]]
}

function init()
   assert(event ~= nil,
      "this script is meant to be included by other scripts and " ..
         "should not be called directly.")
end

function prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   local tnum = get_table_num()
   for i, sql_stmt in ipairs(sql_stmts) do
      for j = 0, tnum do
          query = string.format(sql_stmt, j)
      end
   end

   query string.format(sql_stmts.)

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

local function get_table_num()
   return sysbench.rand.uniform(1, sysbench.opt.tables)
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