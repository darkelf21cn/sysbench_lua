--[[
   This is about to simulate the membercard distribution scenario. All the membercards are
   store in the membercard_pool table. When a user requests a new membercard, it will assign
   a free card to the user with a minimum card id. To make sure the min id is assigned an
   no card will be assigned more than once, we use an exclusive lock to make sure the card
   that we are going to assign is properly locked. It causes massive locks when the
   parallelism is high. The test case presents how the skip_locked hint helps with this.
]]

-- Command line options
sysbench.cmdline.options = {
   total_cards =
      {"Number of rows", 1000000},
   lock_mode =
      {"The lock mode that used for fetching a un-allocated record. {exclusive, skip_locked}", "exclusive"},
   reconnect =
      {"Reconnect after every N events. The default (0) is to not reconnect", 0},
}

function prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   local pad = "@@@@@@@@-@@@@@@@@-@@@@@@@@-@@@@@@@@-@@@@@@@@-@@@@@@@@-@@@@@@@@-" ..
               "@@@@@@@@-@@@@@@@@-@@@@@@@@-@@@@@@@@-@@@@@@@@-@@@@@@@@-@@@@@@@@-" ..
               "@@@@@@@@-@@@@@@@@-@@@@@@@@-@@@@@@@@-@@@@@@@@-@@@@@@@@-@@@@@@@@"

   print("Creating tables")
   local query
   query = [[
CREATE TABLE membercard_pool (
   id BIGINT,
   is_allocated TINYINT,
   pad VARCHAR(255),
   insert_dt DATETIME DEFAULT CURRENT_TIMESTAMP,
   update_dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY (id),
   KEY (is_allocated)
)]]
   con:query(query)

   if (sysbench.opt.total_cards > 0) then
      print(string.format("Inserting %d records into 'membercard_pool'", sysbench.opt.total_cards))
   end
   query = "INSERT INTO membercard_pool (id, is_allocated, pad) VALUES"
   con:bulk_insert_init(query)
   for i = 1, sysbench.opt.total_cards do
      query = string.format("(%d, 0, '%s')", sysbench.rand.unique(), sysbench.rand.string(pad))
      con:bulk_insert_next(query)
   end
   con:bulk_insert_done()
end

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   print("Dropping table 'membercard_pool'...")
   con:query("DROP TABLE IF EXISTS membercard_pool")
end

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()

   stmt = {}
   param = {}
   prepare_statements()
end

function thread_done()
   con:disconnect()
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

function prepare_allocate_member_card()
   local key = "allocate_member_card"
   local query = [[
UPDATE
   membercard_pool a
   INNER JOIN (SELECT MIN(id) AS id FROM membercard_pool WHERE is_allocated = 0 %s) b
      ON a.id = b.id
SET a.is_allocated = 1]]
   local lock = ""
   if sysbench.opt.lock_mode == "exclusive" then
      lock = "FOR UPDATE"
   else
      lock = "FOR UPDATE SKIP LOCKED"
   end
   stmt[key]= con:prepare(string.format(query, lock))
end

function prepare_begin()
   stmt.begin = con:prepare("BEGIN")
end

function prepare_commit()
   stmt.commit = con:prepare("COMMIT")
end

function prepare_statements()
   prepare_allocate_member_card()
   prepare_begin()
   prepare_commit()
end

function begin()
   stmt.begin:execute()
end

function commit()
   stmt.commit:execute()
end

local function execute_allocate_member_card()
   stmt.allocate_member_card:execute()
end

function event()
   begin()
   execute_allocate_member_card()
   commit()
end
