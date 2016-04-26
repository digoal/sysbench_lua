pathtest = string.match(test, "(.*/)") or ""

dofile(pathtest .. "common.lua")

function copydata(table_id)
  local ctl
  local f
  local i
  local c_val
  local pad_val
  local content
  local query

  query = [[
CREATE UNLOGGED TABLE sbtest]] .. table_id .. [[ (
id SERIAL NOT NULL,
k INTEGER,
c CHAR(120) DEFAULT '' NOT NULL,
pad CHAR(60) DEFAULT '' NOT NULL,
PRIMARY KEY (id)
) ]]

  db_query(query)

  os.execute ('export PGPASSWORD=' .. pgsql_password)
  os.execute ('test -f sbtest' .. table_id .. '.dat && mknod sbtest' .. table_id .. '.dat p')
  os.execute ('./gendata ' .. oltp_table_size .. ' >> sbtest'..table_id ..'.dat &')
  os.execute ('cat sbtest' .. table_id .. '.dat | psql -h ' .. pgsql_host .. ' -p ' .. pgsql_port .. ' -U ' .. pgsql_user .. ' -d ' .. pgsql_db .. ' -c "copy sbtest' .. table_id .. ' from stdin with csv"')
end

function create_index(table_id)
  db_query("select setval('sbtest" .. table_id .. "_id_seq', " .. (oltp_table_size+1) .. ")" )
  db_query("CREATE INDEX k_" .. table_id .. " on sbtest" .. table_id .. "(k)")
end

function thread_init(thread_id)
   local index_name
   local i

   set_vars()

   print("thread prepare"..thread_id)

   if (oltp_secondary) then
     index_name = "KEY xid"
   else
     index_name = "PRIMARY KEY"
   end

   for i=thread_id+1, oltp_tables_count, num_threads  do
     copydata(i)
     create_index(i)
   end
end

function event(thread_id)
   os.exit()
end
