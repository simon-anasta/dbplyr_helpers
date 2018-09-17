##################################################################################################################
# Description: Utility functions to support R development in the IDI with dbplyr.
#
# Author: Simon Anastasiadis
# 
# History (reverse order):
# 2018-08-23 SA write_to_sandpit function merged in
# 2018-04-24 SA v1
##################################################################################################################

# library
library(DBI)
library(dplyr)
library(dbplyr)

#' Assert function for run time testing of code.
#' Throws an error if condition is not TRUE
#' 
assert = function(condition, msg){
  # condition must be logical
  if(! is.logical(condition)){
    msg <- sprintf("expected condition to be logical, received %s instead\n",class(condition))
    stop(msg)
  }
  # check condition and throw error
  if(condition == FALSE)
    stop(msg)
}

#' Inform user with time stamped measure
#'
run_time_inform_user = function(msg){
  # time
  now = as.character(Sys.time())
  # display
  cat(now, "|", msg, "\n")
}

#' Create database connection point
#' (duplicate connections to IDI_Clean cause trouble with joins)
#' 
create_database_connection = function(database_name){
  # check input
  assert(is.character(database_name),"database name must be character")
  # connection
  connection_string = SIAtoolbox::set_conn_string(db = database_name)
  db_connection = dbConnect(odbc::odbc(), .connection_string = connection_string)
}

#' Map SQL table into R
#' Creates access point for R to run queries on SQL server
#' 
#' db_hack lets us bypass the requirement that the table exists in the database.
#' This is useful for working in the IDI as an IDI_sandpit connection can be used to
#' access the IDI_Clean. This is necessary when joining two tables, one from the
#' sandpit and the other from clean.
#' 
create_access_point = function(db_connection, schema_name, table_name, db_hack = FALSE){
  # check input
  assert(is.character(schema_name),"schema name must be character")
  assert(is.character(table_name),"table name must be character")
  # table in connection
  assert(db_hack | table_name %in% dbListTables(db_connection),"table is not found in database")
  # access table
  if(nchar(schema_name) > 0)
    table_access = tbl(db_connection, from = in_schema(schema_name, table_name))
  else
    table_access = tbl(db_connection, from = table_name)
  return(table_access)
}

#' union all
#' Requires as input tables with identical column names. Provides as output
#' a single table the "union all" of all the input tables.
#' 
union_all = function(table_a,table_b, list_of_columns){
  
  # connection
  connection = table_a$src$con
  
  table_a = table_a %>% select(list_of_columns)
  table_b = table_b %>% select(list_of_columns)
  
  sql_query = build_sql(con = connection,
                      sql_render(table_a),
                      "\nUNION ALL\n",
                      sql_render(table_b)
  )
  
  return(tbl(connection, sql(sql_query)))
}

#' Delete existing sandpit table
#' 
delete_sandpit_table = function(sandpit_connection, sandpit_schema, sandpit_tbl_name){
  # checks
  assert(is.character(sandpit_tbl_name), "new table name must be of type character")
  assert(!grepl("[;:'(){}? ]",sandpit_schema), "sandpit schema must not contain special characters or white space")
  
  # remove table if it exists
  removal_query = build_sql("IF OBJECT_ID('",sql(sandpit_schema),".",
                            escape(ident(sandpit_tbl_name), con = sandpit_connection),
                            "','U') IS NOT NULL \n",
                            "DROP TABLE ",sql(sandpit_schema),".",
                            escape(ident(sandpit_tbl_name), con = sandpit_connection),
                            ";")
  result = dbExecute(sandpit_connection, as.character(removal_query))
}

#' Write to sandpit
#' Returning connection to the new table
#' 
write_to_sandpit = function(input_tbl, sandpit_connection, sandpit_schema, sandpit_tbl_name, OVERWRITE = FALSE){
  # checks
  assert(is.tbl(input_tbl), "input table must be of type tbl")
  assert(is.character(sandpit_tbl_name), "new table name must be of type character")
  assert(!grepl("[;:'(){}? ]",sandpit_schema), "sandpit schema must not contain special characters or white space")
  
  # remove table if it exists
  if(OVERWRITE)
    delete_sandpit_table(sandpit_connection, sandpit_schema, sandpit_tbl_name)

  # connection
  connection = input_tbl$src$con
  # setup
  from_id = ident(paste0("long",floor(runif(1,1000000,9999999))))
  
  # SQL query
  sql_query = build_sql(con = connection
                        ,"SELECT *\n"
                        ,"INTO ",sql(sandpit_schema),".",escape(ident(sandpit_tbl_name), con = connection),"\n"
                        ,"FROM (\n"
                        ,sql_render(input_tbl)
                        ,"\n) ", from_id
  )
  
  # run query
  result = dbExecute(db_con_IDI_sandpit, as.character(sql_query))
  
  # load and return new table
  create_access_point(sandpit_connection, sandpit_schema, sandpit_tbl_name)
}

#' Add clustered index to a table
#' Note: at most a single clustered index can be added to each table.
#' This operation is potentially expensive, so should be used only where needed.
#' 
create_clustered_index = function(sandpit_connection, sandpit_schema, sandpit_tbl_name, cluster_columns){
  index_name = "my_clustered_index"
  
  query = paste0("CREATE CLUSTERED INDEX ",index_name," ON ",
                 sandpit_schema,".",sandpit_tbl_name,
                 " ( [",
                 paste0(cluster_columns,collapse = "], ["),
                 "] )")
  
  # print(query)
  result = dbExecute(sandpit_connection, as.character(query))
}

