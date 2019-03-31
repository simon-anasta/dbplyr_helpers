##################################################################################################################
#' Description: Utility functions to support R development in the IDI.
#'
#' Input:
#'
#' Output:
#' 
#' Author: Simon Anastasiadis
#' 
#' Dependencies:
#' 
#' Notes: Connection details NOT FOR RELEASE!!
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2018-11-05 New connection string function
#' 2018-09-18 removal of reference to sandpit
#' 2018-08-23 SA write_to_sandpit function merged in
#' 2018-04-24 SA v1
#'#################################################################################################################

# connection details
DEFAULT_SERVER = "server.name.url.nz"
DEFAULT_DATABASE = "database_name"
DEFAULT_PORT = NA # fetch from within SQL if needed
# DO NOT RELEASE THESE VALUES

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

#' Not In
#' Negative of %in%
#'
'%not_in%' = function(x,y){
  !('%in%'(x,y))
}

#' Create connection string
#' 
set_connection_string = function(server,database,port = NA){
  connstr = "DRIVER=ODBC Driver 11 for SQL Server; "
  connstr = paste0(connstr, "Trusted_Connection=Yes; ")
  connstr = paste0(connstr, "DATABASE=",database,"; ")
  connstr = paste0(connstr, "SERVER=",server)
  if(!is.na(port))
    connstr = paste0(connstr, ", ",port)
  return(connstr)
}

#' Create database connection point
#' (duplicate connections to IDI_Clean cause trouble with joins)
#' 
create_database_connection = function(..., server = NA, database = NA, port = NA){
  # checks
  assert("odbc" %in% installed.packages(), "odbc package must be installed to connect to database")
  assert(length(list(...)) == 0, "database connection arguments must be named")
  
  # default values
  if(is.na(server))
    server = DEFAULT_SERVER
  if(is.na(database))
    database = DEFAULT_DATABASE
  if(is.na(port))
    port = DEFAULT_PORT
  # connect
  connection_string = set_connection_string(server, database, port)
  db_connection = dbConnect(odbc::odbc(), .connection_string = connection_string)
}

#' Map SQL table into R
#' Creates access point for R to run queries on SQL server
#' 
#' The same db_connection must be used foreach table access point. Otherwise
#' you will notbeable to join tables together. Tables from different databases
#' within thesame server can be accessed bythe same connection.
#' 
create_access_point = function(db_connection, schema, tbl_name){
  # check input
  assert(is.character(schema),"schema name must be character")
  assert(is.character(tbl_name),"table name must be character")
  # special characters (reduce SQL injection risk)
  assert(!grepl("[;:'(){}? ]",schema), "schema must not contain special characters or white space")
  assert(!grepl("[;:'(){}? ]",tbl_name), "table must not contain special characters or white space")
  
  # access table
  if(nchar(schema) > 0)
    table_access = tbl(db_connection, from = in_schema(schema, tbl_name))
  else
    table_access = tbl(db_connection, from = tbl_name)
  return(table_access)
}

#' write out temporary table for reuse
#' 
write_for_reuse = function(db_connection, schema, tbl_name, tbl_to_save, index_columns = NA){
  
  # random suffix for table name to reduce collisions & overrights
  # tbl_name = paste0("chh_tmp_notifications_in_gaps_",floor(runif(1)*1E10))
  # currently implemented elsewhere but could centralize it here
  
  run_time_inform_user("writing temporary table")
  saved_table = write_to_database(tbl_to_save, db_connection, schema, tbl_name, OVERWRITE = TRUE)
  run_time_inform_user("completed write")
  
  if(length(index_columns) > 1 | !is.na(index_columns[1])){
    result = create_clustered_index(db_connection, schema, tbl_name, index_columns)
    run_time_inform_user("added index")
  }
  
  return(saved_table)
}

#' Check for required columns
#' 
table_contains_required_columns = function(tbl_to_check, required_columns, only = FALSE){
  
  # column names of table
  col_names_to_check = colnames(tbl_to_check)
  
  # correct indicator
  correct = TRUE
  
  # required columns in table
  correct = correct & all(required_columns %in% col_names_to_check)
  
  # only required columns in table
  if(only)
    correct = correct & all(col_names_to_check %in% required_columns)
  
  return(correct)
}

#' union all
#' Requires as input tables with identical column names. Provides as output
#' a single table the "union all" of all the input tables.
#' 
union_all = function(table_a,table_b, list_of_columns){
  
  # connection
  db_connection = table_a$src$con
  
  table_a = table_a %>% ungroup() %>% select(list_of_columns)
  table_b = table_b %>% ungroup() %>% select(list_of_columns)
  
  sql_query = build_sql(con = db_connection,
                      sql_render(table_a),
                      "\nUNION ALL\n",
                      sql_render(table_b)
  )
  
  return(tbl(db_connection, sql(sql_query)))
}

#' Delete existing table
#' 
delete_table = function(db_connection, schema, tbl_name, mode = "table"){
  # special characters (reduce SQL injection risk)
  assert(!grepl("[;:'(){}? ]",schema), "schema must not contain special characters or white space")
  assert(!grepl("[;:'(){}? ]",tbl_name), "table must not contain special characters or white space")
  assert(is.character(mode), "mode must be of type character")
  model = tolower(mode)
  assert(mode %in% c("view", "table"), "mode but be in (view, table)")
  
  # type
  if(mode == "table")
    code = "U"
  if(mode == "view")
    code = "V"
  
  # remove database name if schema
  if(mode == "view")
    schema = strsplit(schema,"\\.")[[1]][2]

  # remove table if it exists
  removal_query = build_sql("IF OBJECT_ID('",sql(schema),".",
                            escape(ident(tbl_name), con = db_connection),
                            "','",sql(code),"') IS NOT NULL \n",
                            "DROP ",sql(toupper(mode))," ",sql(schema),".",
                            escape(ident(tbl_name), con = db_connection),
                            ";")
  save_to_sql(removal_query, paste0("delete_",mode))
  result = dbExecute(db_connection, as.character(removal_query))
}

#' Write to database
#' Returning connection to the new table
#' 
write_to_database = function(input_tbl, db_connection, schema, tbl_name, OVERWRITE = FALSE){
  # special characters (reduce SQL injection risk)
  assert(!grepl("[;:'(){}? ]",schema), "schema must not contain special characters or white space")
  assert(!grepl("[;:'(){}? ]",tbl_name), "table must not contain special characters or white space")
  # checks
  assert(is.tbl(input_tbl), "input table must be of type tbl")
  
  # remove table if it exists
  if(OVERWRITE)
    delete_table(db_connection, schema, tbl_name)

  # connection
  tbl_connection = input_tbl$src$con
  # setup
  from_id = ident(paste0("long",floor(runif(1,1000000,9999999))))
  
  # SQL query
  sql_query = build_sql(con = tbl_connection
                        ,"SELECT *\n"
                        ,"INTO ",sql(schema),".",escape(ident(tbl_name), con = db_connection),"\n"
                        ,"FROM (\n"
                        ,sql_render(input_tbl)
                        ,"\n) ", from_id
  )
  
  # run query
  save_to_sql(sql_query, "write_to_database")
  result = dbExecute(db_connection, as.character(sql_query))
  
  # load and return new table
  create_access_point(db_connection, schema, tbl_name)
}

#' Add clustered index to a table
#' Note: at most a single clustered index can be added to each table.
#' This operation is potentially expensive, so should be used only where needed.
#' 
create_clustered_index = function(db_connection, schema, tbl_name, cluster_columns){
  # special characters (reduce SQL injection risk)
  assert(!grepl("[;:'(){}? ]",schema), "schema must not contain special characters or white space")
  assert(!grepl("[;:'(){}? ]",tbl_name), "table must not contain special characters or white space")
  # table in connection
  assert(tbl_name %in% dbListTables(db_connection),"table is not found in database")
  # columns are in table
  assert(all(cluster_columns %in% colnames(create_access_point(db_connection, schema, tbl_name))), 
         "database table does not have the required columns")
  
  
  index_name = "my_clustered_index"
  
  query = paste0("CREATE CLUSTERED INDEX ",index_name," ON ",
                 schema,".",tbl_name,
                 " ( [",
                 paste0(cluster_columns,collapse = "], ["),
                 "] )")
  
  # print(query)
  save_to_sql(query, "add_clustered_index")
  result = dbExecute(db_connection, as.character(query))
}

#' Append rows to an existing table
#' Note: character strings can not exceed the length of existing character strings in original dataset.
#' 
append_database_table = function(db_connection, schema, tbl_name, list_of_columns, table_to_append){
  # special characters (reduce SQL injection risk)
  assert(!grepl("[;:'(){}? ]",schema), "schema must not contain special characters or white space")
  assert(!grepl("[;:'(){}? ]",tbl_name), "table must not contain special characters or white space")
  # table in connection
  assert(tbl_name %in% dbListTables(db_connection),"table is not found in database")
  # tables contain list of columns
  assert(all(list_of_columns %in% colnames(table_to_append)), "table to append does not have required columns")
  assert(all(list_of_columns %in% colnames(create_access_point(db_connection, schema, tbl_name))), 
         "database table does not have the required columns")

  table_to_append = table_to_append %>% ungroup() %>% select(list_of_columns)
  
  sql_list_of_columns = paste0(escape(ident(list_of_columns), con = db_connection), collapse = ", ")
  
  query = paste0("INSERT INTO ", schema,".",tbl_name,
                 "(",sql(sql_list_of_columns),")",
                 "\n  ",sql_render(table_to_append))
  
  # print(query)
  save_to_sql(query, "append_table")
  result = dbExecute(db_connection, as.character(query))
}

#' Create new table
#' 
create_table = function(db_connection, schema, tbl_name, named_list_of_columns, OVERWRITE = FALSE){
  # special characters (reduce SQL injection risk)
  assert(!grepl("[;:'(){}? ]",schema), "schema must not contain special characters or white space")
  assert(!grepl("[;:'(){}? ]",tbl_name), "table must not contain special characters or white space")
  
  # remove table if it exists
  if(OVERWRITE)
    delete_table(db_connection, schema, tbl_name)
  
  # setup queries
  dbExecute(db_connection, as.character(build_sql(con = db_connection, "SET ANSI_NULLS ON")))
  dbExecute(db_connection, as.character(build_sql(con = db_connection, "SET QUOTED_IDENTIFIER ON")))
  dbExecute(db_connection, as.character(build_sql(con = db_connection, "SET ANSI_PADDING ON")))
  
  # SQL query
  sql_query = build_sql(con = db_connection
                        ,"CREATE TABLE ", sql(schema), ".", sql(tbl_name),"(","\n"
                        ,sql(paste0("[",names(named_list_of_columns),"] "
                                , named_list_of_columns, collapse = ",\n")),"\n"
                        ,") ON [PRIMARY]"
  )
  
  # run query
  save_to_sql(sql_query, "create_table")
  result = dbExecute(db_connection, as.character(sql_query))
  
  # post queries
  dbExecute(db_connection, as.character(build_sql(con = db_connection, "SET ANSI_PADDING OFF")))
  
  return(result)
}

#' Delete all tables in the schema with the given prefix.
#' Intended for use deleting temporary tables (e.g. tmp_table_name)
#'
purge_tables_by_prefix = function(db_connection, schema, tbl_prefix, mode = "table", exclude = NA){
  # special characters (reduce SQL injection risk)
  assert(!grepl("[;:'(){}? ]",schema), "schema must not contain special characters or white space")
  
  # get all tables in database
  all_tables = dbListTables(db_connection)
  # keep only with prefix
  tables_to_drop = all_tables[grepl(paste0('^',tbl_prefix), all_tables)]
  # exclude if exists
  if(!all(is.na(exclude)))
    tables_to_drop = tables_to_drop[tables_to_drop %not_in% exclude]
  
  for(tbl_name in tables_to_drop)
    delete_table(db_connection, schema, tbl_name, mode)
  
}

#' Save SQL queries to files
#'
save_to_sql = function(query, desc){
  
  if(!dir.exists("./SQL tmp scripts"))
    dir.create("./SQL tmp scripts")
  
  clean_name = gsub("[. :]","_",desc)
  clean_time = gsub("[. :]","_",Sys.time())
  
  file_name = paste0("./SQL tmp scripts/",clean_name," ",clean_time,".sql")
  
  if(file.exists(file_name)){
    Sys.sleep(1)
    clean_time = gsub("[. :]","_",Sys.time())
    file_name = paste0("./SQL tmp scripts/",clean_name," ",clean_time,".sql")
  }

  writeLines(as.character(query), file_name)
}

#' Robustness support to help handle database connection being reset
#'
robustness_support = function(mode, db_connection, schema, tbl_name, index_columns = NA){
  assert(mode %in% c('save','load'),"Accepted modes are load and save")
  assert(mode != 'load' | is.character(tbl_name), "tbl_name must be of type character if mode = load")
  assert(mode != 'save' | is.tbl(tbl_name), "tbl_name must be of type tbl if mode = save")
  
  if(mode == 'save'){
    out_table = paste0("chh3_tmp_cautious_",floor(runif(1)*1E10))
    tbl_name = write_for_reuse(db_connection, schema, out_table, tbl_name, index_columns)
  }
  
  if(mode == 'load'){
    out_table = create_access_point(db_connection, schema, tbl_name)
  }
  
  return(out_table)
}

#' Copy R table to SQL
#' Work around as existing 'copy_to' function does not appear to work in our environment.
#'
copy_r_to_sql = function(db_connection, schema, sql_table_name, r_table_name,
                         named_list_of_columns, OVERWRITE = FALSE){
  # special character (reduces SQL injection risk)
  assert(!grepl("[;:'(){}? ]",schema), "schema must not contain special characters or white space")
  assert(!grepl("[;:'(){}? ]",sql_table_name), "schema must not contain special characters or white space")
  # corresponding columns
  assert(all(names(named_list_of_columns) %in% colnames(r_table_name)),
         "sql table requests columns not in R table")
  
  tmp = create_table(db_connection, shema, sql_table_name, named_list_of_columns, OVERWRITE)
  
  # trim r table to just variables of interest
  r_table_name = r_table_name %>%
    select(names(named_list_of_columns))
  
  # handle single quotes from input table
  for(coln in colnames(r_table_name)){
    if(is.character(r_table_name[[coln]][1]))
      r_table_name[coln] = apply(r_table_name[coln], 1, function(x) sub("'", "''", x))
  }
  
  # if column type is character or date, wrap in single quotes so SQL reads it as character string
  for(coln in colnames(r_table_name)){
    col_type = named_list_of_columns[[coln]]
    of(grepl("char", col_type) | grepl("date", col_type))
    r_table_name[coln] = apply(r_table_name[coln], 1, function(x) paste0("'", as.character(x), "'"))
  }
  
  # SQL
  sql_cols = paste0("([",paste0(names(named_list_of_columns), collapse = "],["), "])")
  sql_values = paste0(apply(r_table_name, 1, 
                            function(x) paste0("(", paste0(x, collapse = ","),")")),
                      collapse = ",\n")
  
  my_sql = build_sql(con = db_connection,
                     "INSERT INTO ", sql(schema), ".",sql(sql_table_name),"\n",
                     sql(sql_cols), "\n",
                     "VALUES ", sql(sql_values),";")
  
  save_to_sql(my_sql, "write_r_to_sql")
  result = dbExecute(db_connection, as.character(my_sql))
  
  r_table_name = create_access_point(db_connection, schema, sql_table_name)
}
  
