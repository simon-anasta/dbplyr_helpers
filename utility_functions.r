###############################################################################
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
#' 2019-06-13 replaced copy_r_to_sql with recommended solution (past version in HaBiSA)
#' 2019-06-12 combined versions, improved descriptions & clarity
#' 2018-11-05 New connection string function
#' 2018-09-18 removal of reference to sandpit
#' 2018-08-23 SA write_to_sandpit function merged in
#' 2018-04-24 SA v1
#'#############################################################################

# connection details
DEFAULT_SERVER <- "server.domain.name"
DEFAULT_DATABASE <- "DB_NAME"
DEFAULT_PORT <- NA
# DO NOT RELEASE THESE VALUES

# error if connection details are missing
if(is.na(DEFAULT_SERVER)
   | nchar(DEFAULT_SERVER) < 1
   | is.na(DEFAULT_DATABASE)
   | nchar(DEFAULT_DATABASE) < 1)
  stop("Default server and database must be set in utility_functions.R")

# library
library(DBI)
library(dplyr)
library(dbplyr)

#' Assert function for run time testing of code.
#' Throws an error if condition is not TRUE
#' 
assert <- function(condition, msg){
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
#' Prints to console time of function call followed by msg.
#'
run_time_inform_user <- function(msg){
  # time
  now <- as.character(Sys.time())
  # display
  cat(now, "|", msg, "\n")
}

#' Not In
#' Negative of %in%
#'
'%not_in%' <- function(x,y){
  !('%in%'(x,y))
}

#' Create connection string
#' 
#' ODBC connections require a connection string to define the connection
#' between the database and the access point. This string has a default
#' format that this function satisfies.
#' 
#' This function does not need to be called directly to use the utility
#' functions. It is called by create_database_connection. 
#' 
set_connection_string <- function(server,database,port = NA){
  connstr <- "DRIVER=ODBC Driver 11 for SQL Server; "
  connstr <- paste0(connstr, "Trusted_Connection=Yes; ")
  connstr <- paste0(connstr, "DATABASE=",database,"; ")
  connstr <- paste0(connstr, "SERVER=",server)
  if(!is.na(port))
    connstr <- paste0(connstr, ", ",port)
  return(connstr)
}

#' Create database connection point
#' 
#' Any arguments passed to the function need to be named.
#' Default values are set at the top of this script.
#' 
#' A single connection can access multiple associated databases.
#' We recommend using a single connection to access all tables
#' because attempts to join tables from different connections
#' (even if they are in the same database) perform poorly if
#' at all.
#' 
create_database_connection <- function(..., server = NA, database = NA, port = NA){
  # checks
  assert("odbc" %in% installed.packages(), "odbc package must be installed to connect to database")
  assert(length(list(...)) == 0, "database connection arguments must be named")
  
  # default values
  if(is.na(server))
    server <- DEFAULT_SERVER
  if(is.na(database))
    database <- DEFAULT_DATABASE
  if(is.na(port))
    port <- DEFAULT_PORT
  # connect
  connection_string <- set_connection_string(server, database, port)
  db_connection <- dbConnect(odbc::odbc(), .connection_string = connection_string)
}

#' Closes an open database connection
#' 
#' Good practice is to close open connections once they are finished with.
#' Large numbers of open connections can degrade performance.
#' 
close_database_connection <- function(db_connection){
  dbDisconnect(db_connection)
}

#' Map SQL table into R
#' Creates access point for R to run queries on SQL server.
#' 
#' Use this in place of loading a table into R. It will be treated as an R dataframe
#' but the data will remain on the SQL server (instead of in R memory).
#' 
#' Input 'schema' should include database and schema name (where applicable).
#' e.g. "[database].[schema]"
#' 
#' The same db_connection must be used foreach table access point. Otherwise
#' you will not be able to join tables together. Tables from different databases
#' within the same server can be accessed bythe same connection.
#' 
create_access_point <- function(db_connection, schema, tbl_name){
  # check input
  no_special_characters(schema)
  no_special_characters(tbl_name)
  
  # access table
  if(nchar(schema) > 0)
    table_access <- tbl(db_connection, from = in_schema(schema, tbl_name))
  else
    table_access <- tbl(db_connection, from = tbl_name)
  return(table_access)
}

#' write out temporary table for reuse
#' 
#' Takes an R object (tbl_to_save) that is an SQL table that has been
#' manipulated/transformed and saves it as a new table in SQL.
#' Returns a connection to the new table.
#' 
#' For complex or extended analyses, this is recommended as it reduces
#' the complexity of the underlying SQL query that defines the manipulated
#' table.
#' 
write_for_reuse <- function(db_connection, schema, tbl_name, tbl_to_save, index_columns = NA){
  # check input
  no_special_characters(schema)
  no_special_characters(tbl_name)
  
  run_time_inform_user("writing temporary table")
  saved_table <- write_to_database(tbl_to_save, db_connection, schema, tbl_name, OVERWRITE = TRUE)
  run_time_inform_user("completed write")
  
  if(length(index_columns) > 1 | !is.na(index_columns[1])){
    result <- create_clustered_index(db_connection, schema, tbl_name, index_columns)
    run_time_inform_user("added index")
  }
  
  return(saved_table)
}

#' Check for required columns
#' 
#' Returns TRUE if the table contains all the required columns.
#' If only = TRUE, returns TRUE if the table ONLY contains all
#' the required columns.
#' 
#' Checks column names only, does not consider contents.
#' 
table_contains_required_columns <- function(tbl_to_check, required_columns, only = FALSE){
  
  # column names of table
  table_column_names <- colnames(tbl_to_check)
  
  # required columns in table
  correct <- all(required_columns %in% table_column_names)
  # only required columns in table
  if(only)
    correct <- correct & all(table_column_names %in% required_columns)
  
  return(correct)
}

#' Check input string for absence of special characters
#' Intended to prevent accidental SQL injection
#' 
#' No return if input is string and contains no special characters.
#' Errors if input is not string or if string contains special characters
#' or white space
#' 
no_special_characters <- function(in_string){
  msg <- sprintf("%s must be of type character", deparse(substitute(in_string)))
  assert(is.character(in_string), msg)
  
  msg <- sprintf("%s contains special characters", deparse(substitute(in_string)))
  assert(!grepl("[;:'(){}?]", in_string), msg)
  
  msg <- sprintf("%s contains white space", deparse(substitute(in_string)))
  assert(!grepl("[[:space:]]", in_string), msg)
  
  return(NULL)
}

#' Provides UNION ALL functionality from SQL. Appends two tables.
#' 
#' Requires a input tables to have identical column names. Provides as output
#' a single table the "union all" of all the input tables.
#' 
union_all <- function(table_a,table_b, list_of_columns){
  # connection
  db_connection <- table_a$src$con
  
  table_a <- table_a %>% ungroup() %>% select(list_of_columns)
  table_b <- table_b %>% ungroup() %>% select(list_of_columns)
  
  sql_query <- build_sql(con = db_connection,
                         sql_render(table_a),
                         "\nUNION ALL\n",
                         sql_render(table_b)
  )
  return(tbl(db_connection, sql(sql_query)))
}

#' Delete (drop) tables and views from SQL.
#' 
#' Checks for existence of table/view prior to dropping.
#' Can only drop views if the input connection is to the database containing
#' the views.
#' 
delete_table <- function(db_connection, schema, tbl_name, mode = "table"){
  no_special_characters(schema)
  no_special_characters(tbl_name)
  mode <- tolower(mode)
  assert(mode %in% c("view", "table"), "mode must be in (view, table)")
  
  # type
  if(mode == "table") code <- "U"
  if(mode == "view") code <- "V"
  
  # remove database name if schema
  if(mode == "view")
    schema <- strsplit(schema,"\\.")[[1]][2]
  
  # remove table if it exists
  removal_query <- build_sql("IF OBJECT_ID('",sql(schema),".",
                             escape(ident(tbl_name), con = db_connection),
                             "','",sql(code),"') IS NOT NULL \n",
                             "DROP ",sql(toupper(mode))," ",sql(schema),".",
                             escape(ident(tbl_name), con = db_connection),
                             ";")
  save_to_sql(removal_query, paste0("delete_",mode))
  result <- dbExecute(db_connection, as.character(removal_query))
}

#' Write to database
#' Returning connection to the new table
#' 
#' Given a table from a database connection, writes to a new table using the
#' SELECT ... INTO ... FROM ... pattern.
#' 
#' Original table must come from an SQL connection.
#' Does not write R tables into SQL. Use copy_r_to_sql for this.
#' 
#' E.g. the following works
#'       my_table <- create_access_point(....)
#'       write_to_database(my_table, ...)
#' But this will not work
#'       my_table <- data.frame(x = 1:10, y = 1:10)
#'       write_to_database(my_table, ...)
#' 
write_to_database <- function(input_tbl, db_connection, schema, tbl_name, OVERWRITE = FALSE){
  no_special_characters(schema)
  no_special_characters(tbl_name)
  # checks
  assert("tbl_sql" %in% class(input_tbl), "input table must originate from sql connection")
  
  # remove table if it exists
  if(OVERWRITE) delete_table(db_connection, schema, tbl_name)
  
  # connection
  tbl_connection <- input_tbl$src$con
  # setup
  from_id <- ident(paste0("long",floor(runif(1,1000000,9999999))))
  
  # SQL query
  sql_query <- build_sql(con = tbl_connection
                         ,"SELECT *\n"
                         ,"INTO ",sql(schema),".",escape(ident(tbl_name), con = db_connection),"\n"
                         ,"FROM (\n"
                         ,sql_render(input_tbl)
                         ,"\n) ", from_id
  )
  
  # run query
  save_to_sql(sql_query, "write_to_database")
  result <- dbExecute(db_connection, as.character(sql_query))
  
  # load and return new table
  create_access_point(db_connection, schema, tbl_name)
}

#' Add clustered index to a table
#' Note: at most a single clustered index can be added to each table.
#' This operation is potentially expensive, so should be used only where needed.
#' 
create_clustered_index <- function(db_connection, schema, tbl_name, cluster_columns){
  no_special_characters(schema)
  no_special_characters(tbl_name)
  # table in connection
  assert(tbl_name %in% dbListTables(db_connection),"table is not found in database")
  # columns are in table
  assert(all(cluster_columns %in% colnames(create_access_point(db_connection, schema, tbl_name))), 
         "database table does not have the required columns")
  
  query <- paste0("CREATE CLUSTERED INDEX my_index_name ON ",
                  schema,".",tbl_name,
                  " ( [",
                  paste0(cluster_columns,collapse = "], ["),
                  "] )")
  
  # print(query)
  save_to_sql(query, "add_clustered_index")
  result <- dbExecute(db_connection, as.character(query))
}

#' Append rows to an existing table
#' 
#' Given a table from a database connection, append it to an existing table
#' using the INSERT INTO ... (COLUMN NAMES) SELECT ... FROM ... pattern.
#' 
#' Like write_to_database, the original table must come from an SQL connection.
#' Does not append R tables onto SQL tables. You first need to write the R table
#' into SQL.
#' 
#' A common error occurs when character strings in the appended table exceed the
#' length of the existing varchar(n) limit in the original dataset.
#' E.g. appending the string 'ABCDE' into a varchar(3) column will error.
#' 
append_database_table <- function(db_connection, schema, tbl_name, list_of_columns, table_to_append){
  no_special_characters(schema)
  no_special_characters(tbl_name)
  assert("tbl_sql" %in% class(table_to_append), "table to append must originate from sql connection")
  # table in connection
  assert(tbl_name %in% dbListTables(db_connection),"table is not found in database")
  # tables contain list of columns
  assert(all(list_of_columns %in% colnames(table_to_append)),
         "table to append does not have required columns")
  assert(all(list_of_columns %in% colnames(create_access_point(db_connection, schema, tbl_name))), 
         "database table does not have the required columns")
  
  table_to_append <- table_to_append %>% ungroup() %>% select(list_of_columns)
  
  sql_list_of_columns <- paste0(escape(ident(list_of_columns), con = db_connection), collapse = ", ")
  
  query <- paste0("INSERT INTO ", schema,".",tbl_name,
                  "(",sql(sql_list_of_columns),")",
                  "\n  ",sql_render(table_to_append))
  
  # print(query)
  save_to_sql(query, "append_table")
  result <- dbExecute(db_connection, as.character(query))
}

#' Create new table in the database
#' 
#' Creates an empty table in the database, overwriting an existing copy if
#' instructed. Main use is to setup an empyt table that results can be iteratively
#' appended to.
#' 
#' Not necessary if creating a table directly from an existing table.
#' Use write_to_database for this.
#' 
#' named_list_of_columns takes the format name = "sql type"
#' For example:
#' list_of_columns <- (number = "[int] NOT NULL",
#'                     date = "[date] NOT NULL",
#'                     character = "[varchar](25) NULL")
#' 
create_table <- function(db_connection, schema, tbl_name, named_list_of_columns, OVERWRITE = FALSE){
  no_special_characters(schema)
  no_special_characters(tbl_name)
  
  # remove table if it exists
  if(OVERWRITE) delete_table(db_connection, schema, tbl_name)
  
  # setup queries
  dbExecute(db_connection, as.character(build_sql(con = db_connection, "SET ANSI_NULLS ON")))
  dbExecute(db_connection, as.character(build_sql(con = db_connection, "SET QUOTED_IDENTIFIER ON")))
  dbExecute(db_connection, as.character(build_sql(con = db_connection, "SET ANSI_PADDING ON")))
  
  # main SQL query
  sql_query <- build_sql(con = db_connection
                         ,"CREATE TABLE ", sql(schema), ".", sql(tbl_name),"(","\n"
                         ,sql(paste0("[",names(named_list_of_columns),"] "
                                     , named_list_of_columns, collapse = ",\n")),"\n"
                         ,") ON [PRIMARY]"
  )
  
  # run query
  save_to_sql(sql_query, "create_table")
  result <- dbExecute(db_connection, as.character(sql_query))
  # post queries
  dbExecute(db_connection, as.character(build_sql(con = db_connection, "SET ANSI_PADDING OFF")))
  return(result)
}

#' Delete all tables in the schema with the given prefix.
#'  
#' For (sub)projects we favour a naming convention where using naming prefixes.
#' E.g. prj_table1, prj_table2, prj_tmp_table3
#' This function permits the deleting of all tables with common name prefix.
#' Intended for use deleting temporary tables
#' E.g. prefix = "tmp_" would delete tmp_table1, tmp_table2, tmp_tmp_table but not my_table
#' 
#' Use with caution. 
#'
purge_tables_by_prefix <- function(db_connection, schema, tbl_prefix, mode = "table", exclude = NA){
  no_special_characters(schema)
  assert(nchar(tbl_prefix) >= 1, "zero length prefix blocked to prevent accidental delete all")
  
  # get all tables in database
  all_tables <- dbListTables(db_connection)
  # keep only with prefix
  tables_to_drop <- all_tables[grepl(paste0('^',tbl_prefix), all_tables)]
  # exclude if exists
  if(!all(is.na(exclude)))
    tables_to_drop <- tables_to_drop[tables_to_drop %not_in% exclude]
  
  for(tbl_name in tables_to_drop)
    delete_table(db_connection, schema, tbl_name, mode)
}

#' Save SQL queries to files
#' 
#' All the SQL queries that write or change data on the server (but not those that
#' only fetch data) in these utility functions save an SQL code file of the command
#' that was executed.
#' 
#' These scripts are primarily intended to support debugging.
#' They can be deleted without concern.
#'
save_to_sql <- function(query, desc){
  
  if(!dir.exists("./SQL tmp scripts"))
    dir.create("./SQL tmp scripts")
  
  clean_name <- gsub("[. :]","_",desc)
  # clean_time <- gsub("[. :]","_",Sys.time())
  clean_time <- gsub("[.:]","-",format(Sys.time(), "%Y-%m-%d %H%M%OS3")) # includes milliseconds now
  
  file_name <- paste0("./SQL tmp scripts/",clean_time," ",clean_name,".sql")
  
  if(file.exists(file_name)){
    Sys.sleep(1)
    clean_time <- gsub("[. :]","_",Sys.time())
    file_name <- paste0("./SQL tmp scripts/",clean_time," ",clean_name,".sql")
  }
  
  writeLines(as.character(query), file_name)
}

#' Create view
#' Returning connection to the new view
#' 
#' The view equivalent of write_to_database. Given a table from a database
#' connection, defines a new view with this definition.
#' 
#' The original table must come from an SQL connection.
#' Can only create views if the input connection is to the database containing
#' the views.
#' 
create_view <- function(tbl_name, db_connection, schema, view_name, OVERWRITE = FALSE){
  no_special_characters(schema)
  no_special_characters(view_name)
  # checks
  assert("tbl_sql" %in% class(tbl_name), "input table must originate from sql connection")
  
  # remove view if it exists
  if(OVERWRITE) delete_table(db_connection, schema, view_name, mode = "view")
  
  # remove database name from schema
  just_schema <- strsplit(schema,"\\.")[[1]][2]
  
  # SQL query
  sql_query <- build_sql(con = db_connection
                         ,"CREATE VIEW "
                         ,sql(just_schema),".",escape(ident(view_name), con = db_connection)
                         ," AS \n"
                         ,sql_render(tbl_name)," \n"
  )
  
  # run query
  save_to_sql(sql_query, "create_view")
  result <- dbExecute(db_connection, as.character(sql_query))
  
  # load and return new table
  create_access_point(db_connection, schema, view_name)
}

#' Copy R table to SQL
#' 
#' The inbuilt dbplyr copy_to function does not appear to work in our set-up.
#' Worse, it caused errors/locking, preventing other users who are using the
#' same connection method until their R session is restarted.
#' 
#' Hence we implement a established work around using the DBI package.
#' This is a revision of the original function (in previous versions),
#' and uses different functionality to simplify the process.
#' 
copy_r_to_sql <- function(db_connection, schema, sql_table_name, r_table_name, OVERWRITE = FALSE){
  no_special_characters(schema)
  no_special_characters(sql_table_name)
  
  # remove if overwrite
  if(OVERWRITE) delete_table(db_connection, schema, sql_table_name)
  
  suppressMessages( # mutes translation message
    DBI::dbWriteTable(db_connection,
                      DBI::SQL(paste0(schema, ".", sql_table_name)),
                      r_table_name)
  )
  
  r_table_name <- create_access_point(db_connection, schema, sql_table_name)
}

#' Compress a table
#' 
#' Large SQL tables can be compressed to reduce the space they take up.
#' 
compress_table <- function(db_connection, schema, tbl_name){
  no_special_characters(schema)
  no_special_characters(tbl_name)
  
  # SQL query
  sql_query <- build_sql(con = db_connection
                         ,"ALTER TABLE "
                         ,sql(schema),".",escape(ident(tbl_name), con = db_connection)
                         ," REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)"
  )
  
  # run query
  save_to_sql(sql_query, "compress_table")
  result <- dbExecute(db_connection, as.character(sql_query))
}
