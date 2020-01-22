###############################################################################
#' Description: Automated tests for dbplyr helper functions.
#'
#' Input: dbplyr_helper_functions.R
#'
#' Output: Test pass/fail report
#' 
#' Author: Simon Anastasiadis
#' 
#' Dependencies: testthat package, utility_functions.R
#' 
#' Notes:
#' - Some functions (and tests) assume SQL Server environment
#'   due to differences in syntax between different implementations of SQL.
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2020-01-06 SA v1
#' 2019-11-20 SA v0
#'#############################################################################

#' Testing the following functions that handle basic SQL read/write
#' 
#' set_connection_string(server,database,port = NA)
#' create_database_connection(..., server = NA, database = NA, port = NA)
#' create_access_point(db_connection, schema, tbl_name)
#' copy_r_to_sql(db_connection, schema, sql_table_name, r_table_name, OVERWRITE = FALSE)
#' delete_table(db_connection, schema, tbl_name, mode = "table")
#' close_database_connection(db_connection)
#'
context("connect write and read")

test_that("connection can see tables in database",{
  
  db_con = create_database_connection(database = "IDI_Sandpit")
  tables_in_db = DBI::dbListTables(db_con)
  expect_true(length(tables_in_db) > 0)
  
  close_database_connection(db_con)
  expect_error(DBI::dbListTables(db_con))
})

test_that("dbplyr writes, reads, and deletes",{
  # Arrange
  our_db = "[IDI_Sandpit]"
  our_schema = "[DL-MAA2016-15]"
  table_name = paste0("[test",floor(runif(1,1000000,9999999)),"]")
  data(cars)
  
  # Act
  db_con = create_database_connection(database = "IDI_Sandpit")
  remote_table = copy_r_to_sql(db_con, our_db, our_schema, table_name, cars)
  table_written_to_sql = remove_delimiters(table_name, "[]") %in% DBI::dbListTables(db_con)
  
  local_table = collect(create_access_point(db_con, our_db, our_schema, table_name))
  origin_and_read_table_identical = all_equal(cars, local_table, ignore_row_order = TRUE)
  
  delete_table(db_con, our_db, our_schema, table_name, mode = "table")
  table_deleted_from_sql = remove_delimiters(table_name, "[]") %not_in% DBI::dbListTables(db_con)
  close_database_connection(db_con)
  
  # Assert
  expect_true(table_written_to_sql)
  expect_true(origin_and_read_table_identical)
  expect_true(table_deleted_from_sql)
})









