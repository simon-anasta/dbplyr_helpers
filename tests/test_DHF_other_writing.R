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

#' Testing the following functions that do other types of writting
#' 
#' write_to_database(input_tbl, db_connection, schema, tbl_name, OVERWRITE = FALSE)
#' create_table(db_connection, schema, tbl_name, named_list_of_columns, OVERWRITE = FALSE)
#' create_view(tbl_name, db_connection, schema, view_name, OVERWRITE = FALSE)
#' append_database_table(db_connection, schema, tbl_name, list_of_columns, table_to_append)
#' 
context("other writings")

## setup ----

our_sandpit = "[IDI_Sandpit]"
our_schema = "[DL-MAA2016-15]"
our_usercode = "[IDI_UserCode]"
our_views = "[DL-MAA2016-15]"
table_name = paste0("test_tbl",floor(runif(1,1000000,9999999)))
view_name = paste0("test_view",floor(runif(1,1000000,9999999)))
data(cars)

## tests ----

test_that("sql accepts creation and appending",{
  # arrange
  table_name1 = paste0("test_tbl",floor(runif(1,1000000,9999999)))
  table_name2 = paste0("test_tbl",floor(runif(1,1000000,9999999)))
  named_list_of_columns = list(number = "[int] NOT NULL", date = "[date] NOT NULL", character = "[varchar](25) NULL")
  table_data = data.frame(number = c(1,2,3),
                          date = c('2000-02-29', '2001-01-01', '2003-12-31'),
                          character = c("a","b","c"), stringsAsFactors = FALSE)
  # act - blank table
  db_con = create_database_connection(database = "IDI_Sandpit")
  create_table(db_con, our_sandpit, our_schema, add_delimiters(table_name1, "[]"), named_list_of_columns)
  new_table = create_access_point(db_con, our_sandpit, our_schema, add_delimiters(table_name1, "[]"))
  table_created_in_sql = table_name1 %in% DBI::dbListTables(db_con)
  new_table_row_count = new_table %>% summarise(num = n()) %>% collect() %>% unlist(use.names = FALSE)
  # act - append
  remote_table = copy_r_to_sql(db_con, our_sandpit, our_schema, add_delimiters(table_name2, "[]"), table_data)
  remote_table_row_count = remote_table %>% summarise(num = n()) %>% collect() %>% unlist(use.names = FALSE)
  append_database_table(db_con, our_sandpit, our_schema, add_delimiters(table_name1, "[]"), colnames(table_data), remote_table)
  appended_table_row_count = new_table %>% summarise(num = n()) %>% collect() %>% unlist(use.names = FALSE)
  # act - delete & tidy up
  delete_table(db_con, our_sandpit, our_schema, add_delimiters(table_name1, "[]"), mode = "table")
  table1_deleted_from_sql = table_name1 %not_in% DBI::dbListTables(db_con)
  delete_table(db_con, our_sandpit, our_schema, add_delimiters(table_name2, "[]"), mode = "table")
  table2_deleted_from_sql = table_name2 %not_in% DBI::dbListTables(db_con)
  close_database_connection(db_con)
  # assert
  expect_true(table_created_in_sql)
  expect_equal(new_table_row_count, 0)
  expect_equal(appended_table_row_count, nrow(table_data))
  expect_equal(remote_table_row_count, nrow(table_data))
  expect_true(table1_deleted_from_sql)
  expect_true(table2_deleted_from_sql)
})

test_that("new tables can be written",{
  copied_table_name = paste0("test_tbl",floor(runif(1,1000000,9999999)))
  written_table_name = paste0("test_tbl",floor(runif(1,1000000,9999999)))
  # act - copy in
  db_con = create_database_connection(database = "IDI_Sandpit")
  copied_table = copy_r_to_sql(db_con, our_sandpit, our_schema, add_delimiters(copied_table_name, "[]"), cars)
  table_copied_to_sql = copied_table_name %in% DBI::dbListTables(db_con)
  # act - rewrite
  written_table = write_to_database(copied_table, db_con, our_sandpit, our_schema, add_delimiters(written_table_name, "[]"))
  table_written_to_sql = written_table_name %in% DBI::dbListTables(db_con)
  # act - delete & tidy up
  delete_table(db_con, our_sandpit, our_schema, add_delimiters(copied_table_name, "[]"), mode = "table")
  copied_deleted_from_sql = copied_table_name %not_in% DBI::dbListTables(db_con)
  delete_table(db_con, our_sandpit, our_schema, add_delimiters(written_table_name, "[]"), mode = "table")
  written_deleted_from_sql = table_written_to_sql %not_in% DBI::dbListTables(db_con)
  close_database_connection(db_con)
  # assert
  expect_true(table_copied_to_sql)
  expect_true(table_written_to_sql)
  expect_true(copied_deleted_from_sql)
  expect_true(written_deleted_from_sql)
})

test_that("views can be created and deleted",{
  # act - view
  db_con = create_database_connection(database = "IDI_UserCode")
  test_table = copy_r_to_sql(db_con, our_sandpit, our_schema, add_delimiters(table_name, "[]"), cars)
  test_view = create_view(test_table, db_con, our_usercode, our_views, add_delimiters(view_name, "[]"))
  view_in_sql = view_name %in% DBI::dbListTables(db_con)
  # act - removal
  delete_table(db_con, our_sandpit, our_schema, add_delimiters(table_name, "[]"), mode = "table")
  delete_table(db_con, our_usercode, our_views, add_delimiters(view_name, "[]"), mode = "view")
  view_deleted_from_sql = view_name %not_in% DBI::dbListTables(db_con)
  close_database_connection(db_con)
  # assert
  expect_true(view_in_sql)
  expect_true(view_deleted_from_sql)
})

