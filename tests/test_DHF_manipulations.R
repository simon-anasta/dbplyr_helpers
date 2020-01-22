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

our_db = "[IDI_Sandpit]"
our_schema = "[DL-MAA2016-15]"

#' Testing the following functions that manipulate tables
#' 
#' union_all(table_a,table_b, list_of_columns)
#' pivot_table(input_tbl, label_column, value_column, aggregator = "SUM")
#' 
context("manipulations")

test_that("union row-binds",{
  # arrange
  head_table_name = add_delimiters(paste0("test",floor(runif(1,1000000,9999999))), "[]")
  tail_table_name = add_delimiters(paste0("test",floor(runif(1,1000000,9999999))), "[]")
  data(cars)
  head_cars = head(cars)
  tail_cars = tail(cars)
  # act - load and union
  db_con = create_database_connection(database = "IDI_Sandpit")
  head_table = copy_r_to_sql(db_con, our_db, our_schema, head_table_name, head_cars)
  tail_table = copy_r_to_sql(db_con, our_db, our_schema, tail_table_name, tail_cars)
  unioned_table = union_all(head_table, tail_table, colnames(cars)) %>% collect()
  rowbound_table = rbind(head_cars, tail_cars)
  # act - delete & tidy up
  delete_table(db_con, our_db, our_schema, head_table_name, mode = "table")
  delete_table(db_con, our_db, our_schema, tail_table_name, mode = "table")
  close_database_connection(db_con)
  # assert
  expect_true(all_equal(unioned_table, rowbound_table, ignore_row_order = TRUE))
})

test_that("pivot replicates tidyr::spread",{
  # arrange
  table_name = add_delimiters(paste0("test",floor(runif(1,1000000,9999999))), "[]")
  in_data_table = data.frame(people = c("bob", "alice", "bob", "alice"),
                             labels = c("age","age", "height", "height"),
                             values = c(10,12,150,160),
                             stringsAsFactors = FALSE)
  out_data_table = data.frame(people = c("bob", "alice"),
                              age = c(10,12),
                              height = c(150,160),
                              stringsAsFactors = FALSE)
  # act - load and pivot
  db_con = create_database_connection(database = "IDI_Sandpit")
  sql_table = copy_r_to_sql(db_con, our_db, our_schema, table_name, in_data_table)
  pivoted_table = sql_table %>%
    pivot_table(label_column = "labels", value_column = "values", aggregator = "SUM") %>%
    collect()
  spread_table = tidyr::spread(in_data_table, labels, values)
  delete_table(db_con, our_db, our_schema, table_name)
  close_database_connection(db_con)
  # assert
  expect_true(all_equal(pivoted_table, out_data_table, ignore_row_order = TRUE, ignore_col_order = TRUE))
  expect_true(all_equal(pivoted_table, spread_table, ignore_row_order = TRUE, ignore_col_order = TRUE))
})

