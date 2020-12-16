###############################################################################
#' Description: Automated test suite for helper functions
#' Outputs automated test results
#'
#' Author: Simon Anastasiadis
#'
#' Notes:
#'  - Working directory must contain this file, folder of test scripts, and the code files to test
#'  - Sourcing the file runs all scripts (2 minutes), individual tests scripts can be run in order line by line
#'  - Uses code folding by headers (Alt + O to collapse all)
#'
#' #############################################################################

## setup ------------------------------------------------------------------------------------------

# working directory
PATH_TO_SCRIPTS <- "~/Path/To/Folder/Containing/R/Files"

# schema for storing user created table
our_schema <- "[]"
# database for storing user created tables
our_db <- "[]"
# database for creating use created views
our_usercode <- "[]"

setwd(PATH_TO_SCRIPTS)

## test everything --------------------------------------------------------------------------------
#
# For quick runs of all tests when checking that nothing has broken

source("utility_functions.R")
source("table_consistency_checks.R")
source("dbplyr_helper_functions.R")
testthat::test_dir("./tests")

## test in sections -------------------------------------------------------------------------------
#
# runs each test file in the recommended order
# (later tests depend on functionality from earlier tests)
#
# Intended for stepping through code, does not run on source

if (FALSE) {
  # test utility_functions.R
  source("utility_functions.R")
  testthat::test_file("./tests/test_UF_independent_functions.R")

  # test table consistency checkers
  source("table_consistency_checks.R")
  testthat::test_file("./tests/test_TCC_base_and_size.R")
  testthat::test_file("./tests/test_TCC_uniques_and_joins.R")
  testthat::test_file("./tests/test_TCC_missings_and_overlaps.R")

  # test dbplyr helpers
  #
  # excludes tests of the functions: create_clustered_index, write_for_reuse
  # purge_tables_by_prefix, and compress_table
  source("dbplyr_helper_functions.R")
  testthat::test_file("./tests/test_DHF_independent_functions.R")
  testthat::test_file("./tests/test_DHF_connect_read_write.R")
  testthat::test_file("./tests/test_DHF_other_writing.R")
  testthat::test_file("./tests/test_DHF_manipulations.R")
}
