# Integration tests for getDecisions
# Requires a .env file in the project root with:
#   MONGO_CONN_STR=mongodb://...
#   TEST_TASK_ID=...

skip_if_no_env <- function() {
  env_file <- here::here(".env")
  if (!file.exists(env_file)) {
    skip("No .env file found — skipping integration tests")
  }
  dotenv::load_dot_env(env_file)
  conn_str <- Sys.getenv("MONGO_CONN_STR")
  task_id  <- Sys.getenv("TEST_TASK_ID")
  if (conn_str == "" || task_id == "") {
    skip("MONGO_CONN_STR or TEST_TASK_ID not set in .env")
  }
  list(conn_str = conn_str, task_id = task_id)
}

test_that("getDecisions returns a data frame with expected columns", {
  env <- skip_if_no_env()

  result <- getDecisions(env$task_id, env$conn_str)

  expect_s3_class(result, "data.frame")
  expect_named(result, c(
    "id", "judgeId", "judge",
    "chosenId", "chosen",
    "notChosenId", "notChosen",
    "timeTaken", "isLeft", "isExternal",
    "createdAt", "exclude", "autoAdded"
  ), ignore.order = FALSE)
})

test_that("getDecisions handles missing optional columns without error", {
  env <- skip_if_no_env()

  result <- getDecisions(env$task_id, env$conn_str)

  # These columns may be NA when not present in the data — that is correct behaviour
  expect_true(all(c("isExternal", "exclude") %in% names(result)))
  expect_true(nrow(result) > 0)
})

test_that("getDecisions returns empty data frame for unknown task", {
  env <- skip_if_no_env()

  result <- getDecisions("00000000-0000-0000-0000-000000000000", env$conn_str)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
})
