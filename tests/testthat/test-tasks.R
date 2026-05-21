# Integration tests for resetAiFeedback
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
  task_id  <- Sys.getenv("TEST_AI_FEEDBACK_REMOVAL_TASK_ID")
  if (conn_str == "" || task_id == "") {
    skip("MONGO_CONN_STR or TEST_TASK_ID not set in .env")
  }
  list(conn_str = conn_str, task_id = task_id)
}

test_that("resetAiFeedback returns NULL invisibly", {
  env <- skip_if_no_env()

  result <- resetAiFeedback(env$task_id, env$conn_str)

  expect_null(result)
})

test_that("resetAiFeedback removes aiReports from candidates", {
  env <- skip_if_no_env()

  resetAiFeedback(env$task_id, env$conn_str)

  candidates <- mongolite::mongo("candidates", url = env$conn_str)
  remaining <- candidates$count(
    paste0('{"localTask":"', env$task_id, '","aiReports":{"$exists":true}}')
  )

  expect_equal(remaining, 0)
})

test_that("resetAiFeedback removes schoolSummary and teacherGroupSummaries from task", {
  env <- skip_if_no_env()

  resetAiFeedback(env$task_id, env$conn_str)

  tasks <- mongolite::mongo("tasks", url = env$conn_str)
  task <- tasks$find(
    query  = paste0('{"_id":"', env$task_id, '"}'),
    fields = '{"schoolSummary":true,"teacherGroupSummaries":true}'
  )

  expect_false("schoolSummary" %in% names(task))
  expect_false("teacherGroupSummaries" %in% names(task))
})

test_that("resetAiFeedback does not error for unknown task id", {
  env <- skip_if_no_env()

  expect_no_error(
    resetAiFeedback("00000000-0000-0000-0000-000000000000", env$conn_str)
  )
})
