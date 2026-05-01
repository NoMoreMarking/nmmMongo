# Integration tests for getTranscripts
# Requires a .env file in the project root with:
#   MONGO_CONN_STR=mongodb://...
#   TEST_TASK_ID=...

skip_if_no_env_transcripts <- function() {
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

test_that("getTranscripts returns a non-empty data frame for a known task ID", {
  env <- skip_if_no_env_transcripts()

  result <- getTranscripts(env$task_id, env$conn_str)

  expect_s3_class(result, "data.frame")
  expect_gt(nrow(result), 0)
  expect_true("id" %in% names(result))
  expect_false("_id" %in% names(result))
})

test_that("getTranscripts accepts a vector of task IDs", {
  env <- skip_if_no_env_transcripts()

  result <- getTranscripts(c(env$task_id, env$task_id), env$conn_str)

  expect_s3_class(result, "data.frame")
  expect_gt(nrow(result), 0)
})

test_that("getTranscripts returns empty data frame for unknown task ID", {
  env <- skip_if_no_env_transcripts()

  result <- getTranscripts("00000000-0000-0000-0000-000000000000", env$conn_str)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
})
