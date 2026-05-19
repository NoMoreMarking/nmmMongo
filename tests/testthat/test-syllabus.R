# Integration tests for syllabus functions
# Requires a .env file in the project root with:
#   MONGO_CONN_STR=mongodb://...
#   TEST_SYLLABUS_ID=...
#   TEST_SYLLABUS_NAME=...
#   TEST_PRODUCT_ID=...

skip_if_no_env_syllabus <- function() {
  env_file <- here::here(".env")
  if (!file.exists(env_file)) {
    skip("No .env file found — skipping integration tests")
  }
  dotenv::load_dot_env(env_file)
  conn_str      <- Sys.getenv("MONGO_CONN_STR")
  syllabus_id   <- Sys.getenv("TEST_SYLLABUS_ID")
  syllabus_name <- Sys.getenv("TEST_SYLLABUS_NAME")
  product_id    <- Sys.getenv("TEST_PRODUCT_ID")
  if (conn_str == "" || syllabus_id == "") {
    skip("MONGO_CONN_STR or TEST_SYLLABUS_ID not set in .env")
  }
  list(
    conn_str      = conn_str,
    syllabus_id   = syllabus_id,
    syllabus_name = syllabus_name,
    product_id    = product_id
  )
}

# ── getSyllabusById ────────────────────────────────────────────────────────────

test_that("getSyllabusById returns a data frame for a known id", {
  env <- skip_if_no_env_syllabus()

  result <- getSyllabusById(env$syllabus_id, env$conn_str)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 1)
  expect_true("name" %in% names(result))
})

test_that("getSyllabusById returns empty data frame for unknown id", {
  env <- skip_if_no_env_syllabus()

  result <- getSyllabusById("00000000-0000-0000-0000-000000000000", env$conn_str)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
})

# ── getSyllabusByName ──────────────────────────────────────────────────────────

test_that("getSyllabusByName returns a data frame for a known name", {
  env <- skip_if_no_env_syllabus()
  if (env$syllabus_name == "") skip("TEST_SYLLABUS_NAME not set in .env")

  result <- getSyllabusByName(env$syllabus_name, env$conn_str)

  expect_s3_class(result, "data.frame")
  expect_gte(nrow(result), 1)
  expect_true("name" %in% names(result))
})

test_that("getSyllabusByName returns empty data frame for unknown name", {
  env <- skip_if_no_env_syllabus()

  result <- getSyllabusByName("__nonexistent__", env$conn_str)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
})

# ── getSyllabusByProduct ───────────────────────────────────────────────────────

test_that("getSyllabusByProduct returns a data frame for a known product", {
  env <- skip_if_no_env_syllabus()
  if (env$product_id == "") skip("TEST_PRODUCT_ID not set in .env")

  result <- getSyllabusByProduct(env$product_id, env$conn_str)

  expect_s3_class(result, "data.frame")
  expect_gte(nrow(result), 1)
  expect_true("product" %in% names(result))
})

test_that("getSyllabusByProduct returns empty data frame for unknown product", {
  env <- skip_if_no_env_syllabus()

  result <- getSyllabusByProduct("00000000-0000-0000-0000-000000000000", env$conn_str)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
})

# ── setSyllabusReadyForJudging ─────────────────────────────────────────────────

test_that("setSyllabusReadyForJudging sets ready and returns a modification result", {
  env <- skip_if_no_env_syllabus()

  result <- setSyllabusReadyForJudging(env$syllabus_id, TRUE, env$conn_str)

  expect_gte(result$matchedCount, 1L)
})

test_that("setSyllabusReadyForJudging unsets ready and returns a modification result", {
  env <- skip_if_no_env_syllabus()

  result <- setSyllabusReadyForJudging(env$syllabus_id, FALSE, env$conn_str)

  expect_gte(result$matchedCount, 1L)
})

# ── setSyllabusFilePrompts ─────────────────────────────────────────────────────

test_that("setSyllabusFilePrompts returns a modification result", {
  env <- skip_if_no_env_syllabus()

  result <- setSyllabusFilePrompts(
    syllabus = env$syllabus_id,
    path     = "test/test-prompt.txt",
    connStr  = env$conn_str
  )

  expect_gte(result$matchedCount, 1L)
})

test_that("setSyllabusFilePrompts writes correct paths to the document", {
  env <- skip_if_no_env_syllabus()

  path <- "test/test-prompt.txt"

  setSyllabusFilePrompts(env$syllabus_id, path, env$conn_str)

  syllabusCollection <- mongolite::mongo(
    db         = "nmm-vegas-db",
    collection = "syllabus",
    url        = env$conn_str
  )
  doc <- syllabusCollection$find(
    query  = paste0('{"_id":"', env$syllabus_id, '"}'),
    fields = '{"filePrompt_judging":1,"filePrompt_studentJudgeSumm":1,"filePrompt_studentAssessTab":1,"filePrompt_teacherAssessTab":1}'
  )

  expect_equal(doc$filePrompt_judging,          paste0("judging/",          path))
  expect_equal(doc$filePrompt_studentJudgeSumm, paste0("studentJudgeSumm/", path))
  expect_equal(doc$filePrompt_studentAssessTab, paste0("studentAssessTab/", path))
  expect_equal(doc$filePrompt_teacherAssessTab, paste0("teacherAssessTab/", path))
})

test_that("setSyllabusFilePrompts returns matchedCount of 0 for unknown syllabus", {
  env <- skip_if_no_env_syllabus()

  result <- setSyllabusFilePrompts(
    syllabus = "00000000-0000-0000-0000-000000000000",
    path     = "test/test-prompt.txt",
    connStr  = env$conn_str
  )

  expect_equal(result$matchedCount, 0L)
})
