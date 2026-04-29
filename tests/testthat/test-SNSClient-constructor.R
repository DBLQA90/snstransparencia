test_that("constructor uses sensible defaults", {
  client <- SNSClient$new()

  expect_s3_class(client, "SNSClient")
  expect_equal(client$lang, "pt")
  expect_equal(client$timeout, 60)
  expect_match(client$base_url, "/api/explore/v2.1$")
  expect_null(client$user_agent)
  expect_false(client$use_cache)
  expect_null(client$cache_dir)
})

test_that("active bindings validate values", {
  client <- SNSClient$new()

  expect_error(client$lang <- "fr")
  expect_error(client$timeout <- 0)
  expect_error(client$base_url <- "")
  expect_error(client$user_agent <- "")
  expect_error(client$use_cache <- "yes")
  expect_error(client$cache_dir <- "")

  client$lang <- "EN"
  client$timeout <- 10
  client$base_url <- "https://example.com/api/explore/v2.1"
  client$user_agent <- "snstransparencia-tests"
  client$use_cache <- TRUE
  client$cache_dir <- tempdir()

  expect_equal(client$lang, "en")
  expect_equal(client$timeout, 10)
  expect_equal(client$base_url, "https://example.com/api/explore/v2.1")
  expect_equal(client$user_agent, "snstransparencia-tests")
  expect_true(client$use_cache)
  expect_equal(client$cache_dir, tempdir())
})

test_that("print returns self invisibly", {
  client <- SNSClient$new()
  expect_output(result <- client$print(), "<SNSClient>")
  expect_identical(result, client)
})
