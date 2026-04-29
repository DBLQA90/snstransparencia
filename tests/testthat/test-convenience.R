test_that("sns_client creates configured clients", {
  client <- sns_client(use_cache = TRUE, cache_dir = tempdir(),
                       user_agent = "snstransparencia-tests")

  expect_s3_class(client, "SNSClient")
  expect_true(client$use_cache)
  expect_equal(client$cache_dir, tempdir())
  expect_equal(client$user_agent, "snstransparencia-tests")
})

test_that("convenience wrappers forward to client methods", {
  client <- sns_client()
  seen_records <- FALSE

  mock <- function(req) {
    if (grepl("/records", req$url)) {
      seen_records <<- TRUE
      return(json_response(list(
        total_count = 1,
        results = list(list(ano = "2026-01", cannabis = 87))
      )))
    }
    json_response(metadata_payload(records_count = 1))
  }

  out <- httr2::with_mocked_responses(
    mock,
    sns_query_records(
      "substancias",
      columns = c("ano", "cannabis"),
      limit = 1,
      .client = client
    )
  )

  expect_true(seen_records)
  expect_equal(out$ano, "2026-01")
  expect_equal(out$cannabis, 87)
})

test_that("catalogue convenience wrappers use supplied clients", {
  cache_dir <- tempfile("sns_cache_")
  client <- sns_client(cache_dir = cache_dir)
  on.exit(unlink(cache_dir, recursive = TRUE), add = TRUE)

  mock <- function(req) {
    json_response(list(
      total_count = 1,
      results = list(
        list(dataset_id = "substancias", title = "Substancias",
             description = "Monitorizar substancias.", publisher = "ICAD")
      )
    ))
  }

  catalogue <- httr2::with_mocked_responses(
    mock,
    sns_cache_catalogue(page_size = 1, .client = client)
  )
  search <- sns_search_cached_catalogue("substancias", .client = client)

  expect_equal(catalogue$dataset_id, "substancias")
  expect_equal(search$dataset_id, "substancias")
  expect_equal(nrow(sns_list_cached_catalogues(.client = client)), 1)
})

test_that("convenience wrappers validate .client", {
  expect_error(
    sns_list_datasets(.client = list()),
    class = "snstransparencia_error"
  )
})
