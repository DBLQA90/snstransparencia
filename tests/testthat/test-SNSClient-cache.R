test_that("download_data writes export and manifest", {
  cache_dir <- tempfile("sns_cache_")
  client <- SNSClient$new(cache_dir = cache_dir)
  on.exit(unlink(cache_dir, recursive = TRUE), add = TRUE)

  seen_export_url <- NULL
  mock <- function(req) {
    if (grepl("/exports/csv", req$url)) {
      seen_export_url <<- utils::URLdecode(req$url)
      return(csv_response("\ufeffano;cannabis\n2026-01;87\n"))
    }
    json_response(metadata_payload(records_count = 1))
  }

  manifest <- httr2::with_mocked_responses(mock, client$download_data(
    "substancias",
    columns = c("ano", "cannabis"),
    filters = list(ano = "2026"),
    date_field = "ano",
    date_from = "2026-01"
  ))

  expect_true(file.exists(manifest$path))
  expect_true(file.exists(file.path(dirname(manifest$path), "manifest.json")))
  expect_equal(manifest$dataset_id, "substancias")
  expect_equal(manifest$format, "csv")
  expect_match(seen_export_url, "/exports/csv", fixed = TRUE)
  expect_match(seen_export_url, "select=ano,cannabis", fixed = TRUE)
  expect_match(seen_export_url, "refine=ano:2026", fixed = TRUE)
  expect_match(seen_export_url, "where=ano >= date'2026-01'", fixed = TRUE)
})

test_that("load_data reads cached CSV exports", {
  cache_dir <- tempfile("sns_cache_")
  client <- SNSClient$new(cache_dir = cache_dir)
  on.exit(unlink(cache_dir, recursive = TRUE), add = TRUE)

  mock <- function(req) {
    if (grepl("/exports/csv", req$url)) {
      return(csv_response("\ufeffano;cannabis\n2026-01;87\n2026-02;170\n"))
    }
    json_response(metadata_payload(records_count = 2))
  }

  manifest <- httr2::with_mocked_responses(mock, client$download_data("substancias"))
  out <- client$load_data("substancias", cache_key = manifest$cache_key)

  expect_s3_class(out, "data.frame")
  expect_equal(out$ano, c("2026-01", "2026-02"))
  expect_equal(out$cannabis, c(87L, 170L))
  expect_true(file.exists(manifest$rds_path))
})

test_that("load_data reads cached JSONL exports", {
  cache_dir <- tempfile("sns_cache_")
  client <- SNSClient$new(cache_dir = cache_dir)
  on.exit(unlink(cache_dir, recursive = TRUE), add = TRUE)

  mock <- function(req) {
    if (grepl("/exports/jsonl", req$url)) {
      return(csv_response(
        paste0(
          "{\"ano\":\"2026-01\",\"cannabis\":87}\n",
          "{\"ano\":\"2026-02\",\"cannabis\":170}\n"
        )
      ))
    }
    json_response(metadata_payload(records_count = 2))
  }

  manifest <- httr2::with_mocked_responses(
    mock,
    client$download_data("substancias", format = "jsonl")
  )
  out <- client$load_data("substancias", cache_key = manifest$cache_key)

  expect_s3_class(out, "data.frame")
  expect_equal(out$ano, c("2026-01", "2026-02"))
  expect_equal(out$cannabis, c(87L, 170L))
})

test_that("get_data respects use_cache", {
  cache_dir <- tempfile("sns_cache_")
  client <- SNSClient$new(cache_dir = cache_dir, use_cache = TRUE)
  on.exit(unlink(cache_dir, recursive = TRUE), add = TRUE)

  calls <- 0L
  mock <- function(req) {
    calls <<- calls + 1L
    if (grepl("/exports/csv", req$url)) {
      return(csv_response("\ufeffano;cannabis\n2026-01;87\n"))
    }
    json_response(metadata_payload(records_count = 1))
  }

  first <- httr2::with_mocked_responses(mock, client$get_data("substancias"))
  second <- client$get_data("substancias")

  expect_equal(first, second)
  expect_equal(calls, 2L) # metadata + export on first call; none on second
})

test_that("get_data without cache does not persist exports", {
  cache_dir <- tempfile("sns_cache_")
  client <- SNSClient$new(cache_dir = cache_dir, use_cache = FALSE)
  on.exit(unlink(cache_dir, recursive = TRUE), add = TRUE)

  mock <- function(req) {
    if (grepl("/exports/csv", req$url)) {
      return(csv_response("\ufeffano;cannabis\n2026-01;87\n"))
    }
    json_response(metadata_payload(records_count = 1))
  }

  out <- httr2::with_mocked_responses(mock, client$get_data("substancias"))

  expect_equal(nrow(out), 1)
  expect_equal(nrow(client$list_cached()), 0)
})

test_that("is_updated compares cached and live data_processed timestamps", {
  cache_dir <- tempfile("sns_cache_")
  client <- SNSClient$new(cache_dir = cache_dir)
  on.exit(unlink(cache_dir, recursive = TRUE), add = TRUE)

  old_mock <- function(req) {
    if (grepl("/exports/csv", req$url)) {
      return(csv_response("\ufeffano;cannabis\n2026-01;87\n"))
    }
    json_response(metadata_payload(data_processed = "2026-01-01T00:00:00+00:00"))
  }
  manifest <- httr2::with_mocked_responses(old_mock, client$download_data("substancias"))

  new_mock <- function(req) {
    json_response(metadata_payload(data_processed = "2026-02-01T00:00:00+00:00"))
  }
  unchanged_mock <- function(req) {
    json_response(metadata_payload(data_processed = "2026-01-01T00:00:00+00:00"))
  }

  expect_true(httr2::with_mocked_responses(
    new_mock,
    client$is_updated("substancias", cache_key = manifest$cache_key)
  ))
  expect_false(httr2::with_mocked_responses(
    unchanged_mock,
    client$is_updated("substancias", cache_key = manifest$cache_key)
  ))
})

test_that("list_cached and clear_cache report cache state", {
  cache_dir <- tempfile("sns_cache_")
  client <- SNSClient$new(cache_dir = cache_dir)
  on.exit(unlink(cache_dir, recursive = TRUE), add = TRUE)

  mock <- function(req) {
    if (grepl("/exports/csv", req$url)) {
      return(csv_response("\ufeffano;cannabis\n2026-01;87\n"))
    }
    json_response(metadata_payload(records_count = 1))
  }

  httr2::with_mocked_responses(mock, client$download_data("substancias"))
  cached <- client$list_cached()

  expect_equal(nrow(cached), 1)
  expect_equal(cached$dataset_id, "substancias")
  expect_true(file.exists(cached$path))
  expect_true(client$clear_cache("substancias"))
  expect_equal(nrow(client$list_cached()), 0)
})

test_that("catalogue cache can be loaded, searched, listed, and cleared", {
  cache_dir <- tempfile("sns_cache_")
  client <- SNSClient$new(cache_dir = cache_dir)
  on.exit(unlink(cache_dir, recursive = TRUE), add = TRUE)

  calls <- 0L
  mock <- function(req) {
    calls <<- calls + 1L
    json_response(list(
      total_count = 2,
      results = list(
        list(dataset_id = "substancias", title = "Substancias",
             description = "Monitorizar substancias.", publisher = "ICAD",
             records_count = 122),
        list(dataset_id = "portal-base", title = "Contratos",
             description = "Contratos publicos.", publisher = "IMPIC",
             records_count = 326428)
      )
    ))
  }

  catalogue <- httr2::with_mocked_responses(
    mock,
    client$cache_catalogue(page_size = 2)
  )
  expect_message(
    cached_again <- client$cache_catalogue(page_size = 2),
    "Using cached catalogue"
  )
  cached_catalogues <- client$list_cached_catalogues()
  loaded <- client$load_catalogue()
  search <- client$search_cached_catalogue("substancias")

  expect_equal(calls, 1L)
  expect_equal(catalogue, cached_again)
  expect_equal(loaded, catalogue)
  expect_equal(search$dataset_id, "substancias")
  expect_equal(nrow(cached_catalogues), 1)
  expect_equal(cached_catalogues$rows, 2)
  expect_equal(nrow(client$list_cached()), 0)
  expect_true(client$clear_catalogue_cache(cached_catalogues$cache_key))
  expect_equal(nrow(client$list_cached_catalogues()), 0)
  expect_error(client$load_catalogue(), class = "snstransparencia_error")
})

test_that("load_data errors when cache is absent", {
  client <- SNSClient$new(cache_dir = tempfile("sns_cache_"))
  expect_error(client$load_data("substancias"), class = "snstransparencia_error")
  expect_error(client$is_updated("substancias"), class = "snstransparencia_error")
})
