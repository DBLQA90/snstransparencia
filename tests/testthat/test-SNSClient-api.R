test_that("list_datasets returns catalogue rows", {
  client <- SNSClient$new()

  mock <- function(req) {
    expect_match(req$url, "/catalog/datasets")
    json_response(list(
      total_count = 2,
      results = list(
        list(dataset_id = "substancias", title = "Substancias",
             records_count = 122, modified = "2026-03-16T11:08:26Z",
             publisher = "ICAD"),
        list(dataset_id = "portal-base", title = "Contratos",
             records_count = 326428, modified = "2026-04-13T14:16:30Z",
             publisher = "IMPIC")
      )
    ))
  }

  out <- httr2::with_mocked_responses(mock, client$list_datasets(limit = 2))

  expect_s3_class(out, "data.frame")
  expect_equal(out$dataset_id, c("substancias", "portal-base"))
  expect_equal(out$records_count, c(122, 326428))
})

test_that("list_all_datasets paginates catalogue results", {
  client <- SNSClient$new()
  offsets <- integer()

  mock <- function(req) {
    url <- utils::URLdecode(req$url)
    offset <- if (grepl("offset=2", url, fixed = TRUE)) 2L else 0L
    offsets <<- c(offsets, offset)

    if (offset == 0L) {
      return(json_response(list(
        total_count = 3,
        results = list(
          list(dataset_id = "substancias", title = "Substancias"),
          list(dataset_id = "portal-base", title = "Contratos")
        )
      )))
    }

    json_response(list(
      total_count = 3,
      results = list(
        list(dataset_id = "rastreios-oncologicos",
             title = "Rastreios Oncologicos")
      )
    ))
  }

  out <- httr2::with_mocked_responses(
    mock,
    client$list_all_datasets(page_size = 2)
  )

  expect_equal(out$dataset_id, c(
    "substancias", "portal-base", "rastreios-oncologicos"
  ))
  expect_equal(offsets, c(0L, 2L))
})

test_that("search_datasets quotes simple search terms", {
  client <- SNSClient$new()
  seen_url <- NULL

  mock <- function(req) {
    seen_url <<- utils::URLdecode(req$url)
    json_response(list(
      total_count = 1,
      results = list(
        list(dataset_id = "substancias", title = "Substancias")
      )
    ))
  }

  out <- httr2::with_mocked_responses(
    mock,
    client$search_datasets("substancias", limit = 1)
  )

  expect_equal(out$dataset_id, "substancias")
  expect_match(seen_url, 'where="substancias"', fixed = TRUE)
})

test_that("get_metadata and get_fields parse dataset schema", {
  client <- SNSClient$new()

  mock <- function(req) {
    expect_match(req$url, "/catalog/datasets/substancias")
    json_response(metadata_payload())
  }

  metadata <- httr2::with_mocked_responses(mock, client$get_metadata("substancias"))
  fields <- httr2::with_mocked_responses(mock, client$get_fields("substancias"))

  expect_equal(metadata$dataset_id, "substancias")
  expect_equal(fields$name, c("ano", "cannabis"))
  expect_equal(fields$type, c("date", "int"))
  expect_false("annotations" %in% names(fields))
})

test_that("get_dataset_info returns compact metadata", {
  client <- SNSClient$new()

  mock <- function(req) {
    expect_match(req$url, "/catalog/datasets/substancias")
    json_response(metadata_payload(records_count = 122))
  }

  info <- httr2::with_mocked_responses(mock, client$get_dataset_info("substancias"))

  expect_s3_class(info, "data.frame")
  expect_equal(nrow(info), 1)
  expect_equal(info$dataset_id, "substancias")
  expect_equal(info$title, "Substancias")
  expect_equal(info$publisher, "ICAD")
  expect_equal(info$records_count, 122)
  expect_equal(info$description, "Monitorizar substancias.")
  expect_equal(info$accrual_periodicity, "Mensal")
})

test_that("list_export_formats parses export links", {
  client <- SNSClient$new()

  mock <- function(req) {
    expect_match(req$url, "/catalog/datasets/substancias/exports")
    json_response(list(links = list(
      list(rel = "self", href = "https://example.test/exports"),
      list(rel = "csv", href = "https://example.test/exports/csv"),
      list(rel = "json", href = "https://example.test/exports/json"),
      list(rel = "parquet", href = "https://example.test/exports/parquet")
    )))
  }

  formats <- httr2::with_mocked_responses(mock, client$list_export_formats("substancias"))

  expect_s3_class(formats, "data.frame")
  expect_equal(formats$format, c("csv", "json", "parquet"))
  expect_true(all(grepl("/exports/", formats$href)))
})

test_that("get_facets parses facet values and supports filters", {
  client <- SNSClient$new()
  seen_url <- NULL

  mock <- function(req) {
    seen_url <<- utils::URLdecode(req$url)
    if (!grepl("/facets", req$url)) {
      return(json_response(metadata_payload()))
    }
    json_response(list(facets = list(
      list(name = "ano", facets = list(
        list(name = "2026", value = "2026", count = 2, state = "displayed"),
        list(name = "2025", value = "2025", count = 12, state = "displayed")
      ))
    )))
  }

  facets <- httr2::with_mocked_responses(mock, client$get_facets(
    "substancias",
    facets = "ano",
    filters = list(ano = "2026")
  ))

  expect_s3_class(facets, "data.frame")
  expect_equal(facets$facet, c("ano", "ano"))
  expect_equal(facets$value, c("2026", "2025"))
  expect_equal(facets$count, c(2, 12))
  expect_match(seen_url, 'facet=facet(name="ano")', fixed = TRUE)
  expect_match(seen_url, "refine=ano:2026", fixed = TRUE)
})

test_that("query_records supports columns, filters, and date bounds", {
  client <- SNSClient$new()
  seen_url <- NULL

  mock <- function(req) {
    seen_url <<- utils::URLdecode(req$url)
    if (!grepl("/records", req$url)) {
      return(json_response(metadata_payload()))
    }
    json_response(list(
      total_count = 1,
      results = list(
        list(ano = "2026-01", regiao = "Norte", cannabis = 87)
      )
    ))
  }

  out <- httr2::with_mocked_responses(mock, client$query_records(
    "substancias",
    columns = c("ano", "cannabis"),
    filters = list(ano = "2026"),
    date_field = "ano",
    date_from = "2026-01",
    limit = 10
  ))

  expect_equal(out$ano, "2026-01")
  expect_match(seen_url, "select=ano,cannabis", fixed = TRUE)
  expect_match(seen_url, "refine=ano:2026", fixed = TRUE)
  expect_match(seen_url, "where=ano >= date'2026-01'", fixed = TRUE)
})

test_that("dataset query validation catches unknown fields before querying", {
  client <- SNSClient$new()
  records_requested <- FALSE

  mock <- function(req) {
    if (grepl("/records", req$url)) {
      records_requested <<- TRUE
      return(json_response(list(results = list())))
    }
    json_response(metadata_payload())
  }

  expect_error(
    httr2::with_mocked_responses(
      mock,
      client$query_records("substancias", columns = c("ano", "not_a_field"))
    ),
    "Unknown field",
    class = "snstransparencia_error"
  )
  expect_false(records_requested)
})

test_that("dataset query validation checks date, filter, and facet fields", {
  client <- SNSClient$new()
  mock <- function(req) json_response(metadata_payload())

  expect_error(
    httr2::with_mocked_responses(
      mock,
      client$query_records(
        "substancias",
        columns = "ano",
        date_field = "cannabis",
        date_from = "2026-01"
      )
    ),
    "date or datetime",
    class = "snstransparencia_error"
  )
  expect_error(
    httr2::with_mocked_responses(
      mock,
      client$query_records(
        "substancias",
        columns = "ano",
        filters = list(not_a_field = "x")
      )
    ),
    "Unknown field",
    class = "snstransparencia_error"
  )
  expect_error(
    httr2::with_mocked_responses(
      mock,
      client$get_facets("substancias", facets = "not_a_field")
    ),
    "Unknown field",
    class = "snstransparencia_error"
  )
})

test_that("dataset query validation can be disabled for raw API work", {
  client <- SNSClient$new()
  records_requested <- FALSE

  mock <- function(req) {
    if (grepl("/records", req$url)) {
      records_requested <<- TRUE
      return(json_response(list(
        total_count = 1,
        results = list(list(not_a_field = "raw"))
      )))
    }
    json_response(metadata_payload())
  }

  out <- httr2::with_mocked_responses(
    mock,
    client$query_records(
      "substancias",
      columns = "not_a_field",
      validate = FALSE,
      limit = 1
    )
  )

  expect_true(records_requested)
  expect_equal(out$not_a_field, "raw")
})

test_that("query_records validates API record limits", {
  client <- SNSClient$new()

  expect_error(
    client$query_records("substancias", limit = 101),
    class = "snstransparencia_error"
  )
  expect_error(
    client$query_records("substancias", limit = 100, offset = 10000),
    class = "snstransparencia_error"
  )
  expect_error(
    client$query_records("substancias", columns = "ano", select = "ano"),
    class = "snstransparencia_error"
  )
})

test_that("API errors are surfaced with package class", {
  client <- SNSClient$new()
  mock <- function(req) {
    json_response(list(message = "Bad query"), status = 400)
  }

  expect_error(
    httr2::with_mocked_responses(mock, client$get_metadata("substancias")),
    class = "snstransparencia_api_error"
  )
})
