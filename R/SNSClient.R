#' @title SNS Transparency API Client
#'
#' @description
#' R6 client for the Portuguese SNS Transparency API, powered by the
#' Opendatasoft Explore API v2.1. This first version focuses on dataset
#' discovery, local catalogue caching, metadata inspection, field schemas,
#' metadata-backed validation, small filtered record queries, export downloads,
#' and local caching.
#'
#' sns <- SNSClient$new()
#'
#' if (identical(Sys.getenv("SNSTRANSPARENCIA_RUN_EXAMPLES"), "true")) {
#'   sns$list_datasets(limit = 5)
#'   sns$list_all_datasets(max_records = 100)
#'   sns$cache_catalogue(max_records = 100)
#'   sns$search_cached_catalogue("substancias")
#'   sns$get_fields("substancias")
#'   sns$query_records("substancias", columns = c("ano", "cannabis"), limit = 5)
#' }
#'
#' @importFrom R6 R6Class
#' @export
SNSClient <- R6::R6Class(
  "SNSClient",

  public = list(

    #' @description Create a new SNS Transparency API client.
    #' @param base_url API base URL. Defaults to the SNS Transparency
    #'   Explore API v2.1 endpoint.
    #' @param lang API language, `"pt"` or `"en"`.
    #' @param timeout Request timeout in seconds.
    #' @param user_agent Optional user-agent string.
    #' @param use_cache Whether `get_data()` should reuse local downloads.
    #' @param cache_dir Cache directory. If `NULL`, uses
    #'   `tools::R_user_dir("snstransparencia", "cache")`.
    initialize = function(base_url = "https://transparencia.sns.gov.pt/api/explore/v2.1",
                          lang = "pt", timeout = 60, user_agent = NULL,
                          use_cache = FALSE, cache_dir = NULL) {
      self$base_url <- base_url
      self$lang <- lang
      self$timeout <- timeout
      self$user_agent <- user_agent
      self$use_cache <- use_cache
      self$cache_dir <- cache_dir
    },

    #' @description List datasets in the SNS Transparency catalogue.
    #' @param limit Number of datasets to return. The API maximum is 100.
    #' @param offset Starting offset.
    #' @param where Optional ODSQL `where` expression for catalogue search.
    #' @param order_by Optional ODSQL ordering expression.
    #' @param select Optional catalogue fields to return. Use `NULL` for the
    #'   API default payload.
    #' @param refine Optional facet refinements as a character vector
    #'   (`"field:value"`) or named list (`list(field = value)`).
    #' @param exclude Optional facet exclusions, using the same format as
    #'   `refine`.
    #' @return A data frame of catalogue results.
    list_datasets = function(limit = 100, offset = 0, where = NULL,
                             order_by = "modified desc",
                             select = c("dataset_id", "title", "records_count",
                                        "modified", "publisher"),
                             refine = NULL, exclude = NULL) {
      private$validate_limit(limit, offset, max_limit = 100L, max_window = 10000L)

      params <- private$catalogue_query_params(
        limit = limit, offset = offset, where = where, order_by = order_by,
        select = select, refine = refine, exclude = exclude
      )
      body <- private$get_json("catalog/datasets", params)
      records_to_df(body$results)
    },

    #' @description Retrieve all matching catalogue datasets by paging through
    #'   the API.
    #' @param page_size Number of datasets per API request. The API maximum is
    #'   100.
    #' @param max_records Optional maximum number of datasets to retrieve. Use
    #'   `Inf` to retrieve every matching dataset available within the API
    #'   result window.
    #' @inheritParams list_datasets
    #' @return A data frame of catalogue results.
    list_all_datasets = function(page_size = 100, max_records = Inf,
                                 where = NULL, order_by = "modified desc",
                                 select = c("dataset_id", "title",
                                            "records_count", "modified",
                                            "publisher"),
                                 refine = NULL, exclude = NULL) {
      private$validate_page_size(page_size)
      private$validate_max_records(max_records)
      if (identical(max_records, 0) || identical(max_records, 0L)) {
        return(data.frame(stringsAsFactors = FALSE))
      }

      page_size <- as.integer(page_size)
      offset <- 0L
      rows <- list()
      target <- max_records

      repeat {
        limit <- if (is.finite(target)) {
          min(page_size, as.integer(target) - offset)
        } else {
          page_size
        }
        if (limit <= 0) break
        private$validate_limit(limit, offset, max_limit = 100L,
                               max_window = 10000L)

        params <- private$catalogue_query_params(
          limit = limit, offset = offset, where = where,
          order_by = order_by, select = select, refine = refine,
          exclude = exclude
        )
        body <- private$get_json("catalog/datasets", params)
        page <- records_to_df(body$results)
        rows[[length(rows) + 1L]] <- page

        total_count <- body$total_count %||% NA_integer_
        if (!is.na(total_count)) {
          target <- min(max_records, as.integer(total_count))
          if (target > 10000L) {
            rlang::abort(c(
              "This catalogue query has more than 10000 results.",
              i = "The API result window only allows the first 10000 records."
            ), class = "snstransparencia_error")
          }
        }

        fetched <- sum(vapply(rows, nrow, integer(1)))
        if (nrow(page) == 0 || nrow(page) < limit || fetched >= target) break
        offset <- offset + nrow(page)
      }

      bind_data_frames(rows)
    },

    #' @description Search datasets with a simple full-text catalogue query.
    #' @param query Search text.
    #' @param ... Additional arguments passed to `list_datasets()`.
    #' @return A data frame of matching catalogue results.
    search_datasets = function(query, ...) {
      if (!is.character(query) || length(query) != 1 || !nzchar(query)) {
        rlang::abort("`query` must be a non-empty character string.",
                     class = "snstransparencia_error")
      }
      self$list_datasets(where = quote_odsql_text(query), ...)
    },

    #' @description Download and cache a local copy of the catalogue.
    #' @inheritParams list_all_datasets
    #' @param overwrite Whether to refresh an existing cached catalogue created
    #'   with the same query options.
    #' @return A data frame containing the cached catalogue.
    cache_catalogue = function(page_size = 100, max_records = Inf,
                               where = NULL, order_by = "modified desc",
                               select = c("dataset_id", "title",
                                          "records_count", "modified",
                                          "publisher", "description", "theme",
                                          "keyword"),
                               refine = NULL, exclude = NULL,
                               overwrite = FALSE) {
      prepared <- private$prepare_catalogue_cache(
        page_size = page_size, max_records = max_records, where = where,
        order_by = order_by, select = select, refine = refine,
        exclude = exclude
      )

      existing <- private$read_manifest(prepared$manifest_path)
      if (!isTRUE(overwrite) && !is.null(existing) &&
          file.exists(existing$path)) {
        cached <- tryCatch(readRDS(existing$path), error = function(e) NULL)
        if (!is.null(cached)) {
          message("Using cached catalogue")
          return(cached)
        }
      }

      catalogue <- self$list_all_datasets(
        page_size = page_size, max_records = max_records, where = where,
        order_by = order_by, select = select, refine = refine,
        exclude = exclude
      )

      dir.create(dirname(prepared$rds_path), recursive = TRUE,
                 showWarnings = FALSE)
      saveRDS(catalogue, prepared$rds_path)

      manifest <- list(
        type = "catalogue",
        cache_key = prepared$cache_key,
        params = prepared$params,
        rows = nrow(catalogue),
        path = normalizePath(prepared$rds_path, winslash = "/",
                             mustWork = FALSE),
        cached_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS%z")
      )
      jsonlite::write_json(manifest, prepared$manifest_path,
                           auto_unbox = TRUE, pretty = TRUE, null = "null")
      catalogue
    },

    #' @description Load a cached catalogue.
    #' @param cache_key Optional catalogue cache key. If `NULL`, loads the most
    #'   recently modified cached catalogue.
    #' @return A data frame containing the cached catalogue.
    load_catalogue = function(cache_key = NULL) {
      manifest <- private$resolve_catalogue_manifest(cache_key)
      if (is.null(manifest)) {
        rlang::abort(c(
          "No cached catalogue found.",
          i = "Use `cache_catalogue()` first."
        ), class = "snstransparencia_error")
      }
      readRDS(manifest$path)
    },

    #' @description Search a locally cached catalogue without querying the API.
    #' @param query Search text.
    #' @param fields Catalogue columns to search. Use `NULL` to search all
    #'   character columns.
    #' @param cache_key Optional catalogue cache key.
    #' @param ignore_case Whether matching should ignore case.
    #' @param fixed Whether `query` should be treated as literal text instead
    #'   of a regular expression.
    #' @return A data frame of matching catalogue rows.
    search_cached_catalogue = function(query,
                                       fields = c("dataset_id", "title",
                                                  "description", "publisher",
                                                  "theme", "keyword"),
                                       cache_key = NULL,
                                       ignore_case = TRUE,
                                       fixed = TRUE) {
      if (!is.character(query) || length(query) != 1 || !nzchar(query)) {
        rlang::abort("`query` must be a non-empty character string.",
                     class = "snstransparencia_error")
      }
      if (!is.logical(ignore_case) || length(ignore_case) != 1 ||
          is.na(ignore_case)) {
        rlang::abort("`ignore_case` must be TRUE or FALSE.",
                     class = "snstransparencia_error")
      }
      if (!is.logical(fixed) || length(fixed) != 1 || is.na(fixed)) {
        rlang::abort("`fixed` must be TRUE or FALSE.",
                     class = "snstransparencia_error")
      }

      catalogue <- self$load_catalogue(cache_key)
      if (is.null(fields)) {
        fields <- names(catalogue)[vapply(catalogue, function(x) {
          is.character(x) || is.factor(x)
        }, logical(1))]
      }
      fields <- intersect(fields, names(catalogue))
      if (length(fields) == 0) {
        rlang::abort("None of the requested `fields` exist in the cached catalogue.",
                     class = "snstransparencia_error")
      }

      matches <- rep(FALSE, nrow(catalogue))
      for (field in fields) {
        values <- as.character(catalogue[[field]])
        values[is.na(values)] <- ""
        pattern <- query
        haystack <- values
        grep_ignore_case <- ignore_case
        if (isTRUE(fixed) && isTRUE(ignore_case)) {
          pattern <- tolower(pattern)
          haystack <- tolower(haystack)
          grep_ignore_case <- FALSE
        }
        matches <- matches | grepl(pattern, haystack,
                                   ignore.case = grep_ignore_case,
                                   fixed = fixed)
      }

      out <- catalogue[matches, , drop = FALSE]
      row.names(out) <- NULL
      out
    },

    #' @description List cached catalogues.
    #' @return A data frame with one row per cached catalogue.
    list_cached_catalogues = function() {
      empty <- data.frame(
        cache_key = character(),
        cached_at = character(),
        rows = integer(),
        path = character(),
        size = numeric(),
        where = character(),
        order_by = character(),
        stringsAsFactors = FALSE
      )

      cache_root <- private$get_catalogue_cache_root()
      if (!dir.exists(cache_root)) return(empty)

      manifest_paths <- list.files(cache_root, pattern = "^manifest\\.json$",
                                   recursive = TRUE, full.names = TRUE)
      if (length(manifest_paths) == 0) return(empty)

      rows <- lapply(manifest_paths, function(path) {
        manifest <- private$read_manifest(path)
        if (is.null(manifest) || !identical(manifest$type, "catalogue")) {
          return(NULL)
        }
        data.frame(
          cache_key = manifest$cache_key %||% NA_character_,
          cached_at = manifest$cached_at %||% NA_character_,
          rows = manifest$rows %||% NA_integer_,
          path = manifest$path %||% NA_character_,
          size = if (file.exists(manifest$path)) file.info(manifest$path)$size else NA_real_,
          where = manifest$params$where %||% NA_character_,
          order_by = manifest$params$order_by %||% NA_character_,
          stringsAsFactors = FALSE
        )
      })

      rows <- Filter(Negate(is.null), rows)
      if (length(rows) == 0) return(empty)
      bind_data_frames(rows)
    },

    #' @description Clear cached catalogues.
    #' @param cache_key Optional catalogue cache key. If `NULL`, clears all
    #'   cached catalogues.
    #' @return Invisibly, `TRUE` if catalogue cache was removed and `FALSE`
    #'   otherwise.
    clear_catalogue_cache = function(cache_key = NULL) {
      cache_root <- private$get_catalogue_cache_root()
      if (!dir.exists(cache_root)) return(invisible(FALSE))

      if (is.null(cache_key)) {
        unlink(cache_root, recursive = TRUE)
        return(invisible(TRUE))
      }
      if (!is.character(cache_key) || length(cache_key) != 1 ||
          is.na(cache_key) || !nzchar(cache_key)) {
        rlang::abort("`cache_key` must be NULL or a non-empty character string.",
                     class = "snstransparencia_error")
      }

      cache_dir <- private$get_catalogue_query_cache_dir(cache_key)
      if (!dir.exists(cache_dir)) return(invisible(FALSE))
      unlink(cache_dir, recursive = TRUE)
      invisible(TRUE)
    },

    #' @description Get metadata for one dataset.
    #' @param dataset_id Dataset identifier, e.g. `"substancias"`.
    #' @param select Optional metadata fields to return.
    #' @return A list with the API metadata payload.
    get_metadata = function(dataset_id, select = NULL) {
      validate_dataset_id(dataset_id)
      params <- compact_list(list(
        select = collapse_api_expr(select),
        lang = self$lang
      ))
      private$get_json(private$dataset_path(dataset_id), params)
    },

    #' @description Get a compact one-row summary for one dataset.
    #' @param dataset_id Dataset identifier.
    #' @return A data frame with common metadata fields.
    get_dataset_info = function(dataset_id) {
      metadata_info_df(self$get_metadata(dataset_id))
    },

    #' @description Get the field schema for one dataset.
    #' @param dataset_id Dataset identifier.
    #' @param include_annotations Whether to keep the nested `annotations`
    #'   column. Defaults to `FALSE`.
    #' @return A data frame with field names, labels, types, and descriptions.
    get_fields = function(dataset_id, include_annotations = FALSE) {
      metadata <- self$get_metadata(dataset_id)
      fields <- records_to_df(metadata$fields)
      if (!include_annotations && "annotations" %in% names(fields)) {
        fields$annotations <- NULL
      }
      front <- intersect(c("name", "label", "type", "description",
                           "label_pt", "label_en"), names(fields))
      fields[, c(front, setdiff(names(fields), front)), drop = FALSE]
    },

    #' @description List available export formats for one dataset.
    #' @param dataset_id Dataset identifier.
    #' @param include_self Whether to include the API `self` link.
    #' @return A data frame with `format` and `href` columns.
    list_export_formats = function(dataset_id, include_self = FALSE) {
      validate_dataset_id(dataset_id)
      body <- private$get_json(paste0(private$dataset_path(dataset_id), "/exports"))
      out <- records_to_df(body$links)
      if (nrow(out) == 0) {
        return(data.frame(format = character(), href = character(),
                          stringsAsFactors = FALSE))
      }
      names(out)[names(out) == "rel"] <- "format"
      if (!isTRUE(include_self)) {
        out <- out[out$format != "self", , drop = FALSE]
      }
      row.names(out) <- NULL
      out
    },

    #' @description Get facet values for one dataset.
    #' @param dataset_id Dataset identifier.
    #' @param facets Optional facet field names or raw `facet(...)`
    #'   expressions. Simple field names are converted to `facet(name="field")`.
    #'   If `NULL`, the API default facets are returned.
    #' @param where Optional raw ODSQL where expression.
    #' @param filters Optional exact facet filters as a named list or character
    #'   vector. `list(regiao = "Norte")` becomes `refine=regiao:Norte`.
    #' @param refine Optional raw API refine filters. Combined with `filters`.
    #' @param exclude Optional facet exclusions.
    #' @param timezone Timezone used by the API for date/time formatting.
    #' @param validate Whether to validate facet and filter field names against
    #'   dataset metadata before querying.
    #' @return A data frame with `facet`, `name`, `value`, `count`, and `state`.
    get_facets = function(dataset_id, facets = NULL, where = NULL,
                          filters = NULL, refine = NULL, exclude = NULL,
                          timezone = "Europe/Lisbon", validate = TRUE) {
      validate_dataset_id(dataset_id)
      validate_logical_flag(validate, "validate")
      if (isTRUE(validate)) {
        metadata <- self$get_metadata(dataset_id)
        validate_dataset_query(
          metadata, dataset_id, filters = filters, refine = refine,
          exclude = exclude, facets = facets
        )
      }
      refine <- c(format_facet_filters(filters, "filters"),
                  format_facet_filters(refine, "refine"))

      params <- compact_list(list(
        facet = format_facet_specs(facets),
        where = where,
        refine = if (length(refine) == 0) NULL else refine,
        exclude = format_facet_filters(exclude, "exclude"),
        lang = self$lang,
        timezone = timezone
      ))

      body <- private$get_json(paste0(private$dataset_path(dataset_id), "/facets"),
                               params)
      facets_to_df(body$facets)
    },

    #' @description Query records from one dataset.
    #' @param dataset_id Dataset identifier.
    #' @param columns Optional field names or ODSQL select expressions. This is
    #'   a user-friendly alias for the API `select` parameter.
    #' @param select Optional raw ODSQL select expression. Use either
    #'   `columns` or `select`, not both.
    #' @param where Optional raw ODSQL where expression.
    #' @param filters Optional exact facet filters as a named list or character
    #'   vector. `list(regiao = "Norte")` becomes `refine=regiao:Norte`.
    #' @param refine Optional raw API refine filters. Combined with `filters`.
    #' @param exclude Optional facet exclusions.
    #' @param date_field Optional date field used with `date_from`/`date_to`.
    #' @param date_from,date_to Optional date bounds appended to `where`.
    #' @param order_by Optional ODSQL ordering expression.
    #' @param group_by Optional ODSQL grouping expression.
    #' @param limit Number of records to return. Without `group_by`, the API
    #'   maximum is 100 and the `offset + limit` window must be <= 10000.
    #' @param offset Starting offset.
    #' @param timezone Timezone used by the API for date/time formatting.
    #' @param validate Whether to validate simple column, filter, and date field
    #'   names against dataset metadata before querying.
    #' @return A data frame of records.
    query_records = function(dataset_id, columns = NULL, select = NULL,
                             where = NULL, filters = NULL, refine = NULL,
                             exclude = NULL, date_field = NULL,
                             date_from = NULL, date_to = NULL,
                             order_by = NULL, group_by = NULL,
                             limit = 100, offset = 0,
                             timezone = "Europe/Lisbon", validate = TRUE) {
      validate_dataset_id(dataset_id)
      validate_logical_flag(validate, "validate")
      if (!is.null(columns) && !is.null(select)) {
        rlang::abort("Use either `columns` or `select`, not both.",
                     class = "snstransparencia_error")
      }

      max_limit <- if (is.null(group_by)) 100L else 20000L
      max_window <- if (is.null(group_by)) 10000L else 20000L
      private$validate_limit(limit, offset, max_limit = max_limit,
                             max_window = max_window, allow_zero = TRUE)

      if (isTRUE(validate)) {
        metadata <- self$get_metadata(dataset_id)
        validate_dataset_query(
          metadata, dataset_id, columns = columns, select = select,
          date_field = date_field, filters = filters, refine = refine,
          exclude = exclude
        )
      }

      date_where <- build_date_where(date_field, date_from, date_to)
      where <- combine_where(where, date_where)
      refine <- c(format_facet_filters(filters, "filters"),
                  format_facet_filters(refine, "refine"))

      params <- compact_list(list(
        limit = as.integer(limit),
        offset = as.integer(offset),
        select = collapse_api_expr(columns %||% select),
        where = where,
        refine = if (length(refine) == 0) NULL else refine,
        exclude = format_facet_filters(exclude, "exclude"),
        order_by = order_by,
        group_by = group_by,
        lang = self$lang,
        timezone = timezone
      ))

      body <- private$get_json(paste0(private$dataset_path(dataset_id), "/records"),
                               params)
      records_to_df(body$results)
    },

    #' @description Download a dataset export to the local cache.
    #' @param dataset_id Dataset identifier.
    #' @param format Export format. Supported loading formats are `"csv"`,
    #'   `"json"`, `"jsonl"`, and `"parquet"` if the optional `arrow` package is
    #'   installed. The default is `"csv"`.
    #' @param columns Optional field names or ODSQL select expressions.
    #' @param select Optional raw ODSQL select expression. Use either
    #'   `columns` or `select`, not both.
    #' @param where Optional raw ODSQL where expression.
    #' @param filters Optional exact facet filters as a named list or character
    #'   vector. `list(regiao = "Norte")` becomes `refine=regiao:Norte`.
    #' @param refine Optional raw API refine filters. Combined with `filters`.
    #' @param exclude Optional facet exclusions.
    #' @param date_field Optional date field used with `date_from`/`date_to`.
    #' @param date_from,date_to Optional date bounds appended to `where`.
    #' @param order_by Optional ODSQL ordering expression.
    #' @param group_by Optional ODSQL grouping expression.
    #' @param limit Export limit. Use `-1` to retrieve all matching records.
    #' @param timezone Timezone used by the API for date/time formatting.
    #' @param use_labels Whether exports should use field labels instead of
    #'   field names when supported by the API.
    #' @param epsg Coordinate reference system used for geospatial exports.
    #' @param overwrite Whether to redownload when the same cached query exists.
    #' @param validate Whether to validate simple column, filter, and date field
    #'   names against dataset metadata before downloading.
    #' @return Invisibly, a manifest list describing the cached export.
    download_data = function(dataset_id, format = "csv", columns = NULL,
                             select = NULL, where = NULL, filters = NULL,
                             refine = NULL, exclude = NULL, date_field = NULL,
                             date_from = NULL, date_to = NULL, order_by = NULL,
                             group_by = NULL, limit = -1,
                             timezone = "Europe/Lisbon", use_labels = FALSE,
                             epsg = 4326, overwrite = FALSE,
                             validate = TRUE) {
      validate_dataset_id(dataset_id)
      validate_logical_flag(validate, "validate")
      format <- validate_export_format(format)

      prepared <- private$prepare_export(
        dataset_id = dataset_id, format = format, columns = columns,
        select = select, where = where, filters = filters, refine = refine,
        exclude = exclude, date_field = date_field, date_from = date_from,
        date_to = date_to, order_by = order_by, group_by = group_by,
        limit = limit, timezone = timezone, use_labels = use_labels,
        epsg = epsg
      )

      existing <- private$read_manifest(prepared$manifest_path)
      if (!overwrite && file.exists(prepared$export_path) &&
          file.info(prepared$export_path)$size > 0 && !is.null(existing)) {
        message("Using cached export")
        return(invisible(existing))
      }

      metadata <- self$get_metadata(dataset_id)
      if (isTRUE(validate)) {
        validate_dataset_query(
          metadata, dataset_id, columns = columns, select = select,
          date_field = date_field, filters = filters, refine = refine,
          exclude = exclude
        )
      }
      manifest <- private$download_export(prepared, metadata)
      private$invalidate_parsed_cache(prepared$rds_path)
      invisible(manifest)
    },

    #' @description Load a previously downloaded dataset export from cache.
    #' @param dataset_id Dataset identifier.
    #' @param cache_key Optional cache key returned by `download_data()`.
    #' @param format Optional format used to disambiguate cached exports.
    #' @param use_parsed_cache Whether to reuse parsed `.rds` cache when
    #'   available.
    #' @return A data frame.
    load_data = function(dataset_id, cache_key = NULL, format = NULL,
                         use_parsed_cache = TRUE) {
      validate_dataset_id(dataset_id)
      manifest <- private$resolve_manifest(dataset_id, cache_key, format)
      if (is.null(manifest)) {
        rlang::abort(c(
          paste0("No cached data found for dataset '", dataset_id, "'."),
          i = "Use `download_data()` first."
        ), class = "snstransparencia_error")
      }

      if (isTRUE(use_parsed_cache) && file.exists(manifest$rds_path)) {
        parsed <- tryCatch(readRDS(manifest$rds_path), error = function(e) NULL)
        if (!is.null(parsed)) return(parsed)
      }

      data <- private$read_export_file(manifest$path, manifest$format)
      if (isTRUE(use_parsed_cache)) {
        dir.create(dirname(manifest$rds_path), recursive = TRUE,
                   showWarnings = FALSE)
        saveRDS(data, manifest$rds_path)
      }
      data
    },

    #' @description Download and load a dataset.
    #' @details
    #' Full data retrieval uses the API export endpoint, not the paginated
    #' records endpoint. When `use_cache` is `TRUE`, identical queries reuse the
    #' local export cache. When `use_cache` is `FALSE`, data is downloaded to a
    #' temporary file and parsed without persisting a cache entry.
    #' @inheritParams download_data
    #' @return A data frame.
    get_data = function(dataset_id, format = "csv", columns = NULL,
                        select = NULL, where = NULL, filters = NULL,
                        refine = NULL, exclude = NULL, date_field = NULL,
                        date_from = NULL, date_to = NULL, order_by = NULL,
                        group_by = NULL, limit = -1,
                        timezone = "Europe/Lisbon", use_labels = FALSE,
                        epsg = 4326, refresh = FALSE, validate = TRUE) {
      validate_dataset_id(dataset_id)
      validate_logical_flag(validate, "validate")
      format <- validate_export_format(format)

      prepared <- private$prepare_export(
        dataset_id = dataset_id, format = format, columns = columns,
        select = select, where = where, filters = filters, refine = refine,
        exclude = exclude, date_field = date_field, date_from = date_from,
        date_to = date_to, order_by = order_by, group_by = group_by,
        limit = limit, timezone = timezone, use_labels = use_labels,
        epsg = epsg
      )

      if (self$use_cache && !isTRUE(refresh) &&
          file.exists(prepared$export_path) &&
          file.info(prepared$export_path)$size > 0 &&
          file.exists(prepared$manifest_path)) {
        return(self$load_data(dataset_id, cache_key = prepared$cache_key,
                              format = format))
      }

      if (self$use_cache) {
        manifest <- self$download_data(
          dataset_id = dataset_id, format = format, columns = columns,
          select = select, where = where, filters = filters, refine = refine,
          exclude = exclude, date_field = date_field, date_from = date_from,
          date_to = date_to, order_by = order_by, group_by = group_by,
          limit = limit, timezone = timezone, use_labels = use_labels,
          epsg = epsg, overwrite = isTRUE(refresh), validate = validate
        )
        return(self$load_data(dataset_id, cache_key = manifest$cache_key,
                              format = format))
      }

      metadata <- self$get_metadata(dataset_id)
      if (isTRUE(validate)) {
        validate_dataset_query(
          metadata, dataset_id, columns = columns, select = select,
          date_field = date_field, filters = filters, refine = refine,
          exclude = exclude
        )
      }
      temp_path <- tempfile(fileext = paste0(".", format))
      on.exit(unlink(temp_path), add = TRUE)
      temp_prepared <- prepared
      temp_prepared$export_path <- temp_path
      private$download_export(temp_prepared, metadata, write_manifest = FALSE)
      private$read_export_file(temp_path, format)
    },

    #' @description Check whether a cached export is older than live metadata.
    #' @param dataset_id Dataset identifier.
    #' @param cache_key Optional cache key returned by `download_data()`.
    #' @param format Optional format used to disambiguate cached exports.
    #' @return `TRUE` if the live dataset is newer, `FALSE` otherwise.
    is_updated = function(dataset_id, cache_key = NULL, format = NULL) {
      validate_dataset_id(dataset_id)
      manifest <- private$resolve_manifest(dataset_id, cache_key, format)
      if (is.null(manifest)) {
        rlang::abort(c(
          paste0("No cached data found for dataset '", dataset_id, "'."),
          i = "Use `download_data()` first."
        ), class = "snstransparencia_error")
      }

      metadata <- self$get_metadata(dataset_id)
      live_time <- metadata_data_processed(metadata) %||% metadata_modified(metadata)
      cached_time <- manifest$data_processed %||% manifest$modified
      is_later_api_time(live_time, cached_time)
    },

    #' @description List cached exports.
    #' @return A data frame with one row per cached export.
    list_cached = function() {
      empty <- data.frame(
        dataset_id = character(),
        cache_key = character(),
        format = character(),
        downloaded_at = character(),
        modified = character(),
        data_processed = character(),
        records_count = integer(),
        path = character(),
        size = numeric(),
        parsed = logical(),
        stringsAsFactors = FALSE
      )

      cache_root <- private$get_cache_dir_path()
      if (!dir.exists(cache_root)) return(empty)

      manifest_paths <- list.files(cache_root, pattern = "^manifest\\.json$",
                                   recursive = TRUE, full.names = TRUE)
      if (length(manifest_paths) == 0) return(empty)

      rows <- lapply(manifest_paths, function(path) {
        manifest <- private$read_manifest(path)
        if (is.null(manifest)) return(NULL)
        if (identical(manifest$type, "catalogue")) return(NULL)
        data.frame(
          dataset_id = manifest$dataset_id %||% NA_character_,
          cache_key = manifest$cache_key %||% NA_character_,
          format = manifest$format %||% NA_character_,
          downloaded_at = manifest$downloaded_at %||% NA_character_,
          modified = manifest$modified %||% NA_character_,
          data_processed = manifest$data_processed %||% NA_character_,
          records_count = manifest$records_count %||% NA_integer_,
          path = manifest$path %||% NA_character_,
          size = if (file.exists(manifest$path)) file.info(manifest$path)$size else NA_real_,
          parsed = file.exists(manifest$rds_path),
          stringsAsFactors = FALSE
        )
      })

      rows <- Filter(Negate(is.null), rows)
      if (length(rows) == 0) return(empty)
      do.call(rbind, rows)
    },

    #' @description Clear cached exports.
    #' @param dataset_id Optional dataset identifier. If `NULL`, clears all
    #'   cached data.
    #' @return Invisibly, `TRUE` if cache was removed and `FALSE` otherwise.
    clear_cache = function(dataset_id = NULL) {
      if (!is.null(dataset_id)) validate_dataset_id(dataset_id)

      cache_root <- private$get_cache_dir_path()
      if (!dir.exists(cache_root)) return(invisible(FALSE))

      if (is.null(dataset_id)) {
        unlink(cache_root, recursive = TRUE)
        return(invisible(TRUE))
      }

      dataset_dir <- private$get_dataset_cache_dir(dataset_id)
      if (!dir.exists(dataset_dir)) return(invisible(FALSE))
      unlink(dataset_dir, recursive = TRUE)
      invisible(TRUE)
    },

    #' @description Print client configuration.
    #' @param ... Ignored.
    print = function(...) {
      cache_status <- if (self$use_cache) {
        paste0("enabled (", private$get_cache_dir_path(), ")")
      } else {
        "disabled"
      }
      cat(
        "<SNSClient>\n",
        "  Base URL:     ", self$base_url, "\n",
        "  Language:     ", self$lang, "\n",
        "  Timeout (s):  ", self$timeout, "\n",
        "  User agent:   ", self$user_agent %||% "<default>", "\n",
        "  Cache:        ", cache_status, "\n",
        sep = ""
      )
      invisible(self)
    }
  ),

  private = list(
    .base_url = NULL,
    .lang = NULL,
    .timeout = NULL,
    .user_agent = NULL,
    .use_cache = NULL,
    .cache_dir = NULL,

    dataset_path = function(dataset_id) {
      paste0("catalog/datasets/", utils::URLencode(dataset_id, reserved = TRUE))
    },

    endpoint = function(path) {
      paste0(sub("/+$", "", self$base_url), "/", sub("^/+", "", path))
    },

    catalogue_query_params = function(limit, offset, where = NULL,
                                      order_by = "modified desc",
                                      select = c("dataset_id", "title",
                                                 "records_count", "modified",
                                                 "publisher"),
                                      refine = NULL, exclude = NULL) {
      compact_list(list(
        limit = as.integer(limit),
        offset = as.integer(offset),
        where = where,
        order_by = order_by,
        select = collapse_api_expr(select),
        refine = format_facet_filters(refine, "refine"),
        exclude = format_facet_filters(exclude, "exclude"),
        lang = self$lang
      ))
    },

    prepare_catalogue_cache = function(page_size = 100, max_records = Inf,
                                       where = NULL,
                                       order_by = "modified desc",
                                       select = c("dataset_id", "title",
                                                  "records_count", "modified",
                                                  "publisher"),
                                       refine = NULL, exclude = NULL) {
      private$validate_page_size(page_size)
      private$validate_max_records(max_records)

      params <- normalize_query_params(list(
        page_size = as.integer(page_size),
        max_records = if (is.infinite(max_records)) "Inf" else as.integer(max_records),
        where = where,
        order_by = order_by,
        select = collapse_api_expr(select),
        refine = format_facet_filters(refine, "refine"),
        exclude = format_facet_filters(exclude, "exclude"),
        lang = self$lang
      ))
      cache_key <- private$query_key("catalogue", params)
      cache_dir <- private$get_catalogue_query_cache_dir(cache_key)

      list(
        params = params,
        cache_key = cache_key,
        rds_path = file.path(cache_dir, "catalogue.rds"),
        manifest_path = file.path(cache_dir, "manifest.json")
      )
    },

    prepare_export = function(dataset_id, format, columns = NULL, select = NULL,
                              where = NULL, filters = NULL, refine = NULL,
                              exclude = NULL, date_field = NULL,
                              date_from = NULL, date_to = NULL,
                              order_by = NULL, group_by = NULL, limit = -1,
                              timezone = "Europe/Lisbon",
                              use_labels = FALSE, epsg = 4326) {
      if (!is.null(columns) && !is.null(select)) {
        rlang::abort("Use either `columns` or `select`, not both.",
                     class = "snstransparencia_error")
      }
      if (!is.numeric(limit) || length(limit) != 1 || is.na(limit) || limit < -1) {
        rlang::abort("`limit` must be -1 or a non-negative number.",
                     class = "snstransparencia_error")
      }

      date_where <- build_date_where(date_field, date_from, date_to)
      where <- combine_where(where, date_where)
      refine <- c(format_facet_filters(filters, "filters"),
                  format_facet_filters(refine, "refine"))

      params <- normalize_query_params(list(
        select = collapse_api_expr(columns %||% select),
        where = where,
        refine = if (length(refine) == 0) NULL else refine,
        exclude = format_facet_filters(exclude, "exclude"),
        order_by = order_by,
        group_by = group_by,
        limit = as.integer(limit),
        lang = self$lang,
        timezone = timezone,
        use_labels = tolower(as.character(isTRUE(use_labels))),
        epsg = as.integer(epsg)
      ))

      cache_key <- private$query_key(format, params)
      cache_dir <- private$get_query_cache_dir(dataset_id, cache_key)

      list(
        dataset_id = dataset_id,
        format = format,
        params = params,
        cache_key = cache_key,
        export_path = file.path(cache_dir, paste0("data.", format)),
        manifest_path = file.path(cache_dir, "manifest.json"),
        rds_path = file.path(cache_dir, "data.rds"),
        url = private$endpoint(paste0(private$dataset_path(dataset_id),
                                      "/exports/", format))
      )
    },

    get_json = function(path, params = list()) {
      req <- httr2::request(private$endpoint(path)) |>
        httr2::req_url_query(!!!params, .multi = "explode") |>
        httr2::req_timeout(seconds = self$timeout) |>
        httr2::req_error(is_error = \(resp) FALSE)

      if (!is.null(self$user_agent)) {
        req <- httr2::req_user_agent(req, self$user_agent)
      }

      resp <- tryCatch(
        httr2::req_perform(req),
        error = function(e) {
          rlang::abort(c(
            "SNS Transparency API request failed.",
            x = conditionMessage(e)
          ), class = "snstransparencia_api_error")
        }
      )

      status <- httr2::resp_status(resp)
      if (status >= 400) {
        payload <- tryCatch(
          httr2::resp_body_json(resp, simplifyVector = FALSE),
          error = function(e) NULL
        )
        message <- payload$message %||% httr2::resp_status_desc(resp)
        rlang::abort(c(
          "SNS Transparency API returned an error.",
          x = paste0(status, " - ", message)
        ), class = "snstransparencia_api_error")
      }

      httr2::resp_body_json(resp, simplifyVector = FALSE)
    },

    download_export = function(prepared, metadata, write_manifest = TRUE) {
      req <- httr2::request(prepared$url) |>
        httr2::req_url_query(!!!prepared$params, .multi = "explode") |>
        httr2::req_timeout(seconds = self$timeout) |>
        httr2::req_error(is_error = \(resp) FALSE)

      if (!is.null(self$user_agent)) {
        req <- httr2::req_user_agent(req, self$user_agent)
      }

      dir.create(dirname(prepared$export_path), recursive = TRUE,
                 showWarnings = FALSE)
      temp_path <- paste0(prepared$export_path, ".part")
      on.exit(if (file.exists(temp_path)) unlink(temp_path), add = TRUE)

      resp <- tryCatch(
        httr2::req_perform(req, path = temp_path),
        error = function(e) {
          rlang::abort(c(
            "SNS Transparency export download failed.",
            x = conditionMessage(e)
          ), class = "snstransparencia_api_error")
        }
      )

      status <- httr2::resp_status(resp)
      if (status >= 400) {
        payload <- tryCatch(
          httr2::resp_body_json(resp, simplifyVector = FALSE),
          error = function(e) NULL
        )
        message <- payload$message %||% httr2::resp_status_desc(resp)
        rlang::abort(c(
          "SNS Transparency API returned an error.",
          x = paste0(status, " - ", message)
        ), class = "snstransparencia_api_error")
      }

      if (!file.exists(temp_path) || file.info(temp_path)$size == 0) {
        raw_body <- tryCatch(httr2::resp_body_raw(resp), error = function(e) raw())
        if (length(raw_body) > 0) {
          writeBin(raw_body, temp_path)
        }
      }

      if (!file.exists(temp_path) || file.info(temp_path)$size == 0) {
        rlang::abort("Downloaded export file is empty.",
                     class = "snstransparencia_api_error")
      }

      if (file.exists(prepared$export_path)) unlink(prepared$export_path)
      if (!file.rename(temp_path, prepared$export_path)) {
        rlang::abort("Could not move downloaded export into cache.",
                     class = "snstransparencia_error")
      }

      manifest <- list(
        type = "export",
        dataset_id = prepared$dataset_id,
        title = metadata_title(metadata),
        format = prepared$format,
        params = prepared$params,
        cache_key = prepared$cache_key,
        path = normalizePath(prepared$export_path, winslash = "/",
                             mustWork = FALSE),
        rds_path = normalizePath(prepared$rds_path, winslash = "/",
                                 mustWork = FALSE),
        source_url = req$url,
        records_count = metadata_records_count(metadata),
        modified = metadata_modified(metadata),
        data_processed = metadata_data_processed(metadata),
        downloaded_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS%z")
      )

      if (isTRUE(write_manifest)) {
        jsonlite::write_json(manifest, prepared$manifest_path,
                             auto_unbox = TRUE, pretty = TRUE, null = "null")
      }

      manifest
    },

    read_export_file = function(path, format) {
      if (!file.exists(path)) {
        rlang::abort(paste0("Cached export file does not exist: ", path),
                     class = "snstransparencia_error")
      }

      switch(format,
        csv = utils::read.csv(path, sep = ";", stringsAsFactors = FALSE,
                              check.names = FALSE, fileEncoding = "UTF-8-BOM"),
        json = jsonlite::fromJSON(path, flatten = TRUE),
        jsonl = {
          con <- file(path)
          on.exit(try(close(con), silent = TRUE), add = TRUE)
          jsonlite::stream_in(con, verbose = FALSE)
        },
        parquet = {
          if (!requireNamespace("arrow", quietly = TRUE)) {
            rlang::abort("Loading parquet exports requires the optional `arrow` package.",
                         class = "snstransparencia_error")
          }
          as.data.frame(arrow::read_parquet(path))
        },
        rlang::abort(paste0("Unsupported export format: ", format),
                     class = "snstransparencia_error")
      )
    },

    query_key = function(format, params) {
      signature <- jsonlite::toJSON(list(format = format, params = params),
                                    auto_unbox = TRUE, null = "null")
      tmp <- tempfile()
      on.exit(unlink(tmp), add = TRUE)
      writeLines(signature, tmp, useBytes = TRUE)
      unname(tools::md5sum(tmp))
    },

    get_cache_dir_path = function() {
      path <- if (is.null(self$cache_dir)) {
        tools::R_user_dir("snstransparencia", "cache")
      } else {
        self$cache_dir
      }
      normalizePath(path, winslash = "/", mustWork = FALSE)
    },

    get_catalogue_cache_root = function() {
      file.path(private$get_cache_dir_path(), "_catalogue")
    },

    get_catalogue_query_cache_dir = function(cache_key) {
      file.path(private$get_catalogue_cache_root(), cache_key)
    },

    get_dataset_cache_dir = function(dataset_id) {
      file.path(private$get_cache_dir_path(), dataset_id)
    },

    get_query_cache_dir = function(dataset_id, cache_key) {
      file.path(private$get_dataset_cache_dir(dataset_id), cache_key)
    },

    read_manifest = function(path) {
      if (!file.exists(path)) return(NULL)
      tryCatch(
        jsonlite::fromJSON(path, simplifyVector = FALSE),
        error = function(e) NULL
      )
    },

    resolve_catalogue_manifest = function(cache_key = NULL) {
      cache_root <- private$get_catalogue_cache_root()
      if (!dir.exists(cache_root)) return(NULL)

      paths <- if (!is.null(cache_key)) {
        file.path(private$get_catalogue_query_cache_dir(cache_key), "manifest.json")
      } else {
        list.files(cache_root, pattern = "^manifest\\.json$",
                   recursive = TRUE, full.names = TRUE)
      }
      paths <- paths[file.exists(paths)]
      if (length(paths) == 0) return(NULL)

      manifests <- lapply(paths, private$read_manifest)
      manifests <- Filter(function(x) {
        !is.null(x) && identical(x$type, "catalogue")
      }, manifests)
      if (length(manifests) == 0) return(NULL)

      mtimes <- vapply(manifests, function(x) {
        if (file.exists(x$path)) file.info(x$path)$mtime else as.POSIXct(NA)
      }, as.POSIXct(NA))
      manifests[[which.max(mtimes)]]
    },

    resolve_manifest = function(dataset_id, cache_key = NULL, format = NULL) {
      dataset_dir <- private$get_dataset_cache_dir(dataset_id)
      if (!dir.exists(dataset_dir)) return(NULL)

      paths <- if (!is.null(cache_key)) {
        file.path(dataset_dir, cache_key, "manifest.json")
      } else {
        list.files(dataset_dir, pattern = "^manifest\\.json$",
                   recursive = TRUE, full.names = TRUE)
      }
      paths <- paths[file.exists(paths)]
      if (length(paths) == 0) return(NULL)

      manifests <- lapply(paths, private$read_manifest)
      manifests <- Filter(Negate(is.null), manifests)
      if (!is.null(format)) {
        manifests <- Filter(function(x) identical(x$format, format), manifests)
      }
      if (length(manifests) == 0) return(NULL)

      mtimes <- vapply(manifests, function(x) {
        if (file.exists(x$path)) file.info(x$path)$mtime else as.POSIXct(NA)
      }, as.POSIXct(NA))
      manifests[[which.max(mtimes)]]
    },

    invalidate_parsed_cache = function(path) {
      if (file.exists(path)) unlink(path)
    },

    validate_page_size = function(page_size) {
      if (!is.numeric(page_size) || length(page_size) != 1 ||
          is.na(page_size) || page_size < 1 || page_size > 100) {
        rlang::abort("`page_size` must be between 1 and 100.",
                     class = "snstransparencia_error")
      }
      invisible(TRUE)
    },

    validate_max_records = function(max_records) {
      if (!is.numeric(max_records) || length(max_records) != 1 ||
          is.na(max_records) || max_records < 0) {
        rlang::abort("`max_records` must be a non-negative number or Inf.",
                     class = "snstransparencia_error")
      }
      invisible(TRUE)
    },

    validate_limit = function(limit, offset, max_limit, max_window,
                              allow_zero = FALSE) {
      min_limit <- if (allow_zero) 0 else 1
      if (!is.numeric(limit) || length(limit) != 1 ||
          is.na(limit) || limit < min_limit || limit > max_limit) {
        rlang::abort(
          sprintf("`limit` must be between %s and %s.", min_limit, max_limit),
          class = "snstransparencia_error"
        )
      }
      if (!is.numeric(offset) || length(offset) != 1 ||
          is.na(offset) || offset < 0) {
        rlang::abort("`offset` must be a non-negative number.",
                     class = "snstransparencia_error")
      }
      if ((offset + limit) > max_window) {
        rlang::abort(
          sprintf("`offset + limit` must be <= %s for this API endpoint.",
                  max_window),
          class = "snstransparencia_error"
        )
      }
      invisible(TRUE)
    }
  ),

  active = list(
    #' @field base_url API base URL.
    base_url = function(value) {
      if (missing(value)) return(private$.base_url)
      if (!is.character(value) || length(value) != 1 || !nzchar(value)) {
        stop("`base_url` must be a non-empty character string")
      }
      private$.base_url <- value
    },

    #' @field lang API language (`"pt"` or `"en"`).
    lang = function(value) {
      if (missing(value)) return(private$.lang)
      if (!is.character(value) || length(value) != 1) {
        stop("`lang` must be 'pt' or 'en'")
      }
      private$.lang <- match.arg(tolower(value), c("pt", "en"))
    },

    #' @field timeout Request timeout in seconds.
    timeout = function(value) {
      if (missing(value)) return(private$.timeout)
      if (!is.numeric(value) || length(value) != 1 || is.na(value) || value < 1) {
        stop("`timeout` must be a positive number")
      }
      private$.timeout <- value
    },

    #' @field user_agent Optional user-agent string.
    user_agent = function(value) {
      if (missing(value)) return(private$.user_agent)
      if (!is.null(value) && (!is.character(value) || length(value) != 1 ||
                              !nzchar(value))) {
        stop("`user_agent` must be NULL or a non-empty character string")
      }
      private$.user_agent <- value
    },

    #' @field use_cache Whether `get_data()` reuses local cache.
    use_cache = function(value) {
      if (missing(value)) return(private$.use_cache)
      if (!is.logical(value) || length(value) != 1 || is.na(value)) {
        stop("`use_cache` must be TRUE or FALSE")
      }
      private$.use_cache <- value
    },

    #' @field cache_dir Cache directory path, or `NULL` for default.
    cache_dir = function(value) {
      if (missing(value)) return(private$.cache_dir)
      if (!is.null(value) && (!is.character(value) || length(value) != 1 ||
                              !nzchar(value))) {
        stop("`cache_dir` must be NULL or a non-empty character string")
      }
      private$.cache_dir <- value
    }
  )
)
