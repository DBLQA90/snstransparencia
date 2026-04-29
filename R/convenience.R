#' Convenience Functions
#'
#' @description
#' Functional wrappers around [SNSClient]. These helpers create a default
#' client when `.client` is not supplied, making simple workflows shorter while
#' still allowing advanced users to pass a configured client.
#'
#' @param ... Arguments passed to the corresponding [SNSClient] method.
#' @param .client Optional [SNSClient] instance. If `NULL`, a default client is
#'   created with `sns_client()`.
#' @return The value returned by the corresponding [SNSClient] method.
#'
#' @examples
#' sns <- sns_client()
#'
#' if (identical(Sys.getenv("SNSTRANSPARENCIA_RUN_EXAMPLES"), "true")) {
#'   sns_list_datasets(limit = 5)
#'   sns_search_datasets("substancias")
#'   sns_get_fields("substancias")
#'   sns_query_records("substancias", columns = c("ano", "cannabis"), limit = 5)
#'   sns_get_data("substancias", columns = c("ano", "cannabis"))
#' }
#'
#' @name convenience
NULL

#' Create an SNS Transparency API client
#'
#' @inheritParams SNSClient
#' @return An [SNSClient] instance.
#' @export
sns_client <- function(base_url = "https://transparencia.sns.gov.pt/api/explore/v2.1",
                       lang = "pt", timeout = 60, user_agent = NULL,
                       use_cache = FALSE, cache_dir = NULL) {
  SNSClient$new(
    base_url = base_url,
    lang = lang,
    timeout = timeout,
    user_agent = user_agent,
    use_cache = use_cache,
    cache_dir = cache_dir
  )
}

resolve_client <- function(.client = NULL) {
  if (is.null(.client)) {
    return(sns_client())
  }
  if (!inherits(.client, "SNSClient")) {
    rlang::abort("`.client` must be NULL or an SNSClient instance.",
                 class = "snstransparencia_error")
  }
  .client
}

#' @rdname convenience
#' @description
#' `sns_list_datasets()` lists one page of catalogue datasets.
#' @export
sns_list_datasets <- function(..., .client = NULL) {
  resolve_client(.client)$list_datasets(...)
}

#' @rdname convenience
#' @description
#' `sns_list_all_datasets()` paginates through catalogue datasets.
#' @export
sns_list_all_datasets <- function(..., .client = NULL) {
  resolve_client(.client)$list_all_datasets(...)
}

#' @rdname convenience
#' @description
#' `sns_search_datasets()` searches the remote catalogue.
#' @param query Search text.
#' @export
sns_search_datasets <- function(query, ..., .client = NULL) {
  resolve_client(.client)$search_datasets(query, ...)
}

#' @rdname convenience
#' @description
#' `sns_cache_catalogue()` downloads and caches a local catalogue snapshot.
#' @export
sns_cache_catalogue <- function(..., .client = NULL) {
  resolve_client(.client)$cache_catalogue(...)
}

#' @rdname convenience
#' @description
#' `sns_load_catalogue()` loads a cached catalogue snapshot.
#' @export
sns_load_catalogue <- function(..., .client = NULL) {
  resolve_client(.client)$load_catalogue(...)
}

#' @rdname convenience
#' @description
#' `sns_search_cached_catalogue()` searches a local catalogue snapshot.
#' @export
sns_search_cached_catalogue <- function(query, ..., .client = NULL) {
  resolve_client(.client)$search_cached_catalogue(query, ...)
}

#' @rdname convenience
#' @description
#' `sns_list_cached_catalogues()` lists cached catalogue snapshots.
#' @export
sns_list_cached_catalogues <- function(..., .client = NULL) {
  resolve_client(.client)$list_cached_catalogues(...)
}

#' @rdname convenience
#' @description
#' `sns_clear_catalogue_cache()` clears cached catalogue snapshots.
#' @export
sns_clear_catalogue_cache <- function(..., .client = NULL) {
  resolve_client(.client)$clear_catalogue_cache(...)
}

#' @rdname convenience
#' @description
#' `sns_get_metadata()` returns raw dataset metadata.
#' @param dataset_id Dataset identifier.
#' @export
sns_get_metadata <- function(dataset_id, ..., .client = NULL) {
  resolve_client(.client)$get_metadata(dataset_id, ...)
}

#' @rdname convenience
#' @description
#' `sns_get_dataset_info()` returns a compact one-row dataset summary.
#' @export
sns_get_dataset_info <- function(dataset_id, .client = NULL) {
  resolve_client(.client)$get_dataset_info(dataset_id)
}

#' @rdname convenience
#' @description
#' `sns_get_fields()` returns a dataset field schema.
#' @export
sns_get_fields <- function(dataset_id, ..., .client = NULL) {
  resolve_client(.client)$get_fields(dataset_id, ...)
}

#' @rdname convenience
#' @description
#' `sns_list_export_formats()` lists available export formats.
#' @export
sns_list_export_formats <- function(dataset_id, ..., .client = NULL) {
  resolve_client(.client)$list_export_formats(dataset_id, ...)
}

#' @rdname convenience
#' @description
#' `sns_get_facets()` returns facet values and counts.
#' @export
sns_get_facets <- function(dataset_id, ..., .client = NULL) {
  resolve_client(.client)$get_facets(dataset_id, ...)
}

#' @rdname convenience
#' @description
#' `sns_query_records()` queries a small filtered record set.
#' @export
sns_query_records <- function(dataset_id, ..., .client = NULL) {
  resolve_client(.client)$query_records(dataset_id, ...)
}

#' @rdname convenience
#' @description
#' `sns_download_data()` downloads a filtered export to cache.
#' @export
sns_download_data <- function(dataset_id, ..., .client = NULL) {
  resolve_client(.client)$download_data(dataset_id, ...)
}

#' @rdname convenience
#' @description
#' `sns_load_data()` loads a cached export.
#' @export
sns_load_data <- function(dataset_id, ..., .client = NULL) {
  resolve_client(.client)$load_data(dataset_id, ...)
}

#' @rdname convenience
#' @description
#' `sns_get_data()` downloads and loads a filtered export.
#' @export
sns_get_data <- function(dataset_id, ..., .client = NULL) {
  resolve_client(.client)$get_data(dataset_id, ...)
}

#' @rdname convenience
#' @description
#' `sns_is_updated()` checks whether a cached export is older than live
#' metadata.
#' @export
sns_is_updated <- function(dataset_id, ..., .client = NULL) {
  resolve_client(.client)$is_updated(dataset_id, ...)
}

#' @rdname convenience
#' @description
#' `sns_list_cached()` lists cached exports.
#' @export
sns_list_cached <- function(..., .client = NULL) {
  resolve_client(.client)$list_cached(...)
}

#' @rdname convenience
#' @description
#' `sns_clear_cache()` clears cached exports.
#' @export
sns_clear_cache <- function(..., .client = NULL) {
  resolve_client(.client)$clear_cache(...)
}
