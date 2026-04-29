\name{SNSClient}
\alias{SNSClient}
\title{SNS Transparency API Client}
\description{
R6 client for the Portuguese SNS Transparency API. The client supports
catalogue discovery, local catalogue caching, dataset metadata, field schemas,
metadata-backed validation for common query inputs, small filtered record
queries, facet discovery, export downloads, local caching, and update checks
using the Opendatasoft Explore API v2.1.
}
\usage{
SNSClient
}
\examples{
sns <- SNSClient$new()

if (identical(Sys.getenv("SNSTRANSPARENCIA_RUN_EXAMPLES"), "true")) {
  sns$list_datasets(limit = 5)
  sns$list_all_datasets(max_records = 100)
  sns$cache_catalogue(max_records = 100)
  sns$search_cached_catalogue("substancias")
  sns$list_cached_catalogues()
  sns$get_dataset_info("substancias")
  sns$get_fields("substancias")
  sns$get_facets("substancias", facets = "ano")
  sns$list_export_formats("substancias")
  sns$query_records("substancias", columns = c("ano", "cannabis"), limit = 5)
  sns$get_data("substancias", columns = c("ano", "cannabis"))
}
}
