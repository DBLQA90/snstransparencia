\name{convenience}
\alias{convenience}
\alias{sns_client}
\alias{sns_list_datasets}
\alias{sns_list_all_datasets}
\alias{sns_search_datasets}
\alias{sns_cache_catalogue}
\alias{sns_load_catalogue}
\alias{sns_search_cached_catalogue}
\alias{sns_list_cached_catalogues}
\alias{sns_clear_catalogue_cache}
\alias{sns_get_metadata}
\alias{sns_get_dataset_info}
\alias{sns_get_fields}
\alias{sns_list_export_formats}
\alias{sns_get_facets}
\alias{sns_query_records}
\alias{sns_download_data}
\alias{sns_load_data}
\alias{sns_get_data}
\alias{sns_is_updated}
\alias{sns_list_cached}
\alias{sns_clear_cache}
\title{Convenience Functions}
\usage{
sns_client(
  base_url = "https://transparencia.sns.gov.pt/api/explore/v2.1",
  lang = "pt",
  timeout = 60,
  user_agent = NULL,
  use_cache = FALSE,
  cache_dir = NULL
)

sns_list_datasets(..., .client = NULL)

sns_list_all_datasets(..., .client = NULL)

sns_search_datasets(query, ..., .client = NULL)

sns_cache_catalogue(..., .client = NULL)

sns_load_catalogue(..., .client = NULL)

sns_search_cached_catalogue(query, ..., .client = NULL)

sns_list_cached_catalogues(..., .client = NULL)

sns_clear_catalogue_cache(..., .client = NULL)

sns_get_metadata(dataset_id, ..., .client = NULL)

sns_get_dataset_info(dataset_id, .client = NULL)

sns_get_fields(dataset_id, ..., .client = NULL)

sns_list_export_formats(dataset_id, ..., .client = NULL)

sns_get_facets(dataset_id, ..., .client = NULL)

sns_query_records(dataset_id, ..., .client = NULL)

sns_download_data(dataset_id, ..., .client = NULL)

sns_load_data(dataset_id, ..., .client = NULL)

sns_get_data(dataset_id, ..., .client = NULL)

sns_is_updated(dataset_id, ..., .client = NULL)

sns_list_cached(..., .client = NULL)

sns_clear_cache(..., .client = NULL)
}
\arguments{
\item{base_url}{API base URL. Defaults to the SNS Transparency Explore API v2.1 endpoint.}

\item{lang}{API language, \code{"pt"} or \code{"en"}.}

\item{timeout}{Request timeout in seconds.}

\item{user_agent}{Optional user-agent string.}

\item{use_cache}{Whether \code{get_data()} should reuse local downloads.}

\item{cache_dir}{Cache directory. If \code{NULL}, uses
\code{tools::R_user_dir("snstransparencia", "cache")}.}

\item{...}{Arguments passed to the corresponding \code{\link{SNSClient}} method.}

\item{.client}{Optional \code{\link{SNSClient}} instance. If \code{NULL}, a
default client is created with \code{sns_client()}.}

\item{query}{Search text.}

\item{dataset_id}{Dataset identifier.}
}
\value{
The value returned by the corresponding \code{\link{SNSClient}} method.
\code{sns_client()} returns an \code{\link{SNSClient}} instance.
}
\description{
Functional wrappers around \code{\link{SNSClient}}. These helpers create a
default client when \code{.client} is not supplied, making simple workflows
shorter while still allowing advanced users to pass a configured client.
}
\details{
\code{sns_list_datasets()} lists one page of catalogue datasets.
\code{sns_list_all_datasets()} paginates through catalogue datasets.
\code{sns_search_datasets()} searches the remote catalogue.
\code{sns_cache_catalogue()} downloads and caches a local catalogue snapshot.
\code{sns_load_catalogue()} loads a cached catalogue snapshot.
\code{sns_search_cached_catalogue()} searches a local catalogue snapshot.
\code{sns_list_cached_catalogues()} lists cached catalogue snapshots.
\code{sns_clear_catalogue_cache()} clears cached catalogue snapshots.
\code{sns_get_metadata()} returns raw dataset metadata.
\code{sns_get_dataset_info()} returns a compact one-row dataset summary.
\code{sns_get_fields()} returns a dataset field schema.
\code{sns_list_export_formats()} lists available export formats.
\code{sns_get_facets()} returns facet values and counts.
\code{sns_query_records()} queries a small filtered record set.
\code{sns_download_data()} downloads a filtered export to cache.
\code{sns_load_data()} loads a cached export.
\code{sns_get_data()} downloads and loads a filtered export.
\code{sns_is_updated()} checks whether a cached export is older than live metadata.
\code{sns_list_cached()} lists cached exports.
\code{sns_clear_cache()} clears cached exports.
}
\examples{
sns <- sns_client()

if (identical(Sys.getenv("SNSTRANSPARENCIA_RUN_EXAMPLES"), "true")) {
  sns_list_datasets(limit = 5)
  sns_search_datasets("substancias")
  sns_get_fields("substancias")
  sns_query_records("substancias", columns = c("ano", "cannabis"), limit = 5)
  sns_get_data("substancias", columns = c("ano", "cannabis"))
}
}
