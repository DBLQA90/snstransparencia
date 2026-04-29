# snstransparencia

`snstransparencia` is an R client for the Portuguese SNS Transparencia open
data API.

It helps you:

- discover datasets in the SNS catalogue;
- cache the catalogue locally and search it offline;
- inspect dataset metadata, fields, facets, and export formats;
- query small filtered record sets;
- download filtered exports with selected columns;
- reuse local cached downloads instead of repeatedly querying the API;
- catch common field-name mistakes before sending record/export requests.

The package uses the public Opendatasoft Explore API v2.1 endpoint:

```text
https://transparencia.sns.gov.pt/api/explore/v2.1
```

## Status

This package is under active development and is not yet on CRAN.

The current API is useful, but still early. Method names and arguments may
change before publication.

## Installation

Once the GitHub repository is available:

```r
# install.packages("pak")
pak::pak("DBLQA90/snstransparencia")
```

From the local package directory:

```r
install.packages(
  "/home/dblqa/Desktop/Brincadeiras/ineptR2/snstransparencia",
  repos = NULL,
  type = "source"
)
```

During development you can also load it directly:

```r
pkgload::load_all("/home/dblqa/Desktop/Brincadeiras/ineptR2/snstransparencia")
```

## Create a client

```r
library(snstransparencia)

sns <- SNSClient$new()
```

Use a cache directory if you want to reuse catalogue snapshots and downloaded
exports:

```r
sns <- SNSClient$new(
  use_cache = TRUE,
  cache_dir = tools::R_user_dir("snstransparencia", "cache")
)
```

## Convenience functions

Most workflows can also use package-level `sns_*()` functions. These create a
default client internally:

```r
sns_list_datasets(limit = 5)
sns_get_fields("substancias")
sns_query_records("substancias", columns = c("ano", "cannabis"), limit = 2)
```

When you need shared cache settings or a custom user agent, create one client
and pass it with `.client`:

```r
sns <- sns_client(
  use_cache = TRUE,
  cache_dir = tools::R_user_dir("snstransparencia", "cache")
)

data <- sns_get_data(
  "substancias",
  columns = c("ano", "cannabis"),
  .client = sns
)
```

## Copy-paste examples with substancias

These examples use `substancias`, a small dataset that has been used for live
smoke tests while developing the package.

Inspect the dataset:

```r
library(snstransparencia)

sns <- SNSClient$new()

sns$get_dataset_info("substancias")
sns$get_fields("substancias")
sns$get_facets("substancias", facets = "ano")
```

Query two rows with selected columns:

```r
sns$query_records(
  "substancias",
  columns = c("ano", "cannabis"),
  date_field = "ano",
  date_from = "2026-01",
  order_by = "ano asc",
  limit = 2
)
```

Expected shape:

```text
ano      cannabis
2026-01  87
2026-02  170
```

Download selected columns and load them:

```r
manifest <- sns$download_data(
  "substancias",
  format = "csv",
  columns = c("ano", "cannabis"),
  date_field = "ano",
  date_from = "2026-01",
  order_by = "ano asc",
  limit = 2
)

data <- sns$load_data("substancias", cache_key = manifest$cache_key)
data
```

Or do the same thing in one step:

```r
data <- sns$get_data(
  "substancias",
  format = "csv",
  columns = c("ano", "cannabis"),
  date_field = "ano",
  date_from = "2026-01",
  order_by = "ano asc",
  limit = 2
)
```

## Discover datasets

List the most recently modified datasets:

```r
sns$list_datasets(limit = 5)
```

Example shape:

```text
dataset_id                                      title                         records_count publisher
rastreios-oncologicos                           Rastreios Oncologicos         6989          DE-SNS
evolucao-do-numero-de-unidades-funcionais       Unidades Funcionais           7419          DE-SNS
```

Search the catalogue through the API:

```r
sns$search_datasets("substancias", limit = 10)
```

Retrieve all catalogue rows for a query by paging through the API:

```r
catalogue <- sns$list_all_datasets()
```

For a smaller development call:

```r
catalogue <- sns$list_all_datasets(page_size = 25, max_records = 100)
```

## Cache and search the catalogue locally

Create a local catalogue snapshot:

```r
catalogue <- sns$cache_catalogue()
```

Search that local snapshot without querying the API again:

```r
sns$search_cached_catalogue("diabetes")
```

Example result:

```text
dataset_id  title                              records_count publisher
diabetes    Atividade do Programa de Diabetes  6999          ACSS
```

Inspect cached catalogue snapshots:

```r
sns$list_cached_catalogues()
```

Load the latest cached catalogue:

```r
catalogue <- sns$load_catalogue()
```

Clear catalogue snapshots:

```r
sns$clear_catalogue_cache()
```

Catalogue caches are kept separate from dataset export caches.

## Inspect one dataset

The examples below use the dataset `substancias`, which has been used for live
smoke tests during development.

Get a compact dataset summary:

```r
sns$get_dataset_info("substancias")
```

Example shape:

```text
dataset_id    title        publisher records_count theme
substancias   Substancias  ICAD      122           Saude dos Portugueses
```

Inspect fields:

```r
sns$get_fields("substancias")
```

Example:

```text
name                         label                       type
ano                          Periodo                     date
cannabis                     Cannabis                    int
cocaina_e_derivados          Cocaina e Derivados         int
heroina_e_outros_opiaceos    Heroina e Outros Opiaceos   int
outras_substancias           Outras Substancias          int
```

Inspect available export formats:

```r
sns$list_export_formats("substancias")
```

Common formats include:

```text
csv
json
jsonl
parquet
```

Inspect facet values:

```r
sns$get_facets("substancias", facets = "ano")
```

Example:

```text
facet value count
ano   2026  2
ano   2025  12
ano   2024  12
```

## Query a small number of records

Use `query_records()` for small, interactive result sets from the API records
endpoint:

```r
sns$query_records(
  "substancias",
  columns = c("ano", "cannabis"),
  date_field = "ano",
  date_from = "2026-01",
  order_by = "ano asc",
  limit = 2
)
```

Example result:

```text
ano      cannabis
2026-01  87
2026-02  170
```

You can combine selected columns, facet filters, dates, and ordering:

```r
sns$query_records(
  "substancias",
  columns = c("ano", "cannabis", "alcool"),
  filters = list(ano = "2026"),
  date_field = "ano",
  date_from = "2026-01",
  order_by = "ano asc",
  limit = 10
)
```

## Download data

Use `download_data()` when you want the API export endpoint and a local cached
file:

```r
manifest <- sns$download_data(
  "substancias",
  format = "csv",
  columns = c("ano", "cannabis"),
  date_field = "ano",
  date_from = "2026-01",
  order_by = "ano asc"
)
```

The returned manifest describes the cached export:

```r
manifest$path
manifest$cache_key
```

Load the cached export:

```r
data <- sns$load_data("substancias", cache_key = manifest$cache_key)
```

Or download and load in one step:

```r
data <- sns$get_data(
  "substancias",
  format = "csv",
  columns = c("ano", "cannabis"),
  date_field = "ano",
  date_from = "2026-01",
  order_by = "ano asc"
)
```

Supported loading formats are:

- `csv`
- `json`
- `jsonl`
- `parquet`, if the optional `arrow` package is installed

## Offline/cache workflow

The cache workflow is one of the main reasons to use this package. You can
discover datasets once, download the filtered data you need, and then work from
local files.

Create a cache-enabled client:

```r
library(snstransparencia)

sns <- SNSClient$new(
  use_cache = TRUE,
  cache_dir = tools::R_user_dir("snstransparencia", "cache")
)
```

Cache the catalogue once:

```r
catalogue <- sns$cache_catalogue()
```

Search the cached catalogue locally:

```r
sns$search_cached_catalogue("diabetes")
sns$search_cached_catalogue("substancias")
```

Download and cache a filtered export:

```r
first <- sns$get_data(
  "substancias",
  format = "csv",
  columns = c("ano", "cannabis"),
  date_field = "ano",
  date_from = "2026-01",
  order_by = "ano asc"
)
```

Run the same request again. With `use_cache = TRUE`, it reuses the local export:

```r
second <- sns$get_data(
  "substancias",
  format = "csv",
  columns = c("ano", "cannabis"),
  date_field = "ano",
  date_from = "2026-01",
  order_by = "ano asc"
)
```

List local catalogue snapshots and data exports:

```r
sns$list_cached_catalogues()
sns$list_cached()
```

Load from cache directly:

```r
catalogue <- sns$load_catalogue()
data <- sns$load_data("substancias", format = "csv")
```

These two load calls read local files and do not call the API. By contrast,
`is_updated()` checks live metadata, so it needs API access.

Check whether a cached export is older than the live dataset metadata:

```r
sns$is_updated("substancias")
```

Force a fresh download when needed:

```r
data <- sns$get_data(
  "substancias",
  columns = c("ano", "cannabis"),
  refresh = TRUE
)
```

Clear cached exports:

```r
sns$clear_cache("substancias")
```

Clear catalogue snapshots:

```r
sns$clear_catalogue_cache()
```

## Validation and raw ODSQL

Validation is on by default for the user-friendly query and download helpers:

```r
sns$query_records("substancias", columns = "ano", validate = TRUE)
sns$get_data("substancias", columns = "ano", validate = TRUE)
sns$download_data("substancias", columns = "ano", validate = TRUE)
sns$get_facets("substancias", facets = "ano", validate = TRUE)
```

The package checks simple field names against dataset metadata before querying
records or downloading exports. This catches common mistakes locally, before a
larger API request is made.

For example:

```r
sns$query_records("substancias", columns = "not_a_field")
```

returns a local error similar to:

```text
Unknown field in `columns` for dataset 'substancias'.
not_a_field
Available fields include: ano, cannabis, cocaina_e_derivados, ...
```

Date fields are checked too:

```r
sns$query_records(
  "substancias",
  columns = "ano",
  date_field = "cannabis",
  date_from = "2026-01"
)
```

returns:

```text
`date_field` must be a date or datetime field for dataset 'substancias'.
Field 'cannabis' has type 'int'.
```

For normal use, prefer user-friendly arguments:

```r
sns$query_records(
  "substancias",
  columns = c("ano", "cannabis"),
  filters = list(ano = "2026"),
  date_field = "ano",
  date_from = "2026-01"
)
```

For advanced Opendatasoft SQL expressions, use the raw API arguments and set
`validate = FALSE`:

```r
sns$query_records(
  "substancias",
  select = "ano, sum(cannabis) as cannabis_total",
  group_by = "ano",
  order_by = "ano asc",
  validate = FALSE
)
```

Raw pass-through arguments include `select`, `where`, `order_by`, `group_by`,
raw `refine`, and raw `exclude`.

## Development checks

From the package directory:

```sh
Rscript -e 'testthat::test_local()'
R CMD build --no-build-vignettes .
R CMD check --no-manual --no-vignettes snstransparencia_0.1.0.tar.gz
```

At the time this README was added, the package checked with:

```text
testthat: FAIL 0, WARN 0, SKIP 0, PASS 119
R CMD check: Status OK
```

## Notes

- The API can change independently of this package.
- Large downloads should use `download_data()` or `get_data()`, not
  `query_records()`.
- The default cache directory is `tools::R_user_dir("snstransparencia",
  "cache")`.
- `PROJECT_STATE.md` contains a local development checkpoint for recovering
  context if needed.
