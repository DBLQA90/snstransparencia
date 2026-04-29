json_response <- function(x, status = 200) {
  httr2::response(
    status_code = status,
    headers = list("content-type" = "application/json"),
    body = charToRaw(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"))
  )
}

csv_response <- function(x, status = 200) {
  httr2::response(
    status_code = status,
    headers = list("content-type" = "text/csv; charset=utf-8"),
    body = charToRaw(x)
  )
}

metadata_payload <- function(modified = "2026-01-01T00:00:00+00:00",
                             data_processed = modified,
                             records_count = 1) {
  list(
    dataset_id = "substancias",
    fields = list(
      list(name = "ano", label = "Periodo", type = "date",
           description = NULL, annotations = list(facet = TRUE)),
      list(name = "cannabis", label = "Cannabis", type = "int",
           description = NULL, annotations = list())
    ),
    metas = list(default = list(
      description = "<p>Monitorizar substancias.</p>",
      publisher = "ICAD",
      theme = list("Saude dos Portugueses"),
      keyword = list("Substancias"),
      title = "Substancias",
      records_count = records_count,
      modified = modified,
      data_processed = data_processed
    ), dcat = list(
      creator = "SPMS",
      contributor = "ICAD",
      accrualperiodicity = "Mensal",
      temporal = "janeiro 2016 a fevereiro 2026",
      spatial = "Portugal"
    ))
  )
}
