#' Get audio transcripts by ID
#'
#' @param ids A character vector of transcript IDs.
#' @param connStr A connection string.
#' @return A data frame with transcripts.
#' @examples
#' \dontrun{
#' getTranscripts('e4e50343-e73c-4542-a94e-230dce54096b', 'mongodb://localhost')
#' getTranscripts(c('e4e50343-e73c-4542-a94e-230dce54096b', 'abc123'), 'mongodb://localhost')
#' }
#' @export
#' @import dplyr
getTranscripts <- function(ids, connStr) {
  transcripts <- mongolite::mongo('audio.transcripts', url = connStr)
  idStr <- paste(shQuote(ids, type = "cmd"), collapse = ", ")
  qry <- paste0('{"_id": {"$in": [', idStr, ']}}')
  result <- transcripts$find(qry)
  if (nrow(result) > 0) {
    result <- result |> dplyr::rename("id" = "_id")
  }
  return(result)
}
