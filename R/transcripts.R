#' Get audio transcripts by task ID
#'
#' @param ids A character vector of task IDs.
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
  transcripts <- mongolite::mongo(collection = "audio.transcripts", db = "nmm-vegas-db", url = connStr)
  idStr <- paste(paste0('"', ids, '"'), collapse = ", ")
  qry <- paste0('{"task": {"$in": [', idStr, ']}}')
  result <- transcripts$find(qry, fields = '{"_id": 1, "candidate": 1, "task": 1, "judge": 1, "side": 1, "text": 1, "createdAt": 1}')
  if (nrow(result) > 0) {
    result <- result |> dplyr::rename("id" = "_id")
  }
  return(result)
}
