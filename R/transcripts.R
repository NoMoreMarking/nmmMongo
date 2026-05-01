#' Get audio transcripts for a task
#'
#' @param taskId The task ID.
#' @param connStr A connection string.
#' @return A data frame with transcripts.
#' @examples
#' \dontrun{
#' getTranscripts('e4e50343-e73c-4542-a94e-230dce54096b', 'mongodb://localhost')
#' }
#' @export
#' @import dplyr
getTranscripts <- function(taskId, connStr) {
  transcripts <- mongolite::mongo('audio.transcripts', url = connStr)
  qry <- paste0('{"task":"', taskId, '"}')
  result <- transcripts$find(qry)
  if (nrow(result) > 0) {
    result <- result |> dplyr::rename("id" = "_id")
  }
  return(result)
}
