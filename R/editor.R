#' Get the contents of the editor
#'
#' @param task Task Id
#' @return A data frame with the contents of the editor
#' @examples
#' getEditor(localTask,connStr)
#' @export
getEditor <- function(task,connStr){
  candidates <- mongolite::mongo(db='nmm-vegas-db',collection="candidates",url=connStr)
  qry <- paste0('{"localTask":"',task,'", "hasScans":true}')
  persons <- candidates$find(query = qry,fields = '{"_id" : true}')
  ids <- persons$`_id`
  editor <- mongolite::mongo(db='nmm-vegas-db',collection="editor.contents",url=connStr)
  qry <- paste0('{"_id": {"$in":',toJSON(ids),'}}')
  contents <- editor$find(query = qry,fields = '{"_id" : true, "html":true}')
  return(contents)
}