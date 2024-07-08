#' Get image paths
#'
#' @param task The id of the local task
#' @param connStr A connection string
#' @return A data frame with details from the scans.Pages collection
#' @examples
#' getImages('abc', 'mongodb://') 
#' @export
getImages <- function(task,connStr){
  scansCollection <- mongolite::mongo(db='nmm-vegas-db',collection="scans.Pages",url=connStr)
  qryString <- paste0('{"task":"',task,'"}')
  imageList <- scansCollection$find(query = qryString,fields = '{"qrcode" : true, "url":true, "thumbnail0": true, "pdfpage":true, "createdAt":true}')
  if(nrow(imageList)==0){
    cat('No images found for', task,'.\n')
  }
  return(imageList)
}