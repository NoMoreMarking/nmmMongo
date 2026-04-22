#' Get sales data
#'
#' @param product The product id
#' @param connStr A connection string.
#' @return data frame product data
#' @examples
#' \dontrun{
#' getSales('vqPwL2wWX8qc26nWs', 'mongodb://localhost')
#' }
#' @export
getSales <- function(product,connStr){
  basket <- mongolite::mongo(db='nmm-vegas-db',collection="purchases",url=connStr)
  qryString <- paste0('{"product":"',product,'"}')
  salesList <- basket$find(query = qryString)
  sales <- jsonlite::flatten(salesList)
  return(sales)
}