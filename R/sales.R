#' Get sales data
#'
#' @param project The project id
#' @param connStr A connection string.
#' @return data frame product data
#' @examples
#' getSales('vqPwL2wWX8qc26nWs','mongodb://')
#' @export
getSales <- function(product,connStr){
  basket <- mongolite::mongo(db='nmm-vegas-db',collection="purchases",url=connStr)
  qryString <- paste0('{"product":"',product,'"}')
  salesList <- basket$find(query = qryString)
  sales <- jsonlite::flatten(salesList)
  return(sales)
}