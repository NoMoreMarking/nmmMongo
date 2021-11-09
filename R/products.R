#' Get product from a product id
#'
#' @param id The id of the product
#' @param connStr A connection string
#' @export
getProduct <- function(id, connStr){
  productCollection <- mongolite::mongo(db='nmm-vegas-db',collection="products",url=connStr)
  qryString <- paste0('{"_id":"',id,'"}')
  productList <- productCollection$find(query=qryString, fields = '{"productName":true}',limit = 1)
  if('productName' %in% names(productList)) return (productList)
  return(paste0('no product with id: ',id))
}