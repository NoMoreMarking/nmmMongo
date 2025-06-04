#' Get product from a product id
#'
#' @param id The id of the product
#' @param connStr A connection string
#' @export
getProduct <- function(id, connStr){
  productCollection <- mongolite::mongo(db='nmm-vegas-db',collection="products",url=connStr)
  qryString <- paste0('{"_id":"',id,'"}')
  productList <- productCollection$find(query=qryString, fields = '{"emailContent":false, "sPoints": false}',limit = 1)
  if('productName' %in% names(productList)) return (productList)
  return(paste0('no product with id: ',id))
}

#' Get product id from a product name
#'
#' @param name The name of the product
#' @param connStr A connection string
#' @export
getProductByName <- function(name, connStr){
  productCollection <- mongolite::mongo(db='nmm-vegas-db',collection="products",url=connStr)
  qryString <- paste0('{"productName":"',name,'"}')
  productList <- productCollection$find(query=qryString, fields = '{"emailContent":false, "sPoints": false}',limit = 1)
  if('productName' %in% names(productList)) return (productList)
  return(paste0('no product with name: ',name))
}