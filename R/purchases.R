#' Get purchase from a purchase id
#'
#' @param id The id of the purchase
#' @param connStr A connection string
#' @export
getPurchase <- function(id, connStr){
  purchaseCollection <- mongolite::mongo(db='nmm-vegas-db',collection="purchases",url=connStr)
  qryString <- paste0('{"_id":"',id,'"}')
  purchase <- purchaseCollection$find(query=qryString)
  if(nrow(purchase>0)) return (purchase)
  return(paste0('no purchase with id: ',id))
}

#' Get purchase from a product id
#'
#' @param id The id of the product
#' @param connStr A connection string
#' @export
getPurchaseByProduct <- function(id, connStr){
  purchaseCollection <- mongolite::mongo(db='nmm-vegas-db',collection="purchases",url=connStr)
  qryString <- paste0('{"product":"',id,'"}')
  purchase <- purchaseCollection$find(query=qryString)
  purchase <- purchase %>% 
    select(-any_of(c("migrated", "ygs", "pop", "Xwithdrawn")))
  if(nrow(purchase)>0) return (purchase)
  return(paste0('no purchase with product id: ',id))
}