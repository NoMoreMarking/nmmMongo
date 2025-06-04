#' Get invoice details from an invoice number
#'
#' @param invoice The invoice number
#' @param connStr A connection string.
#' @return A data frame with invoice
#' @examples
#' getInvoiceByNumber(123, 'mongodb://')
#' @export
getInvoiceByNumber <- function(invoice,connStr){
  invoiceCollection <- mongolite::mongo(db='nmm-vegas-db',collection="invoices",url=connStr)
  qryString <- paste0('{"no":',invoice,'}')
  invoiceObject <- invoiceCollection$find(query = qryString)
  if(nrow(invoiceObject)>0){
    return(invoiceObject)
  } else {
    cat('no invoice found for that number') 
  }
}