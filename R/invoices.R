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

#' Get invoice details from dfe number
#'
#' @param dfe The dfe number
#' @param connStr A connection string.
#' @return A data frame with invoices
#' @examples
#' getInvoicesByDfe(123, 'mongodb://')
#' @export
getInvoicesByDfe <- function(dfe,connStr){
  invoiceCollection <- mongolite::mongo(db='nmm-vegas-db',collection="invoices",url=connStr)
  qryString <- paste0('{"dfe":',dfe,'}')
  invoiceObject <- invoiceCollection$find(query = qryString)
  if(nrow(invoiceObject)>0){
    return(invoiceObject)
  } else {
    cat('no invoices found for ', dfe) 
  }
}


#' Create a new invoice
#'
#' @param dfe dfe number
#' @param product product id
#' @param purchase purchase id
#' @param no invoice number
#' @param connStr A connection string.
#' @return A string with confirmation
#' @examples
#' createInvoice(dfe, product, purchase, no, 'mongodb://')
#' @export
createInvoice <- function(dfe, product, purchase_id, no, connStr){
  new_invoice_id <- as.character(uuid::UUIDgenerate())
  created_at = format(lubridate::today(), "%Y-%m-%d")
  expires_at = format(lubridate::today() + lubridate::days(30), "%Y-%m-%d")
  # Connect to the 'invoices' collection using your existing connection string
  invoices_collection <- mongolite::mongo(db='nmm-vegas-db',collection="invoices",url=connStr)
  
  # Create the document to insert
  new_invoice <- tibble(
    `_id` = new_invoice_id,
    dfe = dfe,
    product = product,
    purchase = purchase_id,
    no = no,
    createdAt = created_at,
    expiresAt = expires_at,
    newUtilities = TRUE
  )
  
  # Insert the document into the collection
  result <- invoices_collection$insert(new_invoice)
  row_string <- with(new_invoice, 
                     sprintf("Invoice #%s for %s - Created: %s", 
                             no, dfe, createdAt))
  return(paste("Invoices created: ",result$nInserted, row_string))
  
}