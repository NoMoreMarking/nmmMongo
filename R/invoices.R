#' Get invoice details from an invoice number
#'
#' @param invoice The invoice number
#' @param connStr A connection string.
#' @return A data frame with invoice
#' @examples
#' \dontrun{
#' getInvoiceByNumber(123, 'mongodb://localhost')
#' }
#' @export
getInvoiceByNumber <- function(invoice, connStr) {
  invoices_collection <- mongolite::mongo(db = 'nmm-vegas-db', collection = "invoices", url = connStr)
  qryString <- paste0('{"no":', invoice, '}')
  invoices_collection$find(query = qryString)
}

#' Get invoice details from dfe number
#'
#' @param dfe The dfe number
#' @param connStr A connection string.
#' @return A data frame with invoices
#' @examples
#' \dontrun{
#' getInvoicesByDfe(123, 'mongodb://localhost')
#' }
#' @export
getInvoicesByDfe <- function(dfe, connStr) {
  invoices_collection <- mongolite::mongo(db = 'nmm-vegas-db', collection = "invoices", url = connStr)
  qryString <- paste0('{"dfe":', dfe, '}')
  invoices_collection$find(query = qryString)
}

#' Create a new invoice
#'
#' @param dfe dfe number
#' @param product product id
#' @param purchase_id purchase id
#' @param no invoice number
#' @param connStr A connection string.
#' @return A string with confirmation
#' @examples
#' \dontrun{
#' createInvoice(dfe, product, purchase_id, no, 'mongodb://localhost')
#' }
#' @export
createInvoice <- function(dfe, product, purchase_id, no, connStr) {
  new_invoice_id <- uuid::UUIDgenerate()
  created_at <- format(lubridate::today(), "%Y-%m-%d")
  expires_at <- format(lubridate::today() + lubridate::days(30), "%Y-%m-%d")
  invoices_collection <- mongolite::mongo(db = 'nmm-vegas-db', collection = "invoices", url = connStr)
  new_invoice <- tibble::tibble(
    `_id`     = new_invoice_id,
    dfe       = dfe,
    product   = product,
    purchase  = purchase_id,
    no        = no,
    createdAt = created_at,
    expiresAt = expires_at,
    newUtilities = TRUE
  )
  result <- invoices_collection$insert(new_invoice)
  row_string <- with(new_invoice,
    sprintf("Invoice #%s for %s - Created: %s", no, dfe, createdAt))
  paste("Invoices created: ", result$nInserted, row_string)
}
