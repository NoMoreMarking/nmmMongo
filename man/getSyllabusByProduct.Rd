% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/syllabus.R
\name{getSyllabusByProduct}
\alias{getSyllabusByProduct}
\title{Get syllabus object from a product name}
\usage{
getSyllabusByProduct(product, connStr)
}
\arguments{
\item{product}{The product for the syllabus}

\item{connStr}{A connection string.}
}
\value{
A data frame with syllabus objects
}
\description{
Get syllabus object from a product name
}
\examples{
getSyllabusByProduct('productid', 'mongodb://') 
}
