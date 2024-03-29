% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/levels.R
\name{getLevels}
\alias{getLevels}
\title{Get levels data}
\usage{
getLevels(task, connStr)
}
\arguments{
\item{task}{The task id}

\item{connStr}{A connection string.}
}
\value{
data frame with the task levels
}
\description{
Get levels data
}
\examples{
getLevels('74537cb5-d97e-4d48-b89a-97a81b91cd34','mongodb://')
}
