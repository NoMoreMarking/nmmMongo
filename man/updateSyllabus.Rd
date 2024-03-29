% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tasks.R
\name{updateSyllabus}
\alias{updateSyllabus}
\title{Set syllabus for a task}
\usage{
updateSyllabus(task, syllabus, connStr)
}
\arguments{
\item{task}{The task id}

\item{syllabus}{The syllabus id}

\item{connStr}{A connection string.}
}
\value{
Nothing
}
\description{
Set syllabus for a task
}
\examples{
updateSyllabus('FYB5kMkoh2v5sLsY7','FYB5kMkoh2v5sLsY8', 'mongodb://')
}
