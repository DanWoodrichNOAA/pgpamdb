% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/functions.R
\name{procedure_prog}
\alias{procedure_prog}
\title{Add a layer to bin label explore plot}
\usage{
procedure_prog(conn, procedure_ids)
}
\arguments{
\item{conn}{The database connection}

\item{procedure}{integer id for procedure. multiple can be specified c(1,2,3). see db for code ref}
}
\value{
ggplot plot in first index and numeric underlying data in 2nd index
}
\description{
Add a layer to a bin label explore plot. No easy way to include legend, use for quick comparisons.
}
