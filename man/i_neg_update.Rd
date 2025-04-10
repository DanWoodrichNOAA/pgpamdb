% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/functions.R
\name{i_neg_update}
\alias{i_neg_update}
\title{Update implied negatives}
\usage{
i_neg_update(conn, fgname, procedure, signal_code, high_freq = NULL)
}
\arguments{
\item{conn}{The database connection}

\item{fgname}{The name of the filegroup.}

\item{procedure}{procedure id for the filegroup-procedure-signal_code combination}

\item{signal_code}{signal id for the filegroup-procedure-signal_code combination}

\item{high_freq}{high frequency for implied negatives. will search the db for indications of what this should be if not provided.}
}
\value{
number of rows inserted corresponding to new implied negative detections.
}
\description{
Update the implied negatives of a filegroup-procedure-signal_code combination
}
