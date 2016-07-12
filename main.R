#!/usr/bin/Rscript --vanilla

library(RPostgreSQL)

basedir <- function() {
  args <- commandArgs(trailingOnly = FALSE)
  needle <- "--file="
  m <- grep(needle, args)
  if (length(m) > 0) {
    f <- normalizePath(sub(needle, "", args[m]))
  } else {
    f <- normalizePath(sys.frames()[[1]]$ofile)
  }
  
  dirname(f)
}

setwd(basedir())
source("common.R")
source("config.R")


run.task <- function(command, args, wait = TRUE) {
  status <- system2(command, args, wait = wait)
  if (status != 0) {
    stop(sprintf("%s failed.", command))
  }
}


cat(print.timestamp(), "Reading configurations.\n")

basetime <- args.basetime()
cat(sprintf("basetime: %s\n\n", basetime))

conf <- config()
print(conf)


conn <- dbConnect(dbDriver(conf$db$driver),
                  user = conf$db$user,
                  password = conf$db$password,
                  dbname = conf$db$dbname,
                  host = conf$db$host,
                  port = as.integer(conf$db$port))

SQL = sprintf(
  "SELECT DISTINCT ON (s.basedate, s.offset) s.basedate, s.offset FROM (SELECT TO_CHAR(((TO_TIMESTAMP('%s', 'YYYYMMDDHH24MI')::TIMESTAMP WITHOUT TIME ZONE AT TIME ZONE 'Asia/Seoul') AT TIME ZONE b.name), 'YYYYMMDD') AS basedate, TO_CHAR(b.utc_offset, 'HH24MI') AS offset FROM apps a, pg_timezone_names b WHERE a.company_id <> 5 AND a.status_id = 2 AND a.timezone = b.name 
AND TO_CHAR(((TO_TIMESTAMP('%s', 'YYYYMMDDHH24MI')::TIMESTAMP WITHOUT TIME ZONE AT TIME ZONE 'Asia/Seoul') AT TIME ZONE b.name), 'HH24MI') 
BETWEEN '0000' AND '0029') s", basetime, basetime
)

d <- dbGetQuery(conn, SQL)
invisible(dbDisconnect(conn))

if (nrow(d) > 0) {
  d$basedate <- format(as.Date(d$basedate, format = "%Y%m%d") - 1, format = "%Y%m%d")
  
  for (i in 1:nrow(d)) {
    basedate <- d$basedate[i]
    
    if (grepl("^-", d$offset[i])) {
      offset <- sprintf("B%04d", as.integer(substr(d$offset[i], 2, nchar(d$offset[i]))))  
    } else {
      offset <- sprintf("A%04d", as.integer(d$offset[i]))
    }
    
    cat(print.timestamp(), "** Running analytics job.\n")
    cat(sprintf("basedate: %s\n", basedate))
    cat(sprintf("timezone offset: %s\n", offset))
    
    args <- c(
      "--base-date", basedate, 
      "--offset", offset
    )
    

    tz.basedir <- job.tz.basedir(conf, basedate, offset)

    z <- FALSE
    z <- fs.exists(conf$fs, sprintf("%s/attributes", tz.basedir))
    
    if (!z) {
      cat(print.timestamp(), "No input to process.\n")
    } else {
      command <- file.path(getwd(), "rfm.R")
      run.task(command, args)

      command <- file.path(getwd(), "grouping.R")
      run.task(command, args)

      #command <- file.path(getwd(), "params.R")
      #run.task(command, args)
      
      #command <- file.path(getwd(), "reset_params.R")
      #run.task(command, args)
 
      command <- file.path(getwd(), "palive.R")
      run.task(command, args)
      
      command <- file.path(getwd(), "phases.R")
      run.task(command, args)

      command <- file.path(getwd(), "dbload.R")
      run.task(command, args)
    }
  }
} else {
  cat(print.timestamp(), "No clients to process.\n")
}
