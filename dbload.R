#!/usr/bin/Rscript --vanilla

library(RPostgreSQL)

source("common.R")
source("config.R")


filter <- function(path) {
  f <- character(0)
  
  l <- list.files(path, pattern = "^[^_.]", full.names = TRUE, include.dirs = TRUE)
  repeat {
    isdir <- file.info(l)$isdir
    
    f <- c(f, l[which(!isdir)])
    if (all(!isdir)) {
      break
    }
    
    l <- list.files(l[which(isdir)], pattern = "^[^_.]", full.names = TRUE, include.dirs = TRUE)
  }
  
  f
}


conf <- config()

basedate <- strftime(as.Date(args.basedate(), format = "%Y%m%d"), format = "%Y-%m-%d")
offset <- args.offset()
tz.basedir <- job.tz.basedir(conf, basedate, offset) 

src <- sprintf("%s/lifecycle/phases", tz.basedir)
dst <- tempfile(pattern = "lifecycle-phases.")
dir.create(dst)

dfs.get(conf$fs, src = src, dst = dst, src.del = FALSE)
d <- do.call("rbind", 
             lapply(filter(dst), 
                    function(f) {
                      if (file.info(f)$size > 0) {
                        read.table(file = f, header = FALSE, sep = "\t", quote = "", stringsAsFactors = FALSE)
                      } else {
                        NULL
                      }
                    }))


unlink(x = dst, recursive = TRUE, force = TRUE)

conn <- dbConnect(dbDriver(conf$db$driver),
                 user = conf$db$user,
                 password = conf$db$password,
                 dbname = conf$db$dbname,
                 host = conf$db$host,
                 port = conf$db$port)


dbGetQuery(conn, "BEGIN TRANSACTION")
apply(X = d, 
      MARGIN = 1,
      FUN = function(x) {
        M <- matrix(as.integer(unlist(strsplit(unlist(strsplit(x[2], split = "\001")), split = "\002"))), 
                    ncol = 3,
                    byrow = TRUE)
        
        n <- sum(M[which(M[, 1] == 0 & M[, 2] == 1), 3])
        a <- sum(M[which(M[, 1] != 4 & M[, 2] == 2), 3])
        r <- sum(M[which(M[, 1] != 4 & M[, 2] == 3), 3])
        l <- sum(M[which(M[, 2] == 4), 3])
        w <- sum(M[which(M[, 1] == 4 & M[, 2] != 4), 3])
        
        a0 <- sum(M[which(M[, 1] != 4 & M[, 1] != 2 & M[, 2] == 2), 3])
        r0 <- sum(M[which(M[, 1] != 4 & M[, 1] != 3 & M[, 2] == 3), 3])
        l0 <- sum(M[which(M[, 1] != 4 & M[, 2] == 4), 3])
        
        
        #dbSendQuery(con, sprintf("INSERT INTO lifecycle_states (client_id, new, active, risky, gone, regain, base_date) VALUES (%s, %d, %d, %d, %d, %d, %s)", 
        #                         r[1], n, a, r, l, w, basedate))
        
        #dbSendQuery(con, sprintf("INSERT INTO lifecycle_state_transitions (client_id, new, active, risky, gone, regain, base_date) VALUES (%s, %d, %d, %d, %d, %d, %s)", 
        #                         r[1], n, a0, r0, l0, w, basedate))       
 
      })

dbCommit(conn)
dbDisconnect(conn)
