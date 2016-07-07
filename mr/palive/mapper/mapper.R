library(gsl)
library(BTYD)
library(BTYDplus)
library(hdfs)
library(jsonlite)


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

DATE.FORMAT <- "%Y%m%d"
setup <- function(context) {
  if (is.null(context$basedate)) {
    context$basedate = format(x = Sys.Date(), format = DATE.FORMAT)
  }
  
  context$basedate <- as.Date(x = context$basedate, format = DATE.FORMAT)
  
  dst <- tempfile(pattern = "lifecycle.params.")
  dir.create(dst)
  
  fs <- init.fs(default.fs = context$default.fs)
  fs.get(fs, src = context$params.dir, dst = dst)
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
  
  params = lapply(d[, 2], 
                  function(p) {
                    fromJSON(p)
                  })
  
  names(params) <- d[, 1]
  context$models <- list2env(x = params,
                             hash = TRUE, 
                             parent = emptyenv())
}

map <- function(k, v, context) {
  M.k <- matrix(unlist(strsplit(k, split = "\001")), 
                ncol = 2,
                byrow = TRUE)
  
  M.v <- matrix(unlist(strsplit(v, split = "\001")), 
              ncol = 5,
              byrow = TRUE)
  

  M <- cbind(M.k, M.v)
  rs <- unlist(
    apply(X = M, 
          MARGIN = 1, 
          FUN = function(r) {
            if (exists(x = r[1], envir = context$models)) {
              if (as.numeric(r[3]) == 0) {
                Po <- 0
              } else {
                Po <- cbgcnbd.PAlive(
                  params = get(x = r[1], envir = context$models), 
                  x = as.numeric(r[3]),
                  t.x = as.numeric(r[4]),
                  T.cal = as.numeric(r[7]) - 1)
              }
              
              Pn <- cbgcnbd.PAlive(
                params = get(x = r[1], envir = context$models), 
                x = as.numeric(r[5]), 
                t.x = as.numeric(r[6]), 
                T.cal = as.numeric(r[7]))
              
              paste(r[5], r[6], r[7], Po, Pn, sep = "\001")
            } else {
              NA
            }
          })
  )

  i <- which(!is.na(rs))
  list(k[i], rs[i])
}
