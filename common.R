library(MR1.CDH4.7.1)

print.timestamp <- function() {
  strftime(Sys.time(), format="%Y-%m-%d %H:%M:%S") 
}

is.empty <- function(x) {
  identical(x, NA) || is.null(x) || length(x) == 0
}

job.basedir <- function(conf) {
  conf$job$base_dir
}

job.tz.basedir <- function(conf, basedate, offset) {
  sprintf("%s/%s/%s", job.basedir(conf), basedate, offset)
}

args.basetime <- function() {
  args <- commandArgs(TRUE)
  
  t0 <- NA
  m <- match("--base-time", args, 0L)
  if (m) {
    t0 <- args[m + 1]
  }
  
  t0
}

args.basedate <- function() {
  args <- commandArgs(TRUE)
  
  t0 <- NA
  m <- match("--base-date", args, 0L)
  if (m) {
    t0 <- args[m + 1]
  }
  
  t0
}

args.offset <- function() {
  args <- commandArgs(TRUE)
  
  t0 <- NA
  m <- match("--offset", args, 0L)
  if (m) {
    t0 <- args[m + 1]
  }
  
  t0
}

args.clients <- function() {
  args <- commandArgs(TRUE)

  t0 <- NA
  m <- match("--clients", args, 0L)
  if (m) {
    t0 <- args[m + 1]
  }

  t0
}

fs.exists <- function(fs, input) {
  ls <- dfs.ls(fs, input)
  if (nrow(ls) == 0) {
    FALSE
  } else {
    du <- subset(dfs.du(fs, input), subset = !grepl("/[_\\.][^/]*$", file), select = length)
    sum(du$length, na.rm = TRUE) > 0
  }
}
