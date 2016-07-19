#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config()
basedir <- job.basedir(conf)
tz.basedir <- job.tz.basedir(conf, basedate, offset)

reduce.tasks <- function(tz.basedir) {
  du <- dfs.du(conf$fs, sprintf("%s/lifecycle/params/part*", basedir))
  as.integer(sum(du$length, na.rm = TRUE) / (128 * 1024 * 1024)) + 1
}


task <- conf$job$tasks$reset
args <- c(
  "--base-date", 
  basedate, 
  
  "--input", 
  sprintf("%s/lifecycle/params/*", tz.basedir),
  
  "--output", 
  sprintf("%s/lifecycle/params/%s", basedir, offset)
)

if (task$overwrite) {
  args <- c(args, "--overwrite")
}

props = task$properties 
props$mapred.reduce.tasks = reduce.tasks(tz.basedir)

cat(print.timestamp(), "* Running lifecycle.reset.\n")

cat("properties:\n")
print(props)

cat("args:\n")
print(args)


output.new <- sprintf("%s/lifecycle/params/%s", basedir, offset)
output.old <- sprintf("%s/lifecycle/params/_%s.%s", basedir, offset, basedate)
dfs.rename(
  conf$fs, 
  output.new,
  output.old
)

tryCatch(
  {
    mr.run(
      fs = conf$fs, 
      jt = conf$jt, 
      jar = file.path(getwd(), "lib", conf$jar$mr), 
      class = task$main,
      args = args,
      props = props
    )
  }, error = function(e) {
    dfs.rm(
      conf$fs,
      output.new
    )
    dfs.rename(
      conf$fs, 
      output.old,
      output.new
    )
    
    stop(e)
  }
)

dfs.rm(
  conf$fs, 
  output.old
)

