#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

reduce.tasks <- function(fs, input) {
  du <- dfs.du(fs, input)
  as.integer(sum(du$length, na.rm = TRUE) / (128 * 1024 * 1024)) + 1
}


basedate <- args.basedate()
offset <- args.offset()

conf <- config()
tz.basedir <- job.tz.basedir(conf, basedate, offset)


cat(print.timestamp(), "* Running lifecycle.hiveload.\n")

task <- conf$job$tasks$hiveload
input <- sprintf("%s/lifecycle/palive/*", tz.basedir)
if (fs.exists(conf$fs, input)) {
  args <- c(
    "--base-date", 
    basedate, 
    
    "--input", 
    input,
    
    "--database", 
    task$output$database,
    
    "--table", 
    task$output$table
  )
  
  props = task$properties 
  props$mapred.reduce.tasks = reduce.tasks(conf$fs, input)

  cat("properties:\n")
  print(props)
  
  cat("args:\n")
  print(args)
  
  
  mr.run(
    fs = conf$fs,
    jt = conf$jt,
    jar = file.path(getwd(), "lib", conf$jar$mr),
    class = task$main,
    args = args,
    props = props
  )
} else {
  cat(print.timestamp(), "No input to process.\n") 
}
