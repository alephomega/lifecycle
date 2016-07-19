#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config()
tz.basedir <- job.tz.basedir(conf, basedate, offset)


task <- conf$job$tasks$rfm 
args <- c(
  "--base-date", 
  basedate, 
  
  "--input", 
  sprintf("%s/attributes/{NEW,ACTIVE,ATRISK,LOST,WINBACK}-*", tz.basedir),
  
  "--output", 
  sprintf("%s/lifecycle/rfm", tz.basedir)
)

if (task$overwrite) {
  args <- c(args, "--overwrite")
}


cat(print.timestamp(), "* Running lifecycle.RFM.\n")

cat("properties:\n")
print(task$properties)

cat("args:\n")
print(args)


mr.run(
  fs = conf$fs,
  jt = conf$jt,
  jar = file.path(getwd(), "lib", conf$jar$mr),
  class = task$main,
  args = args,
  props = task$properties
)
