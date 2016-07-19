#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config()
tz.basedir <- job.tz.basedir(conf, basedate, offset)


task <- conf$job$tasks$phases
args <- c(
  "--base-date", 
  basedate, 
  
  "--input", 
  sprintf("%s/lifecycle/palive/*", tz.basedir),
  
  "--output", 
  sprintf("%s/lifecycle/phases", tz.basedir),

  "--intermediate",
  sprintf("%s/lifecycle/phases.tmp", tz.basedir)
)

if (task$overwrite) {
  args <- c(args, "--overwrite")
}


cat(print.timestamp(), "* Running lifecycle.phases.\n")

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
