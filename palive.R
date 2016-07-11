#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config()
tz.basedir <- job.tz.basedir(conf, basedate, offset)


task <- conf$job$tasks$palive
args <- c(
  "--input", 
  sprintf("%s/lifecycle/rfm/*", tz.basedir),
  
  "--output", 
  sprintf("%s/lifecycle/palive", tz.basedir),

  "--map-input-key-class",
  "org.apache.hadoop.io.Text",

  "--map-input-value-class",
  "org.apache.hadoop.io.Text",

  "--reduce-input-key-class",
  "org.apache.hadoop.io.Text",

  "--reduce-input-value-class",
  "org.apache.hadoop.io.Text",

  "--map-output-key-class",
  "org.apache.hadoop.io.Text",

  "--map-output-value-class",
  "org.apache.hadoop.io.Text",

  "--reduce-output-key-class",
  "org.apache.hadoop.io.Text",

  "--reduce-output-value-class",
  "org.apache.hadoop.io.Text",

  "--input-format",
  "org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat",

  "--jri-name",
  "valuepotion.analytics.lifecycle.palive",

  "--jri-mapper", 
  "mapper.R",

  "--map-params", 
  sprintf("{\"params.dir\":\"%s/lifecycle/params/%s\", \"basedate\":\"%s\"}", job.basedir(conf), offset, basedate),

  "--jri-lib",
  "/usr/lib64/R/library/rJava/jri/",

  "--r-home",
  "/usr/lib64/R",

  "--jri-deps",
  "gsl,elliptic,contfrac,hypergeo,BTYD,Rcpp,chron,plyr,reshape2,data.table,lattice,coda,BTYDplus",

  "--jri-repository",
  "git@github.com:valuepotion/CLV.git"
)

if (task$overwrite) {
  args <- c(args, "--overwrite")
}


cat(print.timestamp(), "* Running lifecycle.palive.\n")

cat("properties:\n")
print(task$properties)

cat("args:\n")
print(args)

mr.run(
  fs = conf$fs,
  jt = conf$jt,
  jar = file.path(getwd(), "lib", conf$jar.rmr),
  libjars = list.files(path = file.path(getwd(), "mr", "lib"), full.names = TRUE),
  files = list.files(path = file.path(getwd(), "mr", "palive"), pattern = "\\.R$", full.names = TRUE, recursive = TRUE),
  class = task$main,
  args = args,
  props = task$properties
)
