#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")


drop <- function(table, partition_specs, host, port, db = 'default', user = '', password = '') {
  tool <- J("com/valuepotion/analytics/utils/DropPartitions")
  tool$run(table, .jarray(partition_specs), host, as.integer(port), db, user, password)
}


conf <- config()

cat("args:\n")
print(args)

basedate <- args.basedate()
offset <- args.offset()
tz.basedir <- job.tz.basedir(conf, basedate, offset) 
clients <- unlist(strsplit(args.clients(), split = ','))

cat(print.timestamp(), "* Running lifecycle.drop-partitions\n")
.jaddClassPath(file.path(getwd(), "lib", conf$jar$mr))

task <- conf$job$tasks$drop

specs <- sprintf("basedate='%s',clientid='%s'", basedate, clients)

cat(sprintf("table: %s\n", task$properties$hive.table)) 
cat(sprintf("partitions:\n"))
cat(sprintf("%s\n", paste(specs, collapse = "\n")))

drop(task$properties$hive.table, specs, task$properties$hive.host, task$properties$hive.port, task$properties$hive.db)
