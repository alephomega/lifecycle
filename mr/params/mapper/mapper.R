map <- function(k, v, context) {
   M <- matrix(
     data = unlist(strsplit(k, split = "\001")), 
     ncol = 4, 
     byrow = TRUE)
   
   list(M[, 1], paste(M[, 2], M[, 3], M[, 4], v, sep = "\001"))
}
