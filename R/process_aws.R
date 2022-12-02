
#' Process ADCON data.
#'
#' Get the data from ADCON database, parse and convert into ADT format.
#' 
#' @param dirAWS full path to the directory containing the AWS_DATA folder.\cr
#' @param dirUP full path to the directory containing the AWS_DATA folder on ADT server.
#' 
#' @export

process.adcon <- function(dirAWS, dirUP){
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "ADCON")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_adcon.txt")

    ret <- try(get.adcon.data(dirAWS, dirUP), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        mserr <- gsub('[\r\n]', '', ret[1])
        msg <- "Getting ADCON data failed"
        format.out.msg(paste(mserr, '\n', msg), logPROC)
        return(2)
    }

    return(0)
}


# dirAWS <- "E:/ADT"
# dirUP <- "/home/adt/ADT"
