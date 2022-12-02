
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

#' Process ADCON data.
#'
#' Get the data from ADCON database, parse and convert into ADT format.
#' 
#' @param dirAWS full path to the directory containing the AWS_DATA folder.\cr
#' @param dirPLUSO full path to the directory PULSONIC folder on ADT server.
#' @param initData logical, if \code{TRUE} the initialization data will be processed.
#' 
#' @export

process.pulsonic <- function(dirAWS, dirPLUSO, initData = FALSE){
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "PULSONIC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_pulsonic.txt")

    ret <- try(get.pulsonic.data(dirAWS, dirPLUSO, initData), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        mserr <- gsub('[\r\n]', '', ret[1])
        msg <- "Getting PULSONIC data failed"
        format.out.msg(paste(mserr, '\n', msg), logPROC)
        return(2)
    }

    return(0)
}

# dirAWS <- "/home/adt/ADT"
# dirPLUSO <- "/home/adt/PULSONIC"

