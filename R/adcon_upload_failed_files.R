#' Re-upload files failed to upload.
#'
#' Re-upload all files failed to upload from the previous task to ADT server.
#' 
#' @param dirAWS full path to the directory containing the AWS_DATA folder.\cr
#' @param dirUP full path to the directory containing the AWS_DATA folder on ADT server.
#' 
#' @export

upload.adcon.failed.files <- function(dirAWS, dirUP){
    on.exit({
        if(upload) ssh::ssh_disconnect(session)
    })
    Sys.setenv(TZ = "Africa/Dakar")
    mon <- format(Sys.time(), '%Y%m')

    dirLOCData <- file.path(dirAWS, "AWS_DATA", "DATA", "ADCON")
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "ADCON")
    logPROC <- file.path(dirLOG, paste0("processing_adcon_", mon, ".txt"))
    uploadFailed <- file.path(dirLOG, 'upload_failed.txt')

    session <- connect.ssh(dirAWS)
    if(is.null(session)){
        msg <- "Unable to connect to ADT server\n"
        format.out.msg(msg, logPROC)
        upload <- FALSE
    }else{
        dirUPData <- file.path(dirUP, "AWS_DATA", "DATA", "minutes", "ADCON")
        delayedFiles <- file.path(dirUP, "AWS_DATA", "LOG", "ADCON", "delayed_data.txt")
        upload <- TRUE
    }

    if(!file.exists(uploadFailed)) return(0)

    failedFILES <- readLines(uploadFailed, warn = FALSE)
    failedFILES1 <- failedFILES

    for(uFile in failedFILES){
        locFile <- file.path(dirLOCData, uFile)
        upFile <- file.path(dirUPData, uFile)
        if(upload){
            ret <- try(ssh::scp_upload(session, locFile, to = upFile, verbose = FALSE), silent = TRUE)
            if(inherits(ret, "try-error")){
                if(grepl('disconnected', ret[1])){
                    Sys.sleep(5)
                    session <- connect.ssh(dirAWS)
                    upload <- if(is.null(session)) FALSE else TRUE
                }
            }else{
                ## send uploaded file to ADT to update aws data table on ADT sever, 
                ## scripts to update aws_minutes, qc limit check, 
                ## qc spatial check, update aws_hourly and aws_daily
                ssh::ssh_exec_wait(session, command = c(
                    paste0("echo '", uFile, "' >> ", delayedFiles)
                ))

                failedFILES1 <- failedFILES1[failedFILES1 != uFile]
                if(length(failedFILES1) > 0){
                    cat(failedFILES1, file = uploadFailed, sep = '\n', append = FALSE)
                }
            }
        }
    }

    conLogFile <- file.path(dirAWS, "AWS_DATA", "LOG", "log_error_connection.txt")
    if(file.exists(conLogFile)){
        if(upload){
            upLogDir <- file.path(dirUP, "AWS_DATA", "LOG", "CONNECTION")
            upFile <- paste0('log_error_connection_', format(Sys.time(), '%Y%m%d%H%M%S'), '.txt')
            upFile <- file.path(upLogDir, upFile)
            ssh::scp_upload(session, conLogFile, to = upFile, verbose = FALSE)
            unlink(conLogFile)
        }
    }

    return(0)
}
