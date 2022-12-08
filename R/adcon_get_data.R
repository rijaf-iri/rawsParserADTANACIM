
get.adcon.data <- function(dirAWS, dirUP){
    on.exit({
        on.exit(DBI::dbDisconnect(conn))
        if(upload) ssh::ssh_disconnect(session)
    })

    tz <- "Africa/Dakar"
    Sys.setenv(TZ = tz)
    origin <- "1970-01-01"
    awsNET <- 2

    dirOUT <- file.path(dirAWS, "AWS_DATA", "DATA", "ADCON")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "ADCON")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)

    mon <- format(Sys.time(), '%Y%m')
    logPROC <- file.path(dirLOG, paste0("processing_adcon_", mon, ".txt"))
    awsLOG <- file.path(dirLOG, paste0("AWS_LOG_", mon, ".txt"))
    uploadFailed <- file.path(dirLOG, 'upload_failed.txt')

    conn <- connect.adcon(dirAWS)
    if(is.null(conn)){
        msg <- "An error occurred when connecting to ADCON database"
        format.out.msg(msg, logPROC)
        return(1)
    }

    session <- connect.ssh(dirAWS)
    if(is.null(session)){
        msg <- paste(session, "Unable to connect to ADT server\n")
        format.out.msg(msg, logPROC)
        upload <- FALSE
    }else{
        dirUPData <- file.path(dirUP, "AWS_DATA", "DATA", "minutes", "ADCON")
        dirUPLog <- file.path(dirUP, "AWS_DATA", "LOG", "ADCON")
        ssh::ssh_exec_wait(session, command = c(
            paste0('if [ ! -d ', dirUPData, ' ] ; then mkdir -p ', dirUPData, ' ; fi'),
            paste0('if [ ! -d ', dirUPLog, ' ] ; then mkdir -p ', dirUPLog, ' ; fi')
        ))
        upload <- TRUE
    }

    varFile <- file.path(dirAWS, "AWS_DATA", "CSV", "adcon_pars.csv")
    varTable <- utils::read.table(varFile, sep = ',', header = TRUE,
                                  stringsAsFactors = FALSE, quote = "\"",
                                  fileEncoding = "latin1")
    awsFile <- file.path(dirAWS, "AWS_DATA", "CSV", "adcon_lastDates.csv")
    awsInfo <- utils::read.table(awsFile, sep = ',', header = TRUE,
                                 stringsAsFactors = FALSE, quote = "\"",
                                 fileEncoding = 'latin1')

    for(j in seq_along(awsInfo$id)){
        awsID <- awsInfo$id[j]
        awsVAR <- varTable[varTable$id == awsID, , drop = FALSE]

        qres <- lapply(seq_along(awsVAR$node_var), function(i){
            if(is.na(awsInfo$last[j])){
                query <- paste0("SELECT * FROM historiandata WHERE tag_id=", awsVAR$node_var[i])
            }else{
                query <- paste0("SELECT * FROM historiandata WHERE tag_id=", awsVAR$node_var[i],
                                " AND enddate > ", awsInfo$last[j])
            }

            qres <- try(DBI::dbGetQuery(conn, query), silent = TRUE)
            if(inherits(qres, "try-error")){
                msg <- paste("Unable to get data for", awsID, "parameter", awsVAR$var_name[i])
                format.out.msg(msg, awsLOG)
                return(NULL)
            }

            if(nrow(qres) == 0) return(NULL)

            valid <- qres$tag_id == awsVAR$node_var[i] & qres$status == 0
            qres <- qres[valid, , drop = FALSE]

            if(nrow(qres) == 0) return(NULL)

            end <- as.POSIXct(qres$enddate, origin = origin, tz = tz)
            start <- as.POSIXct(qres$startdate, origin = origin, tz = tz)
            dft <- difftime(end, start, units = "mins")
            ## 10 and 15 mins interval, take obs with greater than 5 mins sampling and less than 20
            qres <- qres[dft >= 5 & dft < 20, , drop = FALSE]

            if(nrow(qres) == 0) return(NULL)

            qres <- qres[, c('enddate', 'measuringvalue'), drop = FALSE]

            return(qres)
        })

        inull <- sapply(qres, is.null)
        if(all(inull)) next

        qres <- qres[!inull]
        awsVAR <- awsVAR[!inull, , drop = FALSE]

        nvar <- c('id', "var_height", "var_code", "stat_code")
        qres <- lapply(seq_along(awsVAR$node_var), function(i){
            x <- qres[[i]]
            v <- awsVAR[i, nvar, drop = FALSE]
            v <- v[rep(1, nrow(x)), , drop = FALSE]
            cbind(v, x)
        })

        qres <- do.call(rbind, qres)
        qres$network <- awsNET

        out <- try(parse.adcon_db.data(qres), silent = TRUE)
        if(inherits(out, "try-error")){
            mserr <- gsub('[\r\n]', '', out[1])
            msg <- paste("Unable to parse data for", awsID)
            format.out.msg(paste(mserr, '\n', msg), awsLOG)
            next
        }

        temps <- as.POSIXct(out$obs_time, origin = origin, tz = tz)
        split_day <- format(temps, '%Y%m%d')
        index_day <- split(seq(nrow(out)), split_day)

        for(s in seq_along(index_day)){
            don <- out[index_day[[s]], , drop = FALSE]
            awsInfo$last[j] <- max(don$obs_time)

            locFile <- paste(range(don$obs_time), collapse = "_")
            locFile <- paste0(awsID, "_", locFile, '.rds')
            locFile <- file.path(dirOUT, locFile)
            saveRDS(don, locFile)

            if(upload){
                upFile <- file.path(dirUPData, basename(locFile))
                ret <- try(ssh::scp_upload(session, locFile, to = upFile, verbose = FALSE), silent = TRUE)

                if(inherits(ret, "try-error")){
                    if(grepl('disconnected', ret[1])){
                        Sys.sleep(5)
                        session <- connect.ssh(dirAWS)
                        upload <- if(is.null(session)) FALSE else TRUE
                    }
                    cat(basename(locFile), file = uploadFailed, append = TRUE, sep = '\n')
                }
            }else{
                cat(basename(locFile), file = uploadFailed, append = TRUE, sep = '\n')
            }

            utils::write.table(awsInfo, awsFile, sep = ",", na = "", col.names = TRUE,
                               row.names = FALSE, quote = FALSE)
        }
    }

    if(upload){
        if(file.exists(logPROC)){
            logPROC1 <- file.path(dirUPLog, basename(logPROC))
            ssh::scp_upload(session, logPROC, to = logPROC1, verbose = FALSE)
        }

        if(file.exists(awsLOG)){
            awsLOG1 <- file.path(dirUPLog, basename(awsLOG))
            ssh::scp_upload(session, awsLOG, to = awsLOG1, verbose = FALSE)
        }
    }

    return(0)
}

parse.adcon_db.data <- function(tmp){
    tz <- "Africa/Dakar"
    origin <- "1970-01-01"
    Sys.setenv(TZ = tz)

    tmp <- tmp[, c("network", "id", "var_height", "var_code",
                   "stat_code", "enddate", "measuringvalue")]
    ## time zone conversion
    # temps <- as.POSIXct(tmp$enddate, origin = origin, tz = "UTC")
    # temps <- time_utc2time_local(temps, tz)
    temps <- as.POSIXct(tmp$enddate, origin = origin, tz = tz)

    tmp$enddate <- as.numeric(temps)
    tmp$limit_check <- NA

    ## units conversion here

    fun_format <- list(as.integer, as.character, as.numeric, as.integer,
                       as.integer, as.integer, as.numeric, as.integer)

    tmp <- lapply(seq_along(fun_format), function(j) fun_format[[j]](tmp[[j]]))
    tmp <- as.data.frame(tmp)
    names(tmp) <- c("network", "id", "height", "var_code",
                    "stat_code", "obs_time", "value", "limit_check")

    return(tmp)
}
