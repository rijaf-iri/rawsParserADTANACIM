
process.pulsonic <- function(dirAWS, dirPLUSO, initData){
    options(scipen = 999)
    tz <- "Africa/Dakar"
    Sys.setenv(TZ = tz)
    origin <- "1970-01-01"
    awsNET <- 1

    dirOUT <- file.path(dirAWS, "AWS_DATA", "DATA", "minutes", "PULSONIC")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "PULSONIC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)

    logPROC <- file.path(dirLOG, "processing_pulsonic.txt")
    awsLOG <- file.path(dirLOG, "AWS_LOG.txt")

    varFile <- file.path(dirAWS, "AWS_DATA", "CSV", "pulsonic_pars.csv")
    varTable <- utils::read.table(varFile, sep = ',', header = TRUE,
                                  stringsAsFactors = FALSE, quote = "\"",
                                  fileEncoding = "latin1")
    awsFile <- file.path(dirAWS, "AWS_DATA", "CSV", "pulsonic_lastDates.csv")
    awsInfo <- utils::read.table(awsFile, sep = ',', header = TRUE,
                                 stringsAsFactors = FALSE, quote = "\"",
                                 fileEncoding = 'latin1')

    srcDATA <- if(initData) "DATA0" else "DATA"
    dirDATA <- file.path(dirPLUSO, srcDATA)

    for(j in seq_along(awsInfo$id)){
        awsID <- awsInfo$id[j]
        awsVAR <- varTable[varTable$id == awsID, , drop = FALSE]

        pattern <- paste0('^', awsID, '_', '.+\\.csv$')
        aws_list <- list.files(dirDATA, pattern)

        if(length(aws_list) == 0) next

        awsL <- strsplit(aws_list, "_")
        len <- sapply(awsL, 'length')
        ilen <- len == 3
        if(!any(ilen)) next

        aws_list <- aws_list[ilen]
        awsL <- awsL[ilen]
        awsL <- do.call(rbind, awsL)
        awsL[, 3] <- gsub('\\.csv', '', awsL[, 3])

        start <- strptime(awsL[, 2], '%Y%m%d%H%M', tz)
        end <- strptime(awsL[, 3], '%Y%m%d%H%M', tz)

        time0 <- as.POSIXct(awsInfo$last[j], origin = origin, tz = tz)
        if(is.na(time0)) time0 <- start[order(start)][1]
        time1 <- Sys.time()

        it <- (end > time0) & (start <= time1)
        if(!any(it)) next
        aws_list <- aws_list[it]

        for(i in seq_along(aws_list)){
            aws_file <- file.path(dirDATA, aws_list[i])
            header <- utils::read.table(text = readLines(aws_file, n = 1, encoding = 'UTF-8'), sep = ',')
            header <- as.character(header[1, ])
            awsDAT <- utils::read.table(aws_file, sep = ',', header = TRUE,
                                          stringsAsFactors = FALSE, quote = "\"",
                                          fileEncoding = 'latin1')

            out <- try(parse.pulsonic.data(awsDAT, header, awsVAR, awsID, awsNET), silent = TRUE)
            if(inherits(out, "try-error")){
                mserr <- gsub('[\r\n]', '', out[1])
                msg <- paste("Unable to parse data for", awsID, "File", aws_list[i])
                format.out.msg(paste(mserr, '\n', msg), awsLOG)
                next
            }

            awsInfo$last[j] <- max(out$obs_time)

            locFile <- paste(range(out$obs_time), collapse = "_")
            locFile <- paste0(awsID, "_", locFile, '.rds')
            locFile <- file.path(dirOUT, locFile)
            saveRDS(out, locFile)

            utils::write.table(awsInfo, awsFile, sep = ",", na = "", col.names = TRUE,
                               row.names = FALSE, quote = FALSE)
        }
    }

    return(0)
}

parse.pulsonic.data <- function(awsDAT, header, awsVAR, awsID, awsNET){
    tz <- "Africa/Dakar"
    Sys.setenv(TZ = tz)

    ## time zone conversion
    # temps <- strptime(awsDAT$Date, "%Y%m%d%H%M", tz = "UTC")
    # temps <- time_utc2time_local(temps, tz)
    temps <- strptime(awsDAT$Date, "%Y%m%d%H%M", tz = tz)
    temps <- as.numeric(temps)

    vcol <- match(awsVAR$pulso_lab, header)
    tmp <- awsDAT[, vcol, drop = FALSE]

    tmp <- lapply(seq(length(vcol)), function(i){
        data.frame(network = awsNET,
                   id = awsID,
                   height = awsVAR$var_height[i],
                   var_code = awsVAR$var_code[i],
                   stat_code = awsVAR$stat_code[i],
                   obs_time = temps,
                   value = tmp[, i])
    })
    tmp <- do.call(rbind, tmp)
    tmp$limit_check <- NA

    tmp$value <- as.numeric(tmp$value)
    tmp <- tmp[!is.na(tmp$value), , drop = FALSE]

    if(nrow(tmp) == 0) return(NULL)

    ## convert radiation from J/cm2 to W/m2
    fac <- switch(awsVAR$data_type[awsVAR$var_code == 8][1],
                  'U' = 1000/6, 
                  'H' = 100/36
                  )
    rad <- tmp$var_code == 8
    tmp$value[rad] <- round(tmp$value[rad] * fac, 3)

    name_tmp <- names(tmp)
    fun_format <- list(as.integer, as.character, as.numeric, as.integer,
                       as.integer, as.integer, as.numeric, as.integer)
    tmp <- lapply(seq_along(fun_format), function(j) fun_format[[j]](tmp[[j]]))
    tmp <- as.data.frame(tmp)
    names(tmp) <- name_tmp

    return(tmp)
}
