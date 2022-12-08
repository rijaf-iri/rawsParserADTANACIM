
format.out.msg <- function(msg, logfile, append = TRUE){
    ret <- c(paste("Time:", Sys.time(), "\n"), msg, "\n",
             "*********************************\n")
    cat(ret, file = logfile, append = append)
}

connect.DBI <- function(con_args, drv, dirAWS){
    args <- c(list(drv = drv), con_args)
    con <- try(do.call(DBI::dbConnect, args), silent = TRUE)
    if(inherits(con, "try-error")){
        conLogFile <- file.path(dirAWS, "AWS_DATA", "LOG", "log_error_connection.txt")
        err <- gsub('[\r\n]', '', con)
        drv_info <- paste0('Driver: ', class(drv)[1],
                           '; Package: ', attributes(class(drv))$package, '\n')
        msg <- paste("Unable to connect to the database\n", drv_info, err)
        format.out.msg(msg, conLogFile)
        return(NULL)
    }

    return(con)
}

connect.adcon <- function(dirAWS){
    ff <- file.path(dirAWS, "AWS_DATA", "AUTH", "adcon.con")
    adcon <- readRDS(ff)
    conn <- connect.DBI(adcon$connection, RPostgres::Postgres(), dirAWS)
    if(is.null(conn)){
        Sys.sleep(3)
        conn <- connect.DBI(adcon$connection, RPostgres::Postgres(), dirAWS)
        if(is.null(conn)) return(NULL)
    }

    return(conn)
}

connect.ssh <- function(dirAWS){
    ff <- file.path(dirAWS, "AWS_DATA", "AUTH", "adt.cred")
    ssh <- readRDS(ff)
    session <- try(do.call(ssh::ssh_connect, ssh$cred), silent = TRUE)
    if(inherits(session, "try-error")){
        conLogFile <- file.path(dirAWS, "AWS_DATA", "LOG", "log_error_connection.txt")
        err <- gsub('[\r\n]', '', session)
        msg <- paste("SSH: unable to connect to ADT server\n", err)
        format.out.msg(msg, conLogFile)
        return(NULL)
    }

    return(session)
}
