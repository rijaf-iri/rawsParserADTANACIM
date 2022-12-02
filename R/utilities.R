
format.out.msg <- function(msg, logfile, append = TRUE){
    ret <- c(paste("Time:", Sys.time(), "\n"), msg, "\n",
             "*********************************\n")
    cat(ret, file = logfile, append = append)
}

connect.DBI <- function(con_args, drv){
    args <- c(list(drv = drv), con_args)
    con <- try(do.call(DBI::dbConnect, args), silent = TRUE)
    if(inherits(con, "try-error")) return(NULL)
    con
}

connect.adcon <- function(dirAWS){
    ff <- file.path(dirAWS, "AWS_DATA", "AUTH", "adcon.con")
    adcon <- readRDS(ff)
    conn <- connect.DBI(adcon$connection, RPostgres::Postgres())
    if(is.null(conn)){
        Sys.sleep(3)
        conn <- connect.DBI(adcon$connection, RPostgres::Postgres())
        if(is.null(conn)) return(NULL)
    }

    return(conn)
}

connect.ssh <- function(dirAWS){
    ff <- file.path(dirAWS, "AWS_DATA", "AUTH", "adt.cred")
    ssh <- readRDS(ff)
    session <- try(do.call(ssh::ssh_connect, ssh$cred), silent = TRUE)
    if(inherits(session, "try-error")) return(NULL)

    return(session)
}
