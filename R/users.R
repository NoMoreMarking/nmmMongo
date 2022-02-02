
#' Get user details from an auth string
#'
#' @param auth The auth for the user
#' @param connStr A connection string.
#' @return A data frame with user details
#' @examples
#' getUser("auth0|607457cfe1af3d0072229628", 'mongodb://') will find the user with the auth 'auth0|607457cfe1af3d0072229628'
#' @export
getUser <- function(auth,connStr){
  userCollection <- mongolite::mongo(db='nmm-vegas-db',collection="user.personals",url=connStr)
  qryString <- paste0('{"user":"',auth,'"}')
  user <- userCollection$find(query = qryString)
  if(nrow(user)>0){
    return(user)
  } else {
    cat('no user found for that auth') 
  }
}