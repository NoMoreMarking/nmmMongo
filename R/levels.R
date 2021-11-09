#' Get levels data
#'
#' @param task The task id
#' @param connStr A connection string.
#' @return data frame with the task levels
#' @examples
#' getLevels('74537cb5-d97e-4d48-b89a-97a81b91cd34','mongodb://')
#' @export
getLevels <- function(task,connStr){
  levels <- mongolite::mongo(db='nmm-vegas-db',collection="levels",url=connStr)
  qryString <- paste0('{"task":"',task,'"}')
  levelsList <- levels$find(query = qryString)
  return(levelsList)
}


#' Set levels data
#'
#' @param task The task id
#' @param boundaries A dataframe with the columns: task, name, value, include, cutOff
#' @param connStr A connection string.
#' @return The levels that were updated
#' @examples
#' setLevels('74537cb5-d97e-4d48-b89a-97a81b91cd34','mongodb://')
#' @export
setLevels <- function(task,boundaries,connStr){
  if(sum(c('task','name','value','include','cutOff') %in% names(boundaries))!=5) return ('Fields must include: task,name,value,include,cutOff')
  # Remove existing levels
  levels <- mongolite::mongo(db='nmm-vegas-db',collection="levels",url=connStr)
  qryStr <- paste0('{"task" : "',task,'"}')
  existingLevels <- getLevels(task, connStr)
  n <- nrow(existingLevels)
  for(i in 1:n){
    levels$remove(qryStr,just_one = TRUE)  
  }
  insertBoundaries <- boundaries %>% mutate(createdAt=lubridate::now()) %>% select(task, name, value, include, cutOff,createdAt)
  out <- levels$insert(insertBoundaries)
  return(out)
}
