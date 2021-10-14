#' Get judges for a syllabus
#'
#' @param syllabus The syllabus id
#' @param connStr A connection string.
#' @return A data frame with judge details.
#' @examples
#' getJudgesBySyllabus(syllabusid, 'mongodb://')
#' @export
getJudgesBySyllabus <- function(syllabus, connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  pipeline <- paste0('
  [
        { 
  "$match" : {
  "syllabus" : "',syllabus,'"
  }
}, 
  { 
  "$lookup" : {
  "from" : "judges", 
  "localField" : "_id", 
  "foreignField" : "localTask", 
  "as" : "taskJudges"
  }
  }, 
  { 
  "$unwind" : {
  "path" : "$taskJudges"
  }
  }, 
  { 
  "$project" : {
  "name" : 1.0, 
  "taskJudges._id" : 1.0,
  "taskJudges.email" : 1.0, 
  "taskJudges.localComparisons" : 1.0, 
  "taskJudges.modComparisons" : 1.0, 
  "taskJudges.trueScore" : 1.0, 
  "taskJudges._medianTimeTaken" : 1.0, 
  "taskJudges._perLeft" : 1.0,
  "taskJudges.excludeMod" : 1.0,
  "taskJudges.exclude" : 1.0
  }
  }
  ]')
  judgeList <- tasks$aggregate(pipeline,options = '{"allowDiskUse":true}')
  judges <- jsonlite::flatten(judgeList)  
  return(judges)
}

#' Get judge details
#'
#' @param taskId Task Id
#' @param modTask Is the task id a moderation task, TRUE or FALSE
#' @param connStr A connection string.
#' @return A data frame with judge details.
#' @examples
#' getJudges(localTask,modTask=FALSE,connStr)
#' getJudges(modTask,modTask=TRUE,connStr)
#' @export
getJudges <- function(taskId,modTask=FALSE,connStr){
  judges <- mongolite::mongo(db='nmm-vegas-db',collection="judges",url=connStr)
  if(modTask){
    qryString <- paste0('{"modTask":"',taskId,'"}')
  } else {
    qryString <- paste0('{"localTask":"',taskId,'"}')
  }
  judgeList <- judges$find(query = qryString,fields = '{"owners" : false, "timesTaken":false}')
  return(judgeList)
}

#' Get school details from a dfe
#'
#' @param dfe The dfe for the school
#' @param connStr A connection string.
#' @return A data frame with school names and urns
#' @examples
#' getSchoolByName(999999999990, 'mongodb://') will find the school with the dfe 999999999990.
#' @export
getSchoolByDfe <- function(dfe,connStr){
  schoolsCollection <- mongolite::mongo(db='nmm-vegas-db',collection="schools",url=connStr)
  qryString <- paste0('{"dfe":',dfe,'}')
  school <- schoolsCollection$find(query = qryString,fields = '{"schoolName" : true, "urn": true, "assemblySchoolSlug":true}')
  if(nrow(school)>0){
    return(school)
  } else {
    cat('no school found for that dfe') 
  }
}

#' Get product from a product id
#'
#' @param id The id of the product
#' @param connStr A connection string
#' @export
getProduct <- function(id, connStr){
  productCollection <- mongolite::mongo(db='nmm-vegas-db',collection="products",url=connStr)
  qryString <- paste0('{"_id":"',id,'"}')
  productList <- productCollection$find(query=qryString, fields = '{"productName":true}',limit = 1)
  if('productName' %in% names(productList)) return (productList$productName)
  return(paste0('no product with id: ',id))
}

#' Exclude a judge from a task
#'
#' @param judge_id The judge id
#' @param modTask Is this a mod task, TRUE or FALSE?
#' @param connStr A connection string.
#' @return Nothing
#' @examples
#' excludeJudge('o544AXuwEBADM9Lrk', FALSE,'mongodb://')
#' @export
excludeJudge <- function(judge_id,modTask=FALSE,connStr){
  judges <- mongolite::mongo('judges',url=connStr)
  qry <- paste0('{"_id":"',judge_id,'"}')
  if(modTask){
    result <- judges$update(query=qry, update='{"$set":{"excludeMod": true}}')
  } else {
    result <- judges$update(query=qry, update='{"$set":{"exclude": true}}')
  }
  return (result)
}

#' Exclude a judge from a moderation task
#'
#' @param judge_id The judge id
#' @param connStr A connection string.
#' @return Nothing
#' @examples
#' excludeJudge('o544AXuwEBADM9Lrk', 'mongodb://')
#' @export
excludeJudgeMod <- function(judge_id,connStr){
  judges <- mongolite::mongo('judges',url=connStr)
  qry <- paste0('{"_id":"',judge_id,'"}')
  result <- judges$update(query=qry, update='{"$set":{"excludeMod": true}}')
  return (result)
}

#' Re-include judges in a moderation task
#'
#' @param task The task id
#' @param connStr A connection string.
#' @return Records affected
#' @examples
#' resetModJudges('o544AXuwEBADM9Lrk', 'mongodb://')
#' @export
resetModJudges <- function(task,connStr){
  judges <- mongolite::mongo('judges',url=connStr)
  qry <- paste0('{"excludeMod" : true,"modTask":"',task,'"}')
  result <- judges$update(query=qry, update='{"$set":{"excludeMod": false}}', multiple = TRUE)
  return (result)
}

#' Get sales data
#'
#' @param project The project id
#' @param connStr A connection string.
#' @return data frame product data
#' @examples
#' getSales('vqPwL2wWX8qc26nWs','mongodb://')
#' @export
getSales <- function(product,connStr){
  basket <- mongolite::mongo(db='nmm-vegas-db',collection="purchases",url=connStr)
  qryString <- paste0('{"product":"',product,'"}')
  salesList <- basket$find(query = qryString)
  sales <- jsonlite::flatten(salesList)
  return(sales)
}


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
