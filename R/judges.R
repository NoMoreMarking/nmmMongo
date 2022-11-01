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
  "taskJudges.exclude" : 1.0,
  "taskJudges.quota" : 1.0
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

#' Update an individual judge quota
#'
#' @param judge The judge id
#' @param quota The new quota
#' @param connStr A connection string.
#' @return List with modifiedCount, matchedCount, upsertedCount
#' @examples
#' updateJudgeQuota('ad888bb7-e47a-4e6b-b827-1498c752a389',55, 'mongodb://')
#' @export
updateJudgeQuota <- function(judge,quota,connStr){
  judges <- mongolite::mongo('judges',url=connStr)
  pipeline <- paste0('{"_id":"',judge,'"}')
  updateStr <- paste0('{"$set":{"quota": ',quota,'}}')
  out <- tasks$update(pipeline, updateStr)
  return (out)
}