#' Get decisions from a task or tasks
#'
#' @param taskId The task ID.
#' @param connStr A connection string.
#' @return A data frame with decisions.
#' @examples
#' getDecisions('67059790-569b-440f-a858-bddabf313e07', 'mongodb://')
#' @export
#' @import dplyr
getDecisions <- function(taskId, connStr) {
  decisions <- mongolite::mongo('decisions', url = connStr)
  pipeline <- paste0(
    '    [
    {
    "$match" : {
    "task" : "',
    taskId,
    '"
    }
    },
    {
    "$lookup" : {
    "from" : "judges",
    "localField" : "judge",
    "foreignField" : "_id",
    "as" : "decisionJudge"
    }
    },
    {
    "$unwind" : {
    "path" : "$decisionJudge"
    }
    },
    {
    "$project" : {
    "decisionJudge._id":  1.0,
    "decisionJudge.email" : 1.0,
    "chosen" : 1.0,
    "notChosen" : 1.0,
    "timeTaken" : 1.0,
    "isLeft" : 1.0,
    "isExternal" : 1.0,
    "createdAt" : 1.0,
    "exclude" : 1.0,
    "autoAdded" : 1.0
    }
    },
    {
    "$lookup" : {
    "from" : "candidates",
    "localField" : "chosen",
    "foreignField" : "_id",
    "as" : "chosenCandidate"
    }
    },
    {
    "$unwind" : {
    "path" : "$chosenCandidate"
    }
    },
    {
    "$project" : {
    "decisionJudge._id":  1.0,
    "decisionJudge.email" : 1.0,
    "chosenCandidate.qrcode" : 1.0,
    "notChosen" : 1.0,
    "timeTaken" : 1.0,
    "isLeft" : 1.0,
    "isExternal" : 1.0,
    "createdAt" : 1.0,
    "exclude" : 1.0,
    "autoAdded" : 1.0
    }
    },
    {
    "$lookup" : {
    "from" : "candidates",
    "localField" : "notChosen",
    "foreignField" : "_id",
    "as" : "notChosenCandidate"
    }
    },
    {
    "$unwind" : {
    "path" : "$notChosenCandidate"
    }
    },
    {
    "$project" : {
    "decisionJudge._id":  1.0,
    "decisionJudge.email" : 1.0,
    "chosenCandidate.qrcode" : 1.0,
    "notChosenCandidate.qrcode" : 1.0,
    "timeTaken" : 1.0,
    "isLeft" : 1.0,
    "isExternal" : 1.0,
    "createdAt" : 1.0,
    "exclude" : 1.0,
    "autoAdded" : 1.0
    }
    }
    ]
    '
  )
  
  taskDecisions <- decisions$aggregate(pipeline,options = '{"allowDiskUse":true}')
  if(nrow(taskDecisions)>0){
    taskDecisions <- jsonlite::flatten(taskDecisions)
    taskDecisions <- taskDecisions %>% rename(
      "id"= "_id",
      "judgeId"="decisionJudge._id",
      "judge"="decisionJudge.email",
      "chosen"="chosenCandidate.qrcode",
      "notChosen"="notChosenCandidate.qrcode"
    )
  }
  return(taskDecisions)
}

#' Exclude a decision from a task
#' @param decision A decision id.
#' @param connStr A connection string.
#' @return updated count
#' @export
#' @import mongolite
#' @import tidyr
#' 
#' 
excludeDecision <- function(decision, connStr){
  decisions <- mongolite::mongo('decisions',url=connStr)
  qry <- paste0('{"_id":"',decision,'"}')
  result <- decisions$update(query=qry, update='{"$set":{"exclude": true}}')
  return (result)
}

#' Re-include all decisions from a task
#' @param task The task id
#' @param connStr A connection string.
#' @return updated count
#' @export
#' @import mongolite
#' @import tidyr
#' 
#' 
resetDecisions <- function(task, connStr){
  decisions <- mongolite::mongo('decisions',url=connStr)
  qry <- paste0('{"task":"',task,'"}')
  result <- decisions$update(query=qry, update='{"$unset":{"exclude": ""}}', multiple=TRUE)
  return (result)
}


#' Exclude decisions from a task
#' @param decisions A vector of decision ids.
#' @param connStr A connection string.
#' @return updated count
#' @export
#' @import mongolite
#' @import tidyr
#' 
#' 
excludeDecisions <- function(decisions, connStr){
  decisionStr <- paste(decisions,collapse='","')
  decisions <- mongolite::mongo('decisions',url=connStr)
  qry <- paste0('{"_id" : {"$in" : ["',decisionStr,'"]}}')
  result <- decisions$update(query=qry, update='{"$set":{"exclude": true}}', multiple=TRUE)
  return (result)
}

#' Recode AI decisions as human decisions
#' @param task The task id
#' @param connStr A connection string.
#' @return updated count
#' @export
#' @import mongolite
#' @import tidyr
#' 
#' 
humaniseDecisions <- function(task, connStr){
  decisions <- mongolite::mongo('decisions',url=connStr)
  qry <- paste0('{"task":"',task,'"}')
  result <- decisions$update(query=qry, update='{"$unset":{"autoAdded": ""}}', multiple=TRUE)
  return (result)
}

#' Recode AI decisions as robot decisions
#' @param task The task id
#' @param connStr A connection string.
#' @return updated count
#' @export
#' @import mongolite
#' @import tidyr
#' 
#' 
robotiseDecisions <- function(task, connStr){
  decisions <- mongolite::mongo('decisions',url=connStr)
  qry <- paste0('{"task":"',task,'"}')
  result <- decisions$update(query=qry, update='{"$set":{"autoAdded": true}}', multiple=TRUE)
  return (result)
}




