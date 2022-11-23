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
    "createdAt" : 1.0
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
    "createdAt" : 1.0
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
    "createdAt" : 1.0
    }
    }
    ]
    '
  )
  
  taskDecisions <- decisions$aggregate(pipeline,options = '{"allowDiskUse":true}')
  if(nrow(taskDecisions)>0){
    taskDecisions <- jsonlite::flatten(taskDecisions)
    taskDecisions <- taskDecisions %>% select(
      judgeId = decisionJudge._id,
      judge = decisionJudge.email,
      chosen = chosenCandidate.qrcode,
      notChosen = notChosenCandidate.qrcode,
      timeTaken,
      isLeft,
      createdAt
    )  
  }
  return(taskDecisions)
}
