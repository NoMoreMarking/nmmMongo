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
  "taskJudges._perLeft" : 1.0
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

#' Get school name from a dfe
#'
#' @param dfe The dfe for the school
#' @param connStr A connection string.
#' @return A data frame with syllabus ids
#' @examples
#' getSchoolByName(999999999990, 'mongodb://') will find the school with the dfe 999999999990.
#' @export
getSchoolByDfe <- function(dfe,connStr){
  schoolsCollection <- mongolite::mongo(db='nmm-vegas-db',collection="schools",url=connStr)
  qryString <- paste0('{"dfe":',dfe,'}')
  school <- schoolsCollection$find(query = qryString,fields = '{"schoolName" : true}')
  if(nrow(school)>0){
    return(school$schoolName)
  } else {
    cat('no school found for that dfe') 
  }
}

#' Get syllabus ids from a syllabus name
#'
#' @param name The name for the syllabus
#' @param connStr A connection string.
#' @return A data frame with syllabus ids
#' @examples
#' getSyllabusByName('SACS', 'mongodb://') will match all SACS syllabuses.
#' @export
getSyllabusByName <- function(name,connStr){
  syllabusCollection <- mongolite::mongo(db='nmm-vegas-db',collection="syllabus",url=connStr)
  qryString <- paste0('{"name":{"$regex":"',name,'","$options":"i"}}')
  syllabusList <- syllabusCollection$find(query = qryString,fields = '{"name" : true}')
  return(syllabusList)
}

#' Get task ids, names and number of decisions completed for tasks with a given moderation code
#'
#' @param modCode The modCode for the tasks required
#' @param connStr A connection string.
#' @return A data frame with task details.
#' @examples
#' getTasks('7e606c3c-8e33-4c11-801f-6d168d79a869', 'mongodb://') will match all APW 2019 Year 3 tasks.
#' @export
getTasksByModCode <- function(modCode,connStr){
  tasksCollection <- mongolite::mongo(db='nmm-vegas-db',collection="tasks",url=connStr)
  qryString <- paste0('{"modCode":"',modCode,'"}')
  taskList <- tasksCollection$find(query = qryString, fields = '{"pugTemplate" : false, "owners": false, "reliabilities":false, "notes":false}')
  tasks <- jsonlite::flatten(taskList)
  return(tasks)
}

#' Get task ids, names and number of decisions completed
#'
#' @param syllabus The syllabus id for the tasks required
#' @param connStr A connection string.
#' @return A data frame with task details.
#' @examples
#' getTasks('7e606c3c-8e33-4c11-801f-6d168d79a869', 'mongodb://') will match all APW 2019 Year 3 tasks.
#' @export
getTasksBySyllabus <- function(syllabus,connStr){
  tasksCollection <- mongolite::mongo(db='nmm-vegas-db',collection="tasks",url=connStr)
  qryString <- paste0('{"syllabus":"',syllabus,'"}')
  taskList <- tasksCollection$find(query = qryString, fields = '{"pugTemplate" : false, "owners": false, "reliabilities":false}')
  tasks <- jsonlite::flatten(taskList)
  return(tasks)
}

#' Get task ids, names and number of decisions completed
#'
#' @param taskName Tasks will be matched if their name includes the taskName string.
#' @param connStr A connection string.
#' @return A data frame with task details.
#' @examples
#' getTasks('Sharing Standards 2017-2018 Year 5', 'mongodb://') will match all Year 5 tasks.
#' @export
getTasks <- function(taskName,connStr){
  tasksCollection <- mongolite::mongo(db='nmm-vegas-db',collection="tasks",url=connStr)
  qryString <- paste0('{"name":{"$regex":"',taskName,'","$options":"i"}}')
  taskList <- tasksCollection$find(query = qryString)
  tasks <- jsonlite::flatten(taskList)
  if(nrow(tasks)==0){
    cat('No tasks found for', taskName,'.\n')
  }
  return(tasks)
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
    result <- judges$update(query=qry, update='{"$set":{"excludeMod": "true"}}')
  } else {
    result <- judges$update(query=qry, update='{"$set":{"exclude": "true"}}')
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
  result <- judges$update(query=qry, update='{"$set":{"excludeMod": "true"}}')
  return (result)
}

#' Set a moderation code for a task
#'
#' @param task The task id.
#' @param modCode The moderation code.
#' @param connStr A connection string.
#' @return Nothing
#' @examples
#' setModerationCode('FJRRtRsNxm7TG93o9','sZBLjuzPgiJwonDTw', 'mongodb://')
#' @export
setModerationCode <- function(task,modCode,connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  qry <- paste0('{"_id":"',task,'"}')
  updateStr <- paste0('{"$set":{"modCode": "',modCode,'"}}')
  tasks$update(qry, updateStr)
  return(NULL)
}

#' Get candidates from a syllabus
#'
#' @param syllabus The task syllabus
#' @param connStr A connection string.
#' @return A data frame with persons
#' @examples
#' getPersonsFromSyllabus('ee64c76a-5312-4e95-b315-ef5aca44539b', 'mongodb://')
#' @export
#' @import dplyr
getPersonsFromSyllabus <- function(syllabus, connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  pipeline <- paste0('[{"$match" : {"syllabus" : "',syllabus,'"}},{"$lookup" : {"from" : "candidates", "localField" : "_id", "foreignField" : "localTask", "as" : "taskCandidates"}}, { "$unwind" : {"path" : "$taskCandidates"}},{"$project":{"taskCandidates" : 1.0}}, {"$project" : {"taskCandidates.owners" : 0.0,"taskCandidates.opponents":0.0,"taskCandidates.localOpponents":0.0,"taskCandidates.modOpponents":0.0,"taskCandidates.scans":0.0 }},{"$replaceRoot" : {"newRoot" : "$taskCandidates"}}]')
  taskPersons <- tasks$aggregate(pipeline,options = '{"allowDiskUse":true}')
  dfe_str <- "[0-9]{7,12}"
  taskPersons <- taskPersons %>% mutate(
    dfe = str_extract(taskName, dfe_str)  
  )
  return(taskPersons)
}

#' Get candidates from a task or tasks
#'
#' @description Get the candidates from a task. If the task is a moderation task, set mod=TRUE
#' @param task The task id
#' @param mod Is it a moderation task you want?
#' @param connStr A connection string.
#' @return A data frame with persons
#' @examples
#' getPersonsFromTask('ee64c76a-5312-4e95-b315-ef5aca44539b',mod = TRUE, 'mongodb://')
#' @export
#' @import dplyr
getPersonsFromTask <- function(task,mod=FALSE,connStr){
  candidates <- mongolite::mongo('candidates',url=connStr)
  if(!mod) {
    qryString <- paste0('{"localTask":"',task,'"}')
  } else {
    qryString <- paste0('{"modTask":"',task,'"}')
  }
  taskPersons <- candidates$find(qryString, fields='{"owners" : 0.0,"opponents":0.0,"localOpponents":0.0,"modOpponents":0.0,"scans":0.0 }')
  if(nrow(taskPersons)>0)
  {
    dfe_str <- "[0-9]{7,12}"
    taskPersons <- taskPersons %>% mutate(
      dfe = str_extract(taskName, dfe_str)
    )  
  }
  return(taskPersons)
}

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
  return(taskDecisions)
}

#' Set anchor scaling factors for tasks
#'
#' @param task The task id
#' @param uScale Anchor scaling factor
#' @param uiMean Mean of anchor scale
#' @param connStr A connection string.
#' @return Nothing
#' @examples
#' updateScaling('FYB5kMkoh2v5sLsY7',6, 55, 'mongodb://')
#' @export
updateScaling <- function(task,uScale,uiMean,connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  pipeline <- paste0('{"_id":"',task,'"}')
  updateStr <- paste0('{"$set":{"anchorScalingUScale": ',uScale,', "anchorScalingUMean":',uiMean,'}}')
  tasks$update(pipeline, updateStr)
  return (NULL)
}

#' Get scaling factors for tasks
#'
#' @param task The task id
#' @param connStr A connection string.
#' @return data frame with _id, scaling.uScale and scaling.uiMean
#' @examples
#' getScaling('FYB5kMkoh2v5sLsY7','mongodb://')
#' @export
getScaling <- function(task,connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  qryString <- paste0('{"_id":"',task,'"}')
  taskList <- tasks$find(query = qryString,
                         fields = '{"name": true, "scalingUScale" : true,"scalingUMean": true, "anchorScalingUScale" : true, "anchorScalingUMean" : true}')
  tasks <- jsonlite::flatten(taskList)
  return(tasks)
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
