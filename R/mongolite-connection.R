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
  taskList <- tasksCollection$find(query = qryString, fields = '{"pugTemplate" : false, "owners": false, "reliabilities":false}')
  if(nrow(taskList)==0){
    cat('No tasks found for', taskName,'.\n')
  }
  return(taskList)
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
  taskDecisions <- jsonlite::flatten(taskDecisions)
  taskDecisions <- taskDecisions %>% select(
    judgeId = decisionJudge._id,
    judge = decisionJudge.email,
    chosen = chosenCandidate.qrcode,
    notChosen = notChosenCandidate.qrcode,
    timeTaken
  )
  return(taskDecisions)
}

#' Set syllabus for a task
#'
#' @param task The task id
#' @param syllabus The syllabus id
#' @param connStr A connection string.
#' @return Nothing
#' @examples
#' updateSyllabus('FYB5kMkoh2v5sLsY7','FYB5kMkoh2v5sLsY8', 'mongodb://')
#' @export
updateSyllabus <- function(task,syllabus,connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  pipeline <- paste0('{"_id":"',task,'"}')
  updateStr <- paste0('{"$set":{"syllabus": "',syllabus,'"}}')
  out <- tasks$update(pipeline, updateStr)
  taskReturn <- tasks$find(query = pipeline, fields = '{"name" : true, "syllabus": true}')
  syllabusCollection <- mongolite::mongo('syllabus', url=connStr)
  syllabusReturn <- syllabusCollection$find(query=paste0('{"_id":"',syllabus,'"}'), fields = '{"name" : true}')
  cat('Syllabus for ', taskReturn$name, ' set to ', syllabusReturn$name, '\n')
  return (out)
}

#' Set anchor scaling factors for tasks
#'
#' @param task The task id
#' @param uScale Anchor scaling factor
#' @param uiMean Mean of anchor scale
#' @param anchorScale Anchor scaling or normal scaling?
#' @param connStr A connection string.
#' @return Nothing
#' @examples
#' updateScaling('FYB5kMkoh2v5sLsY7',6, 55, TRUE, 'mongodb://')
#' @export
updateScaling <- function(task,uScale,uiMean,anchorScale=TRUE,connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  pipeline <- paste0('{"_id":"',task,'"}')
  if(anchorScale){
    updateStr <- paste0('{"$set":{"anchorScalingUScale": ',uScale,', "anchorScalingUMean":',uiMean,'}}')
  } else {
    updateStr <- paste0('{"$set":{"scalingUScale": ',uScale,', "scalingUMean":',uiMean,'}}')
  }
  out <- tasks$update(pipeline, updateStr)
  return (out)
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
