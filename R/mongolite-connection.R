#' Get user details
#'
#' @param connStr A connection string.
#' @return A data frame with user details.
#' @examples
#' getUsers('mongodb://')
#' @export
getUsers <- function(connStr){
  users <- mongolite::mongo(db='nmm-v2',collection="users",url=connStr)
  qryString <- paste0('{}')
  userList <- users$find(query = qryString,
                         fields = '{"_id" : true,"createdAt": true}')
  names(userList) <- c('id','createdAt')
  return(userList)
}

#' Get judge details
#'
#' @param taskId Task Id
#' @param connStr A connection string.
#' @param infit Only judges equal to or above this infit
#' @param comparisons Only judges with equal of fewer than these comparisons
#' @return A data frame with user details.
#' @examples
#' getJudges('ocjZYZAYLuZJyAv4h', 'mongodb://')
#' @export
getJudges <- function(taskId,connStr,infit=0,comparisons=10000){
  judges <- mongolite::mongo(db='nmm-v2',collection="judges",url=connStr)
  #qryString <- paste0('{"task":"',taskId,'","trueScore":"$gt"}')
  
  qryString <- paste0('{
      "$and": [
      {"task":"',taskId,'"},
      {"$or": [
        {"trueScore":{
          "$gt":',infit,'
        }}
        ,
        {"localComparisons":{
          "$lt":',comparisons,'
        }}
        ]}]
    }'
  )
  
  judgeList <- judges$find(query = qryString,
                           fields = '{"_id" : true,"emailLower":true,"localComparisons":true,"trueScore":true,"medianTimeTaken":true,"createdAt": true, "excludeAnalysis": true}')
  return(judgeList)
}

#' Get task ids, names and number of decisions completed. Updated for nmm-vegas-db.
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
  if(nrow(tasks)>0){
    dfe_str <- "[0-9]{7}"
    tasks <- tasks %>% mutate(
      dfe = str_extract(name, dfe_str)  
    )
  } else {
    cat('No tasks found for', taskName,'.\n')
  }
  return(tasks)
}

#' Get task id, name and number of decisions completed.
#'
#' @param taskName Exact task name.
#' @param connStr A connection string.
#' @return A data frame with task ids, task names and numbers of judgements.
#' @examples
#' getTasks('Sharing Standards 2017-2018 Year 5', 'mongodb://') will match all Year 5 tasks.
#' @export
getTask <- function(taskName,connStr){
  tasks <- mongolite::mongo(db='nmm-v2',collection="tasks",url=connStr)
  qryString <- paste0('{"name":"',taskName,'"}')
  taskList <- tasks$find(query = qryString,
                         fields = '{"name" : true,"dash.judgements": true}')
  tasks <- jsonlite::flatten(taskList)
  if(nrow(tasks)>0){
    names(tasks) <- c('id','name','judgements')
    return(tasks)
  } else {
    cat('No task returned\n')
    return(NULL)
  }
  
}

#' Get level names and cut off values for a task
#'
#' @param taskName The task name. Doesn't support regular expressions.
#' @param connStr A connection string.
#' @return A data frame with level ids, names and cutoffs.
#' @examples
#' getLevels('Sharing Standards 2017-2018 Year 5 Moderation Task A', 'mongodb://')
#' @export
getLevels <- function(taskName, connStr){
  tsk <- nmmMongo::getTask(taskName, connStr)
  if(!is.null(tsk)){
    taskId <- tsk$id
    levels <- mongolite::mongo('levels',url=connStr)
    qryString <- paste0('{"task" : "',taskId,'"}')
    levelList <- levels$find(query = qryString,
                             fields = '{"name" : true,"cutOff": true}')
    if(nrow(levelList)>0){
      names(levelList) <- c('id','cutOff','name')
    } else {
      cat('No levels found\n')
      return(NULL)
    }
    return(levelList)
  }
  else{
    return(NULL)
  }
}

#' Exclude a judge from a task
#'
#' @param judge_id The judge id
#' @param connStr A connection string.
#' @return Nothing
#' @examples
#' excludeJudge('o544AXuwEBADM9Lrk', 'mongodb://')
#' @export
excludeJudge <- function(judge_id,connStr){
  judges <- mongolite::mongo('judges',url=connStr)
  qry <- paste0('{"_id":"',judge_id,'"}')
  judges$update(query=qry, update='{"$set":{"excludeAnalysis": 1}}')
  return (NULL)
}

#' Update anchor value for a person id
#'
#' @param id The person id
#' @param anchorValue The anchor value
#' @param connStr A connection string.
#' @return Nothing
#' @examples
#' updateAnchor('o544AXuwEBADM9Lrk',0.27, 'mongodb://')
#' @export
updateAnchor <- function(id,anchorValue,connStr){
  persons <- mongolite::mongo('persons',url=connStr)
  pipeline <- paste0('{"_id":"',id,'"}')
  updateStr <- paste0('{"$set":{"anchorScore": ',anchorValue,'}}')
  persons$update(pipeline, updateStr)
  return (NULL)
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

#' Get persons from a task or tasks
#'
#' @param syllabus The task syllabus
#' @param connStr A connection string.
#' @return A data frame with persons
#' @examples
#' getPersons('ee64c76a-5312-4e95-b315-ef5aca44539b', 'mongodb://')
#' @export
#' @import dplyr
getPersons <- function(syllabus, connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  pipeline <- paste0('[{"$match" : {"syllabus" : "',syllabus,'"}},{"$lookup" : {"from" : "candidates", "localField" : "_id", "foreignField" : "localTask", "as" : "taskCandidates"}}, { "$unwind" : {"path" : "$taskCandidates"}},{"$project":{"taskCandidates" : 1.0}}, {"$project" : {"taskCandidates.owners" : 0.0,"taskCandidates.opponents":0.0,"taskCandidates.localOpponents":0.0,"taskCandidates.modOpponents":0.0,"taskCandidates.scans":0.0 }},{"$replaceRoot" : {"newRoot" : "$taskCandidates"}}]')
  taskPersons <- tasks$aggregate(pipeline,options = '{"allowDiskUse":true}')
  dfe_str <- "[0-9]{7}"
  taskPersons <- taskPersons %>% mutate(
    dfe = str_extract(taskName, dfe_str)  
  )
  return(taskPersons)
}

#' Get scores from a task or tasks
#'
#' @param taskName The task name, tasks will be matched if their name includes the taskName string.
#' @param connStr A connection string.
#' @return A data frame with scores
#' @examples
#' getScores('Sharing Standards 2017-2018 Year 3', 'mongodb://')
#' @export
#' @import dplyr
getScores <- function(taskName, connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  pipeline <- paste0('[
                     { "$match": { "name":{"$regex":"',taskName,'","$options":"i"}}},
                     { "$lookup": { "from": "persons", "localField": "_id", "foreignField": "task", "as": "Persons"} },
                     { "$unwind": { "path": "$Persons" } },
                     { "$project": { "taskName": "$name", "scaledScore":"$Persons.scaledScore","trueScore":"$Persons.trueScore"} } ]')
  taskPersons <- tasks$aggregate(pipeline,options = '{"allowDiskUse":true}')
  dfe_str <- "[0-9]{7}"
  taskPersons <- taskPersons %>% mutate(
    dfe = str_extract(taskName, dfe_str)  
  )
  return(taskPersons)
}

#' Get decisions from a task or tasks
#'
#' @param taskName The task name, tasks will be matched if their name includes the taskName string.
#' @param connStr A connection string.
#' @return A data frame with decisions.
#' @examples
#' getDecisions('Sharing Standards 2017-2018 Year 3 9365206', 'mongodb://')
#' @export
#' @import dplyr
getDecisions <- function(taskName, connStr) {
  
  tasks <- mongolite::mongo('tasks',url=connStr)
  
  pipeline <- paste0('[
  { "$match": { "name":{"$regex":"',taskName,'","$options":"i"}}},
  { "$project": {"_id":1, "name":1}},
  { "$lookup": {
      "from" : "persons",
      "localField" : "_id",
      "foreignField" : "task",
      "as" : "Persons"}},
  { "$project": {"_id":1, "name":1, "Persons._id":1, "Persons.name":1}},
  {
      "$lookup": {
      "from" : "judges",
      "localField" : "_id",
      "foreignField" : "task",
      "as" : "Judges"
      }
      },{
      "$project": {
      "_id":1, "name":1, "Persons":1, "Judges._id":1, "Judges.email":1
      }
      },{
      "$lookup": {
      "from" : "decisions",
      "localField" : "_id",
      "foreignField" : "task",
      "as" : "Decisions"
      }
      }
      ]')
  
  taskDecisions <- tasks$aggregate(pipeline,options = '{"allowDiskUse":true}')
  
  decisions <- taskDecisions$Decisions[[1]]
  judges <- taskDecisions$Judges[[1]]
  players <- taskDecisions$Persons[[1]]
  tasks <- taskDecisions %>% select("_id","name")
  
  # print(head(decisions))
  # print(head(judges))
  # print(head(tasks))
  # print(head(players))
  
  if(length(decisions)>0){
    
    df <- left_join(decisions, judges, by=c('judge'='_id'))
    df <- left_join(df, players, by=c('chosen'='_id'))
    df <- left_join(df, players, by=c('notChosen'='_id'))
    df <- left_join(df, tasks, by=c('task'='_id'))
    df <- df %>% filter(!is.na(name.x),!is.na(name.y)) %>%
      select(task=name,chosen=name.x, notChosen=name.y,judge=email,timeTaken,createdAt,judge_id=judge)
    
  } else {
    df <- NULL
  }
  
  return(df)
}

#' Set script as export into anchor set
#'
#' @param name The script name
#' @param task The task id
#' @param connStr A connection string.
#' @return Nothing
#' @examples
#' updateExport('XcuTh8dh9ontyKj86','7CHCVQ', 'mongodb://')
#' @export
updateExport <- function(name, task, connStr){
  persons <- mongolite::mongo('persons',url=connStr)
  pipeline <- paste0('{"name":"',name,'","task":"',task,'"}')
  updateStr <- paste0('{"$set":{"isAnchor": "Y"}}')
  persons$update(pipeline, updateStr)
  return (NULL)
}

#' Set scaling factors for tasks
#'
#' @param task The task id
#' @param uScale Scaling factor
#' @param uiMean Mean of scale
#' @param connStr A connection string.
#' @return Nothing
#' @examples
#' updateScaling('FYB5kMkoh2v5sLsY7',6, 55, 'mongodb://')
#' @export
updateScaling <- function(task,uScale,uiMean,connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  pipeline <- paste0('{"_id":"',task,'"}')
  updateStr <- paste0('{"$set":{"anchorScaling.uScale": ',uScale,', "anchorScaling.uiMean":',uiMean,'}}')
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
                         fields = '{"name": true, "scaling.uScale" : true,"scaling.uiMean": true, "anchorScaling.uScale" : true, "anchorScaling.uiMean" : true}')
  tasks <- jsonlite::flatten(taskList)
  return(tasks)
}

#' Get sales data. Updated for nmm-vegas-db.
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

#' Get pages for a candidate
#'
#' @param qrcode QR Code
#' @param task Task id
#' @param connStr A connection string.
#' @return A count of pages
#' @examples
#' getPages('AF85HY','w2XzcMW3Rwb7pthe6', 'mongodb://')
#' @export
#' @import dplyr
getPages <- function(qrcode, task, connStr){
  pdfs <- mongolite::mongo('scans.qrcodes.pdfs',url=connStr)
  qryString <- paste0('{"qrcode":"',qrcode,'","task":"',task,'"}')
  pages <- pdfs$find(qryString)
  n <- nrow(pages)
  return(n)
}


#' Get judging pair for a judge id
#'
#' @param id The judge id.
#' @param connStr A connection string.
#' @return A named list with the id of the pair & name of the left and right script
#' @examples
#' getJudging('efY4MWWcNuoHYPZ5B', 'mongodb://')
#' @export
getJudging <- function(judge, connStr) {
  
  judging <- mongolite::mongo('judging',url=connStr)
  
  pipeline <- paste0('[{
                     "$match": {"pair.judge":"',judge,'"}},
                     {"$lookup":
                     {
                     "from": "persons",
                     "localField": "pair.left._id",
                     "foreignField": "_id",
                     "as": "leftScript"
                     }
                     },
                     {
                     "$lookup":
                     {
                     "from": "persons",
                     "localField": "pair.right._id",
                     "foreignField": "_id",
                     "as": "rightScript"
                     }
                     }]')
  
  judgingPair <- judging$aggregate(pipeline,options = '{"allowDiskUse":true}')
  left <- judgingPair$leftScript[[1]]$name
  right <- judgingPair$rightScript[[1]]$name
  id <-judgingPair$`_id` 
  return(c("id"=id,"left"=left,"right"=right))
}

#' Remove a judging pair with a judging id from getJudging
#'
#' @param judgingid The name of the pair to remove
#' @param connStr A connection string.
#' @return NULL
#' @examples
#' removeJudgingId('gpftTfdPKeBEiz7pd', 'mongodb://')
#' @export
#' @import dplyr
removeJudgingId <- function(judgingid, connStr){
  judging <- mongolite::mongo('judging',url=connStr)
  qryString <- paste0('{"_id":"',judgingid,'"}')
  judging$remove(qryString, just_one = TRUE)
  return(NULL)
}
