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
#' @return A data frame with user details.
#' @examples
#' getJudges('ocjZYZAYLuZJyAv4h', 'mongodb://')
#' @export
getJudges <- function(taskId,connStr){
  judges <- mongolite::mongo(db='nmm-v2',collection="judges",url=connStr)
  qryString <- paste0('{"task":"',taskId,'"}')
  judgeList <- judges$find(query = qryString,
                         fields = '{"_id" : true,"emailLower":true,"localComparisons":true,"trueScore":true,"medianTimeTaken":true,"createdAt": true}')
  return(judgeList)
}

#' Get task ids, names and number of decisions completed.
#'
#' @param taskName Tasks will be matched if their name includes the taskName string.
#' @param connStr A connection string.
#' @return A data frame with task details.
#' @examples
#' getTasks('Sharing Standards 2017-2018 Year 5', 'mongodb://') will match all Year 5 tasks.
#' @export
getTasks <- function(taskName,connStr){
  tasks <- mongolite::mongo(db='nmm-v2',collection="tasks",url=connStr)
  qryString <- paste0('{"name":{"$regex":"',taskName,'","$options":"i"}}')
  taskList <- tasks$find(query = qryString,
                         fields = '{"name" : true,"anchor" : true,"dash.candidates": true,"dash.judgements": true,"dash.judges": true,"reliability":true,"completed.genPages":true,"completed.respPages":true,"completed.valid":true,"completed.invalid":true,"completed.notDetected":true,"completed.unread":true}')
  tasks <- jsonlite::flatten(taskList)
  if(nrow(tasks)>0){
    if(ncol(tasks==6)){
      names(tasks) <- c('id','name','anchor','reliability','decisions','candidates','judges')
    } else {
      names(tasks) <- c('id','name','anchor','reliability','decisions','candidates','judges','generatedPages','scanUploads','valid','invalid','notDetected','unread')
    }
    return(tasks)
  } else {
    cat('No tasks found')
    return(NULL)
  }

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
#' @param taskName The task name, tasks will be matched if their name includes the taskName string.
#' @param connStr A connection string.
#' @return A data frame with persons
#' @examples
#' getPersons('Sharing Standards 2017-2018 Year 3', 'mongodb://')
#' @export
#' @import dplyr
getPersons <- function(taskName, connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  pipeline <- paste0('[
  { "$match": { "name":{"$regex":"',taskName,'","$options":"i"}}},
    { "$lookup": { "from": "persons", "localField": "_id", "foreignField": "task", "as": "Persons"} },
    { "$unwind": { "path": "$Persons" } },
    { "$project": { "task": "$_id", "taskName": "$name", "person": "$Persons._id", "name": "$Persons.name", "status": "$Persons.status", "candidate": "$Persons.bio.candidate", "firstName": "$Persons.bio.firstName", "lastName": "$Persons.bio.lastName", "dob": "$Persons.bio.dobs", "gender": "$Persons.bio.gender", "group": "$Persons.bio.group", "pp": "$Persons.bio.pupilPremium" , "comparisons":"$Persons.comparisons","theta":"$Persons.trueScore","seTrueScore":"$Persons.seTrueScore","scaledScore":"$Persons.scaledScore","seScaledScore":"$Persons.seScaledScore","level":"$Persons.level", "infit":"$Persons.infit", "anchorScore":"$Persons.anchorScore"} } ]')
  taskPersons <- tasks$aggregate(pipeline,options = '{"allowDiskUse":true}')
  if(nrow(taskPersons)>0){
    chr <- nchar(unique(taskPersons$taskName))
    strt <- chr - 6
    taskPersons <- taskPersons %>% mutate(dfe=substr(taskName,start=strt,stop=chr))
  }
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
  if(nrow(taskPersons)>0){
    chr <- nchar(unique(taskPersons$taskName))
    strt <- chr - 6
    taskPersons <- taskPersons %>% mutate(dfe=substr(taskName,start=strt,stop=chr))
  }
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
