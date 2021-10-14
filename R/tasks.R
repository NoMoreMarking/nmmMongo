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

#' Set fromSyllabus for a task
#'
#' @param task The task id
#' @param syllabus The syllabus id
#' @param connStr A connection string.
#' @return Nothing
#' @examples
#' updateFromSyllabus('FYB5kMkoh2v5sLsY7','FYB5kMkoh2v5sLsY8', 'mongodb://')
#' @export
updateFromSyllabus <- function(task,syllabus,connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  pipeline <- paste0('{"_id":"',task,'"}')
  updateStr <- paste0('{"$set":{"fromSyllabus": "',syllabus,'"}}')
  out <- tasks$update(pipeline, updateStr)
  taskReturn <- tasks$find(query = pipeline, fields = '{"name" : true, "syllabus": true}')
  syllabusCollection <- mongolite::mongo('syllabus', url=connStr)
  syllabusReturn <- syllabusCollection$find(query=paste0('{"_id":"',syllabus,'"}'), fields = '{"name" : true}')
  cat('fromSyllabus for ', taskReturn$name, ' set to ', syllabusReturn$name, '\n')
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