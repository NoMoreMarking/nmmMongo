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
  # dfe_str <- "[0-9]{7,12}"
  # taskPersons <- taskPersons %>% mutate(
  #   dfe = str_extract(taskName, dfe_str)  
  # )
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
  taskPersons <- candidates$find(qryString, fields='{"owners" : 0.0,"opponents":0.0,"localOpponents":0.0,"modOpponents":0.0 }')
  return(taskPersons)
}

#' Set candidate anchor values
#'
#' @description Update candidate anchoring properties in a mod task
#' @param firstName the name of the anchor
#' @param lastName a label for the anchor
#' @param modTask the id of the moderation task
#' @param theta the anchor value of the script
#' @param connStr A connection string.
#' @return the updated records
#' @export
#' 

setAsAnchor <- function(firstName,lastName,modTask,theta, connStr){
  candidates <- mongolite::mongo('candidates',url=connStr)
  pipeline <- paste0('{"firstName":"',firstName,'","localTask":"',modTask,'"}')
  updateStr <- paste0('{"$set":{ "modTask":"',modTask,'","anchor" : true, "anchorScore" : ',theta,',"lastName":"',lastName,'"}}')
  out <- candidates$update(pipeline, updateStr,multiple = FALSE)  
  return (out)
}
