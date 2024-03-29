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

#' Unset anchors in a mod task
#'
#' @description Set anchor = false for all candidates in a mod task
#' @param modTask the name of the mod task
#' @param connStr A connection string.
#' @return the updated records
#' @export
#' 

# Unset anchors
unsetModAnchors <- function(modTask,connStr){
  candidates <- mongolite::mongo('candidates',url=connStr)
  pipeline <- paste0('{"modTask":"',modTask,'"}')
  updateStr <- paste0('{"$set":{ "anchor" : false}}')
  out <- candidates$update(pipeline, updateStr,multiple = TRUE)  
  return (out)
}

#' Update the mark property of a candidate
#' 
#' @description Update the mark property of a candidate
#' @param candidateId The id of the candidate
#' @param mark The mark to set
#' @param connStr A connection string.
#' @return The updated record
#' @export

# Update the mark property of a candidate
updateMark <- function(candidateId, mark, connStr){
  candidates <- mongolite::mongo('candidates',url=connStr)
  pipeline <- paste0('{"_id":"',candidateId,'"}')
  updateStr <- paste0('{"$set":{ "mark" : ',mark,'}}')
  out <- candidates$update(pipeline, updateStr,multiple = FALSE)  
  return (out)
}

#' Get writing age look up table
#'
#' @description Get the writingAgeSet look up table
#' @param writingAgeSet The writing age set
#' @param connStr A connection string.
#' @return The writing age set
#' @examples
#' getWritingAgeSet('KS3-2021-2022', 'mongodb://')
#' @export
#' @import dplyr
getWritingAgeSet <- function(writingAgeSet,connStr){
  scores <- mongolite::mongo('scaled.score.to.writing.age',url=connStr)
  qryString <- paste0('{"writingAgeSet":"',writingAgeSet,'"}')
  writingAges <- scores$find(qryString)
  return(writingAges)
}
