#' Get syllabus object from a syllabus id
#'
#' @param id The name for the syllabus
#' @param connStr A connection string.
#' @return A data frame with syllabus object
#' @examples
#' getSyllabusById('abc', 'mongodb://') 
#' @export
getSyllabusById <- function(id,connStr){
  syllabusCollection <- mongolite::mongo(db='nmm-vegas-db',collection="syllabus",url=connStr)
  qryString <- paste0('{"_id":"',id,'"}')
  syllabusList <- syllabusCollection$find(query = qryString,fields = '{"name" : true, "acYear": true, "modCode":true, "yearGroup":true, "product":true, "writingAgeSet":true, "writingAge": true,"activeStart":true, "startDate":true, "startYear":true, \"filePrompt_judging\":true, \"filePrompt_studentAssessTab\":true, \"filePrompt_studentJudgeSumm\":true, \"filePrompt_teacherAssessTab\":true, \"folderPrompts_createQuiz\":true, \"folderPrompts_quizTemplates\":true}')
  return(syllabusList)
}


#' Get syllabus object from a syllabus name
#'
#' @param name The name for the syllabus
#' @param connStr A connection string.
#' @return A data frame with syllabus object
#' @examples
#' getSyllabusByName('SACS', 'mongodb://') 
#' @export
getSyllabusByName <- function(name,connStr){
  syllabusCollection <- mongolite::mongo(db='nmm-vegas-db',collection="syllabus",url=connStr)
  qryString <- paste0('{"name":"',name,'"}')
  syllabusList <- syllabusCollection$find(query = qryString,fields = '{"name" : true, "acYear": true, "modCode":true, "yearGroup":true, "product":true, "writingAgeSet":true, "writingAge": true,"activeStart":true, "startDate":true, "startYear":true, \"filePrompt_judging\":true, \"filePrompt_studentAssessTab\":true, \"filePrompt_studentJudgeSumm\":true, \"filePrompt_teacherAssessTab\":true, \"folderPrompts_createQuiz\":true, \"folderPrompts_quizTemplates\":true}')
  return(syllabusList)
}

#' Get syllabus object from a product name
#'
#' @param product The product for the syllabus
#' @param connStr A connection string.
#' @return A data frame with syllabus objects
#' @examples
#' getSyllabusByProduct('productid', 'mongodb://') 
#' @export
getSyllabusByProduct <- function(product,connStr){
  syllabusCollection <- mongolite::mongo(db='nmm-vegas-db',collection="syllabus",url=connStr)
  qryString <- paste0('{"product":"',product,'"}')
  syllabusList <- syllabusCollection$find(query = qryString,fields = '{"name" : true, "acYear": true, "modCode":true, "yearGroup":true, "product":true, "activeStart":true, "startDate":true, "startYear":true, "filePrompt_judging":true, "filePrompt_studentAssessTab":true, "filePrompt_studentJudgeSumm":true, "filePrompt_teacherAssessTab":true, "folderPrompts_createQuiz":true, "folderPrompts_quizTemplates":true}')
  return(syllabusList)
}

#' Set ready for judging on a syllabus
#'
#' @param syllabus The syllabus id
#' @param ready Set ready or not. Boolean.
#' @param connStr A connection string.
#' @return List with modifiedCount, matchedCount, upsertedCount
#' @examples
#' setSyllabusReadyForJudging('ad888bb7-e47a-4e6b-b827-1498c752a389',TRUE, 'mongodb://')
#' @export
setSyllabusReadyForJudging <- function(syllabus,ready,connStr){
  tasks <- mongolite::mongo('tasks',url=connStr)
  pipeline <- paste0('{"syllabus":"',syllabus,'"}')
  if(ready){
    updateStr <- paste0('{"$set":{"readyForJudging": "',lubridate::today(),'"}}')  
  } else {
    updateStr <- '{"$unset" : {"readyForJudging":""}}'
  }
  out <- tasks$update(pipeline, updateStr,multiple = TRUE)
  return (out)
}

#' Set file prompt paths on a syllabus
#'
#' Sets the four filePrompt fields using a shared path suffix, prepending
#' the appropriate subdirectory for each prompt type.
#'
#' @param syllabus The syllabus id
#' @param path Path suffix appended to each prompt subdirectory
#' @param connStr A connection string.
#' @return List with modifiedCount, matchedCount, upsertedCount
#' @examples
#' setSyllabusFilePrompts('abc123', 'apw/2025-2026-y6.txt', 'mongodb://')
#' @export
setSyllabusFilePrompts <- function(syllabus, path, connStr) {
  syllabusCollection <- mongolite::mongo(db = 'nmm-vegas-db', collection = 'syllabus', url = connStr)
  qryString <- paste0('{"_id":"', syllabus, '"}')
  updateStr <- paste0(
    '{"$set":{',
      '"filePrompt_judging":"judging/', path, '",',
      '"filePrompt_studentJudgeSumm":"studentJudgeSumm/', path, '",',
      '"filePrompt_studentAssessTab":"studentAssessTab/', path, '",',
      '"filePrompt_teacherAssessTab":"teacherAssessTab/', path, '",',
      '"folderPrompts_createQuiz":"quizzes/', path, '",',
      '"folderPrompts_quizTemplates":"quiz-templates/', path, '"',
    '}}'
  )
  out <- syllabusCollection$update(qryString, updateStr)
  return(out)
}