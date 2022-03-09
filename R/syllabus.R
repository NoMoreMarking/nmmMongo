#' Get syllabus object from a syllabus name
#'
#' @param name The name for the syllabus
#' @param connStr A connection string.
#' @return A data frame with syllabus object
#' @examples
#' getSyllabusByName('SACS', 'mongodb://') will match all SACS syllabuses.
#' @export
getSyllabusByName <- function(name,connStr){
  syllabusCollection <- mongolite::mongo(db='nmm-vegas-db',collection="syllabus",url=connStr)
  qryString <- paste0('{"name":{"$regex":"',name,'","$options":"i"}}')
  syllabusList <- syllabusCollection$find(query = qryString,fields = '{"name" : true, "acYear": true, "modCode":true, "yearGroup":true, "product":true, "writingAgeSet":true, "writingAge": true}')
  return(syllabusList)
}

#' Get syllabus object from a product name
#'
#' @param product The product for the syllabus
#' @param connStr A connection string.
#' @return A data frame with syllabus objects
#' @examples
#' getSyllabusByProduct('productid', 'mongodb://') will match all SACS syllabuses.
#' @export
getSyllabusByProduct <- function(product,connStr){
  syllabusCollection <- mongolite::mongo(db='nmm-vegas-db',collection="syllabus",url=connStr)
  qryString <- paste0('{"product":"',product,'"}')
  syllabusList <- syllabusCollection$find(query = qryString,fields = '{"name" : true, "acYear": true, "modCode":true, "yearGroup":true, "product":true}')
  return(syllabusList)
}