
#' Get list of schools in a trust by trust and product name
#'
#' @param trustName The trust name
#' @param productName The product name for the trust
#' @param connStr A connection string
#' @return A data frame with school dfes
#' @examples
#' getSchoolsByTrust('Central South', 'mongodb://') will return the schools with the trust Central South
#' @export
getSchoolsByTrust <- function(trustName,productName,connStr){
  matsCollection <- mongolite::mongo(db='nmm-vegas-db',collection="mats",url=connStr)
  pipeline <- paste0('
    [
      { 
        "$match" : { 
          "name" : "',trustName,'"
        }
      }, 
      { 
        "$lookup" : { 
          "from" : "products", 
          "localField" : "product", 
          "foreignField" : "_id", 
          "as" : "productMat"
        }
      }, 
      { 
        "$unwind" : { 
          "path" : "$productMat"
        }
      }, 
      { 
        "$match" : { 
          "productMat.productName" : "',productName,'"
        }
      }, 
      { 
        "$project" : { 
          "dfes" : "$schools"
        }
      }, 
      { 
        "$unwind" : { 
          "path" : "$dfes"
        }
      }
    ]')   
    
  dfes <- matsCollection$aggregate(pipeline,options = '{"allowDiskUse":true}')
  if(nrow(dfes)>0){
    return(dfes)
  } else {
    cat('No schools found for that trust') 
  }
}

