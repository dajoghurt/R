library(plyr)

flattenList <- function(list, expandLevels, arraysToIgnore)
{ 
  listElements <- sapply(list, is.list)
  parentList <- list[!listElements]
  subLists <- list[listElements]
    
  for (subListName in names(subLists))
  {
    if (subListName %in% arraysToIgnore){
      next
    }      
    
    if (expandLevels<=0)
    {         
      temp <- list()
      temp[[subListName]] = length(subLists[[subListName]])
      parentList <- c(parentList, temp)
    }
  }
    
  if (length(parentList)>0)
  {
    parentList[sapply(parentList, is.null)] <- NA
    parentList <- parentList[sapply(parentList, class) != "mongo.oid"]
  }
  
  if (length(subLists)>0 && expandLevels>=1)
  {
    result <- data.frame()
    for (subListName in names(subLists))
    {
      if ( subListName %in% arraysToIgnore ) {
        next
      }
      
      thisList <- subLists[[subListName]]
      flattened <- flattenList(thisList, expandLevels-1, arraysToIgnore)
      
      if (length(parentList)>0){
        if (length(flattened)>0)
          flattened <- cbind(parentList, flattened)
        else
          flattened <- as.data.frame(parentList)        
      }        
      
      if (length(flattened)>0)
        result <- rbind.fill(result, flattened)
    }
    return (result)
  }
  else
  {    
    return (as.data.frame(parentList))
  }    
}


flattenBSON <- function(bson, levelsToFlatten)
{
  result <- data.frame()
    
  parentList <- list()
  subArrays <- vector()
  
  iter <- mongo.bson.iterator.create(bson)
  type <-mongo.bson.iterator.next(iter)
  while (type!=mongo.bson.eoo){
    
    if (type == mongo.bson.oid){
      type <- mongo.bson.iterator.next(iter)
      next
    }
    
    if (type != mongo.bson.array){
      tempList <- list()
      tempList[[mongo.bson.iterator.key(iter)]] = mongo.bson.iterator.value(iter)
      
      parentList <- append(parentList, tempList)
    }
    else {
      size <- 0
      subarray <- mongo.bson.iterator.value(iter)
      
      tempList <- list()
      tempList[[mongo.bson.iterator.key(iter)]] = length(subarray)
      parentList <- append(parentList, tempList)
    }
    
    type <- mongo.bson.iterator.next(iter)
  }
  
  result <- as.data.frame(parentList)
  return (result)
}


cursorToFlatTable <- function(cursor, ...)
{  
  res <- data.frame()
  while (mongo.cursor.next(cursor)) 
  {
    val <- flattenList(mongo.bson.to.list(mongo.cursor.value(cursor)), ...)
    res <- rbind.fill(res, val)
  }
  
  return(as.data.frame(res))
}


getPatientTable <- function(mongo, collection)
{
#   fields <- mongo.bson.from.JSON("{\"SliceSets\": 0, \"Studies\": 0}")

  cursor <- mongo.find(mongo, collection) 
  
  return (cursorToFlatTable(cursor, expandLevels=0, arraysToIgnore = "uploadInfo"))  
}


getSliceSetTable <- function(mongo, collection)
{
  fields <- mongo.bson.from.JSON("{\"Studies\": 0, \"uploadInfo\": 0}")
  cursor <- mongo.find(mongo, collection, fields=fields) 
  return (cursorToFlatTable(cursor, expandLevels=2, arraysToIgnore = c("VoxelObjects", "Studies", "LabeledPoints", "uploadInfo")))  
}