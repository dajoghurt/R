library(plyr)

flattenList <- function(list, expandLevels)
{ 
  listElements <- sapply(list, is.list)
  parentList <- list[!listElements]
  subLists <- list[listElements]
  
  if (expandLevels<=0)
  {
    for (subListName in names(subLists))
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
    for (sub in subLists)
    {
      flattened <- flattenList(sub, expandLevels-1)
      if (length(parentList)>0)
        flattened <- cbind(parentList, flattened)
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

cursorToFlatTable <- function(cursor, levelsToFlatten)
{  
  res <- data.frame()
  while (mongo.cursor.next(cursor)) 
  {
    val <- flattenList(mongo.bson.to.list(mongo.cursor.value(cursor)), levelsToFlatten)
    res <- rbind.fill(res, val)
  }
  
  return(as.data.frame(res))
}


getPatientTable <- function(mongo, collection)
{
#   fields <- mongo.bson.from.JSON("{\"SliceSets\": 0, \"Studies\": 0}")
  cursor <- mongo.find(mongo, collection) 
  return (cursorToFlatTable(cursor, 0))  
}


getSliceSetTable <- function(mongo, collection)
{
  fields <- mongo.bson.from.JSON("{\"Studies\": 0}")
  cursor <- mongo.find(mongo, collection, fields=fields) 
  return (cursorToFlatTable(cursor, 2))  
}