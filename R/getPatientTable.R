library(plyr)
library(digest)

flattenList <- function(list, expandLevels, arraysToIgnore = vector())
{
  listElements <- sapply(list, is.list)  
  parentList <- list[!listElements]
  #subLists <- list[listElements]
  
  for (parentName in names(parentList))
  {
    list[[parentName]] <- NULL
  }
  
  for (subListName in names(list))
  {
#     if (subListName %in% arraysToIgnore){
#       subLists[[subListName]] <- NULL
#       next
#     }
    
    if (expandLevels<=0)
    {
      temp <- list()
      temp[[subListName]] <- length(list[[subListName]])
      parentList <- c(parentList, temp)
      list[[subListName]] <- NULL
    }
  }
    
  if (length(parentList)>0)
  {
    parentList[sapply(parentList, is.null)] <- NA
    parentList <- parentList[sapply(parentList, class) != "mongo.oid"]
  }
  
  compact(list)
  
  if (length(list)>0 && expandLevels>=1)
  {
    flattenedLists <- lapply(list, flattenList, expandLevels=expandLevels-1, arraysToIgnore=arraysToIgnore)
    #result <- ldply(subLists, flattenList, expandLevels=expandLevels-1, arraysToIgnore=arraysToIgnore, .parallel=TRUE)
    
    result <- rbind.fill(flattenedLists)
#     for (subListName in names(subLists))
#     {
#       thisList <- subLists[[subListName]]
#       flattened <- flattenList(thisList, expandLevels-1, arraysToIgnore)
#       
# #       if (length(parentList)>0){
# #         if (length(flattened)>0)
# #           flattened <- cbind(parentList, flattened)
# #         else
# #           flattened <- as.data.frame(parentList)
# #       }
#       
#       if (length(flattened)>0)
#         result <- rbind.fill(result, flattened)
#     }

    if (length(parentList)>0)
    {
      parentFrame <- as.data.frame(parentList)
      parentFrame <- parentFrame[rep(1, each=nrow(result)),]
      result <- cbind(parentFrame, result)
    }
    return (result)
  }
  else
  {
    return (as.data.frame(parentList))
  }
}


cursorToFlatTable <- function(cursor, ...)
{
  res <- data.frame()
  while (mongo.cursor.next(cursor))
  {
    val <- flattenList(mongo.bson.to.list(mongo.cursor.value(cursor)), ...)
    #val <- mongo.bson.to.list(mongo.cursor.value(cursor))
    res <- rbind.fill(res, val)
  }
  
  return(as.data.frame(res))
}


getPatientTable <- function(connection)
{
  fields <- mongo.bson.from.JSON('{"uploadInfo": 0}')
  cursor <- mongo.find(connection$mongo, connection$coll, fields=fields)
  
  return (cursorToFlatTable(cursor, expandLevels=0, arraysToIgnore = "uploadInfo"))
}


calculateImageHash <- function(df, checkUnique = FALSE)
{
  # try to replace the very unhandy image references by a short hash
  imageColumns <- c("Images_pre","Images_ins", "Images_stu", "Images_ser", "Images_cla")
  df$Images_MD5 <- do.call(paste, df[, names(df) %in% imageColumns] )
  df$Images_MD5 <- sapply(df$Images_MD5, digest, algo="md5")
  
  if (checkUnique==FALSE || !anyDuplicated(df$Images_MD5))
    df  <- df[,!(names(df) %in% imageColumns)]
  else
    warning("Image hash was not unique. Keeping full instance information.")
  
  return (df)
}


getSliceSetTable <- function(connection, patientTable=matrix(nrow=0, ncol=0))
{
#    fields <- mongo.bson.from.JSON("{\"Studies\": 0, \"uploadInfo\": 0, \"VoxelObjects\": 0,
#  \"LabeledPoints\": 0}")
#    cursor <- mongo.find(mongo, collection, fields=fields)
#    return (cursorToFlatTable(cursor, expandLevels=2, arraysToIgnore = "uploadInfo"))
  
  patQuery <- character()
  if (nrow(patientTable)>0)
  {
    patQuery <- paste(patQuery, '{"$match": {"$or": [', sep='')    
    for(pat in 1:nrow(patientTable))
    {
      patQuery <- paste(patQuery, '{"$and": [ {"dcmPatientsName": "', patientTable[pat, 1],'"}, {"dcmPatientID": "', patientTable[pat, 2],'"}]}', sep="")
      if (pat<nrow(patientTable))
        patQuery<-paste(patQuery, ',', sep='')
    }    
    patQuery <- paste(patQuery, ']}}', sep='')
    
    print(patQuery)
    
    pipe0 <- mongo.bson.from.JSON(patQuery)
  }
  
  pipe1 <- mongo.bson.from.JSON('
    {"$project": {
        "dcmPatientsName": 1, 
        "dcmPatientID": 1, 
        "dcmPatientSex": 1, 
        "SliceSets": 1}}')
  
  pipe2 <- mongo.bson.from.JSON('{"$unwind": "$SliceSets"}')
  
  pipe3 <- mongo.bson.from.JSON('
    {"$project": {
        "_id": 0,
        "PatientName": "$dcmPatientsName",
        "PatientID": "$dcmPatientID",
        "sex": "$dcmPatientSex",
        "studyUID": "$SliceSets.Study",
        "dcmModality": "$SliceSets.dcmModality",
        "dcmSeriesDescription": "$SliceSets.dcmSeriesDescription",
        "width": "$SliceSets.width",
        "height": "$SliceSets.height",
        "numberOfSlices": "$SliceSets.numberOfSlices",
        "averageSliceDistanceZ": "$SliceSets.averageSliceDistanceZ",
        "minSliceDistanceZ": "$SliceSets.minSliceDistanceZ",
        "maxSliceDistanceZ": "$SliceSets.maxSliceDistanceZ",
        "orientation": "$SliceSets.orientation",
        "pixelSizeX": "$SliceSets.pixelSizeX",
        "pixelSizeY": "$SliceSets.pixelSizeY",
        "scanTime": "$SliceSets.scanTime",
        "sequenceType": "$SliceSets.sequenceType",
        "sliceSpaceType": "$SliceSets.sliceSpaceType",
        "FoR": "$SliceSets.FoR",
        "uploadComment": "$SliceSets.uploadComment",
        "uploadDateTime": "$SliceSets.uploadDateTime",
        "uploadUser": "$SliceSets.uploadUser",
        "Images_pre": "$SliceSets.Images.pre",
        "Images_ins": "$SliceSets.Images.ins",
        "Images_ser": "$SliceSets.Images.ser",
        "Images_stu": "$SliceSets.Images.stu",
        "Images_cla": "$SliceSets.Images.cla"
    }}')
    
  cmd <- list(pipe1, pipe2, pipe3)
  if (nrow(patientTable)>0)
    cmd <- c(pipe0, cmd)
  
  aggrResult = mongo.bson.to.list(mongo.aggregation(connection$mongo, connection$coll, cmd))[["result"]]
  
  df <-  rbind.fill(lapply(aggrResult, as.data.frame ) ) 
  
  
  return (calculateImageHash(df, checkUnique=TRUE))  
}

