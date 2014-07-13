getTestPointTable <- function(connection)
{
  pipe0 <- mongo.bson.from.JSON('
      {"$match": {"JobType": "UAREG_TEST"} }
  ')
  
  pipe1 <- mongo.bson.from.JSON('
  {"$project": {
      "_id": 0,
      "RunID": 1, 
      "StartTime": 1, 
      "EndTime": 1,
      "Computer": 1,
      "UserName": 1,
      "State:": 1,
      "Results": 1}}
  ')

  pipe2 <- mongo.bson.from.JSON('{"$unwind": "$Results"}')
  
  pipe3 <- mongo.bson.from.JSON('
  {"$project": {
        "RunID": 1, 
        "StartTime": 1, 
        "EndTime": 1,
        "Computer": 1,
        "UserName": 1,
        "State:": 1,
        "dcmPatientsName": "$Results.RefContents.dcmPatientsName",
        "dcmPatientID": "$Results.RefContents.dcmPatientID",
        "Images_pre": "$Results.RefContents.SliceSets.Images.pre",
        "Images_ins": "$Results.RefContents.SliceSets.Images.ins",
        "Images_ser": "$Results.RefContents.SliceSets.Images.ser",
        "Images_stu": "$Results.RefContents.SliceSets.Images.stu",
        "Images_cla": "$Results.RefContents.SliceSets.Images.cla",
        "items":      "$Results.items"}}
  ')
 
  pipe4 <- mongo.bson.from.JSON('{"$unwind": "$items"}')
  
  pipe5 <- mongo.bson.from.JSON('
  {"$project": {
      "description": "$items.description",
      "value": "$items.value",
      "ok": "$items.ok",
      "dcmPatientsName": 1,
      "dcmPatientID": 1,
      "RunID": 1, 
      "StartTime": 1, 
      "EndTime": 1,
      "Computer": 1,
      "UserName": 1,
      "State:": 1,
      "Images_pre": 1,
      "Images_ins": 1,
      "Images_ser": 1,
      "Images_stu": 1,
      "Images_cla": 1}}
  ')
    
  cmd <- list(pipe0, pipe1, pipe2, pipe3, pipe4, pipe5)
  aggrResult <- mongo.bson.to.list(mongo.aggregation(connection$mongo, connection$coll, cmd))[["result"]]
  #return( aggrResult)
  df <-  rbind.fill(lapply(aggrResult, as.data.frame ) )
  return (calculateImageHash(df))
}


addSliceSetDetails <- function(dataFrame, patientConnection)
{
  containedPatients <- table(dataFrame$dcmPatientsName, dataFrame$dcmPatientID)
  patientTable <- matrix(ncol=2, nrow=0)
  for (row in 1:nrow(containedPatients))
  {
    for (col in 1:ncol(containedPatients))
    {
      if (containedPatients[row, col]>0)
      {
        #print( paste(rownames(containedPatients)[row], colnames(containedPatients)[col]))
        patientTable <- rbind(patientTable, c(rownames(containedPatients)[row],colnames(containedPatients)[col]))
      }
    }
  }
  
  allSliceSets <- getSliceSetTable(patientConnection, patientTable)
  return ( merge(dataFrame, allSliceSets, by="Images_MD5") )
}