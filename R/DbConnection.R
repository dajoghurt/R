library(rmongodb)

makeDbConnection <- function(hostName="localhost", dbName="Brainbook", collType)
{
  retList <- list()
  retList[["mongo"]] <- mongo.create(host = hostName, db=dbName)
  
  if (mongo.is.connected(retList$mongo)==FALSE){
    stop( paste("Failed to connect to DB ", dbName, " on ", hostName) )
  }
  
  collName <- vector()
  if (collType=="Patients")
    collName <- "Patients"
  else if (collType=="Jobs")
    collName <- "Jobs"
  else
    stop( paste("Unsupported type collection: ", collType));
  
  retList[["coll"]] <- paste(dbName, collName, sep='.')
  retList
}