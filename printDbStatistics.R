source("getPatientTable.R")

printPatientStatistics <- function(patientTable)
{
  print(paste("Total number of patients:", nrow(patientTable) ))
  
  # patient's sex
  barplot(table(patientTable$dcmPatientsSex, useNA="ifany"), main="distribution of patient's sex")
  
  # number of slice sets
  hist(patientTable$SliceSets, main="Number of slice sets per patient", xlab="number of slice sets", ylab="number of patients")

  # number of studies
  hist(patientTable$Studies, main="Number of studies per patient", xlab="number of studies", ylab="number of patients")
  
  # age distribution TODO: what if birth date not present?
  doubleDate <- as.double( gsub('-', '', Sys.Date()) )
  hist((doubleDate-patientTable$dcmPatientsBirthDate)/10000, main="age distribution", xlab="age", ylab="number of patients")
}

printSliceSetStatistics <- function(sliceSetTable)
{
  print(paste("Total number of slice sets:", nrow(sliceSetTable) ))
  
  # modality distribution
  barplot(table(sliceSetTable$dcmModality), main="Slice Set Modalitiy")
  
  # sequence distribution
  barplot(table(sliceSetTable$sequenceType[sliceSetTable$dcmModality=="MR"], useNA="ifany"), main="MR sequence distribution")
    
  # number of slices per set
  hist(sliceSetTable$numberOfSlices, main="number of slices per set", xlab="number of slices", ylab="number of sets")
  
  # slice space type
  barplot(table(sliceSetTable$sliceSpaceType, useNA="ifany"), main="Slice Space Types")
}

printDbStatistics <- function()
{
  dbHost <- "localhost"
  dbName <- "Brainbook"
  collName <- "Patients"
  
  mongo <- mongo.create(host=dbHost, db=dbName)
  
  if (mongo.is.connected(mongo)==FALSE){
    print ("Failed to connect to DB.")
    return
  }  
  
  patientTable <- getPatientTable(mongo, paste(dbName,collName, sep='.'))
  printPatientStatistics( patientTable )
  
  sliceSetTable <- getSliceSetTable(mongo, paste(dbName,collName, sep='.'))
  printSliceSetStatistics( sliceSetTable )
  
  mongo.destroy(mongo)
}



