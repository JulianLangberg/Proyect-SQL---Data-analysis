#Problem 1.1: Establish a connection to the Db2 database
library(RODBC);

dsn_driver <- "{IBM DB2 ODBC Driver - IBMDBCL1}"
dsn_database <- "bludb"           
dsn_hostname <- "ba99a9e6-d59e-4883-8fc0-d6a8c9f7a08f.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud"
dsn_port <- "31321"   
dsn_protocol <- "TCPIP"        
dsn_uid <- "bzt01197"       
dsn_pwd <- "d738KCxiuDiBIP8T"    
dsn_security <- "ssl"

conn_path <- paste("DRIVER=",dsn_driver,";DATABASE=",dsn_database,
                   ";HOSTNAME=",dsn_hostname, ";PORT=",dsn_port, ";PROTOCOL=",dsn_protocol,
                   ";UID=",dsn_uid, ";PWD=",dsn_pwd,";SECURITY=",dsn_security, sep="")
conn <- odbcDriverConnect(conn_path, believeNRows=FALSE)
conn



##Problem 1: Create tables

##Check whether these tables already exist, and drop them if so
tables <- c("FARM_PRICESA", "MONTHLY_FX1A") 
#c("CROP_DATA","CROP_DATA1","CROP_DATA2","FARM_PRICES1","FARM_PRICES", "DAILY_FIX", "MONTHLY_FX") 
for (table in tables) {
  # Drop tables if they already exist
  out <- sqlTables(conn, tableType = "TABLE",
                   tableName = table)
  if (nrow(out)>0) {
    err <- sqlDrop(conn, table,
                   errors=FALSE)  
    if (err==-1) {
      cat("An error has occurred.\n")
      err.msg <- odbcGetErrMsg(conn)
      for (error in err.msg) { 
        cat(error,"\n")
      }
    } 
    else {
      cat ("Table: ",table," was dropped\n")
    }
  }
  else {
    cat ("Table: ", table," does not exist\n")
  }
}


#Exercise 1, Problem 1: Create tables

#CREATE CROP_DATA TABLE 
df1 <- sqlQuery(conn, 
                "CREATE TABLE CROP_DATA_1 (
                CD_ID INTEGER NOT NULL,
                YEAR DATE NOT NULL,
                CROP_TYPE VARCHAR(20) NOT NULL,
                GEO VARCHAR(20) NOT NULL, 
                SEEDED_AREA INTEGER NOT NULL,
                HARVESTED_AREA INTEGER NOT NULL,
                PRODUCTION INTEGER NOT NULL,
                AVG_YIELD INTEGER NOT NULL,
                PRIMARY KEY (CD_ID)
)", 
                    errors=FALSE
                )
if (df1 == -1){
  cat ("An error has occurred.\n")
  msg <- odbcGetErrMsg(conn)
  print (msg)
} else {
  cat ("Table CROP_DATA_1 was created successfully.\n")
}


sqlQuery(conn,head(df1))
df2 <- sqlQuery(conn, 
                "CREATE TABLE FARM_PRICES_1 (
                FP_ID INTEGER NOT NULL,
                DATE DATE NOT NULL,
                CROP_TYPE VARCHAR(20) NOT NULL,
                GEO VARCHAR(20) NOT NULL, 
                PRICE_PERMT FLOAT(6),
                PRIMARY KEY(FP_ID)
)",
                        errors=FALSE
                )
if (df2==-1){
  cat("An error has ocurred.\n")
  msg <- odbcGetErrMsg(conn)
  print(msg)
} else { 
  cat("Table FARM_PRICES_1 was created successfully.\n")
}




df3 <- sqlQuery(conn, "CREATE TABLE DAILY_FX_1 (
                DFX_ID INTEGER NOT NULL,
                DATE DATE NOT NULL, 
                FXUSDCAD FLOAT(6),
                PRIMARY KEY (DFX_ID)
)",
                    errors=FALSE
                )

if (df3 == -1){
  cat ("An error has occurred.\n")
  msg <- odbcGetErrMsg(conn)
  print (msg)
} else {
  cat ("Table DAILY_FX_1 was created successfully.\n")
}


df4 <- sqlQuery(conn, 
                "CREATE TABLE MONTHLY_FX_1 (
                MFX_ID INTEGER NOT NULL,
                DATES DATE NOT NULL,
                FXUSDCAD FLOAT(6),
                PRIMARY KEY(MFX_ID)
)",
                        errors=FALSE
                )
if (df4==-1){
  cat("An error has ocurred.\n")
  msg <- odbcGetErrMsg(conn)
  print(msg)
} else { 
  cat("Table MONTHLY_FX_1 was created successfully.\n")
}

# Excercise 1, Problem 2: Read Datasets and Load Tables
CROP_DATA_df <- read.csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Annual_Crop_Data.csv', colClasses=c(YEAR="character"))
FARM_PRICES_df <- read.csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Monthly_Farm_Prices.csv', colClasses=c(date="character"))
DAILY_FX_df <- read.csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Daily_FX.csv', colClasses=c(date="character"))
MONTHLY_FX_df <- read.csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Monthly_FX.csv', colClasses=c(date="character"))

sqlSave(conn, CROP_DATA_df, "CROP_DATA_1", append=TRUE, fast=FALSE, rownames=FALSE, colnames=FALSE, verbose=FALSE)
sqlSave(conn, FARM_PRICES_df, "FARM_PRICES_1", append=TRUE, fast=FALSE, rownames=FALSE, colnames=FALSE, verbose=FALSE)
sqlSave(conn, DAILY_FX_df, "DAILY_FX_1", append=TRUE, fast=FALSE, rownames=FALSE, colnames=FALSE, verbose=FALSE)
sqlSave(conn, MONTHLY_FX_df, "MONTHLY_FX_1", append=TRUE, fast=FALSE, rownames=FALSE, colnames=FALSE, verbose=FALSE)


query_df1= "SELECT * FROM CROP_DATA_1 LIMIT 5"
sqlQuery(conn, query_df1)
query_df2= "SELECT * FROM FARM_PRICES_1 LIMIT 5"
sqlQuery(conn, query_df2)
query_df3= "SELECT * FROM DAILY_FX_1 LIMIT 5"
sqlQuery(conn, query_df3)
query_df4= "SELECT * FROM MONTHLY_FX_1 LIMIT 5"
sqlQuery(conn, query_df4)

#Exercise 3: Execute SQL queries using the RODBC R package
# Problem 3: How many records are in the farm prices dataset?
query3= "SELECT COUNT(CROP_TYPE) as Records FROM FARM_PRICES_1"
sqlQuery(conn, query3)


#PROBLEM 4 Which provinces are included in the farm prices dataset?
query4= "SELECT DISTINCT GEO FROM FARM_PRICES_1"
sqlQuery(conn, query4)

#Problem 5 How many hectares of Rye were harvested in Canada in 1968?
query5= "SELECT sum(HARVESTED_AREA) as Harvested FROM CROP_DATA_1 
WHERE CROP_TYPE= 'Rye' and GEO='Canada' and YEAR LIKE '1968%' "
sqlQuery(conn, query5)

#Problem 6: Query and display the first 6 rows of the farm prices table for Rye.
query6= "SELECT * FROM FARM_PRICES_1 WHERE CROP_TYPE= 'Rye' LIMIT 6"
sqlQuery(conn, query6)

#Problem 7: Which provinces grew Barley?
query7= "SELECT DISTINCT GEO FROM CROP_DATA_1 WHERE CROP_TYPE='Barley'"
sqlQuery(conn, query7)

#Problem 8: Find the first and last dates for the farm prices data.
query81= "SELECT max(DATE) as Last_Date FROM FARM_PRICES_1"
sqlQuery(conn, query81)
query82= "SELECT min(DATE) as First_Date FROM FARM_PRICES_1;"
sqlQuery(conn, query82)

#Problem 9: Which crops have ever reached a farm price greater than or equal to $350 per metric tonne?
query9= "select DISTINCT CROP_TYPE FROM FARM_PRICES_1 WHERE PRICE_PERMT >=350"
sqlQuery(conn, query9)

#Problem 10: Rank the crop types harvested in Saskatchewan in the year 2000 by their average yield. Which crop performed best?
query101= "SELECT CROP_TYPE, AVG_YIELD  FROM CROP_DATA_1 WHERE GEO='Saskatchewan' 
AND YEAR LIKE '2000%' ORDER BY AVG_YIELD DESC"
query102="SELECT CROP_TYPE, AVG_YIELD  FROM CROP_DATA_1 WHERE GEO='Saskatchewan' 
AND YEAR LIKE '2000%' ORDER BY AVG_YIELD DESC LIMIT 1"


sqlQuery(conn, query102)
view <- sqlQuery(conn, query101)
view


# Problem 11: Rank the crops and geographies by their average yield (KG per hectare) since the year 2000. 
#Which crop and province had the highest average yield since the year 2000?
query111= "SELECT CROP_TYPE, GEO, AVG(AVG_YIELD) AS TEN_YR_AVG_YIELD FROM CROP_DATA2
WHERE YEAR(YEAR)>=2000
GROUP BY CROP_TYPE, GEO
ORDER BY AVG(AVG_YIELD) DESC;"

sqlQuery(conn, query111)

query112= "SELECT CROP_TYPE, GEO, AVG(AVG_YIELD) AS TEN_YR_AVG_YIELD FROM CROP_DATA2
WHERE YEAR(YEAR)>=2000
GROUP BY CROP_TYPE, GEO
ORDER BY AVG(AVG_YIELD) DESC LIMIT 1;"

sqlQuery(conn, query112)

#Problem 12: Use a subquery to determine how much wheat was harvested in Canada in the most recent year of the data.
query12= "SELECT * FROM CROP_DATA_1 
WHERE GEO='Canada' AND CROP_TYPE='Wheat' AND YEAR=(SELECT MAX(YEAR) FROM CROP_DATA_1)"
sqlQuery(conn,query12)

#Problem 13: Use an implicit inner join to calculate the monthly price per metric tonne of Canola grown in
#Saskatchewan in both Canadian and US dollars. Display the most recent 6 months of the data.
query13= "SELECT P.CROP_TYPE,  F.DATES, P.PRICE_PERMT AS PRICE_PERMT_USD, P.PRICE_PERMT*F.FXUSDCAD AS PRICE_PERMT_CAD  
FROM FARM_PRICESA P, MONTHLY_FX1A F 
WHERE P.GEO='Saskatchewan' AND P.CROP_TYPE='Canola' ORDER BY F.DATES DESC limit 6 "
sqlQuery(conn, query13)