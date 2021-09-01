# Loading the GenBank Data: 

The GenBank Data Loader is a data pipeline written primarily in Java to download the entire GenBank data server and load it into a given MySQL server. 
Link: https://bitbucket.org/UVM-BIRD/genbank-loader/src/master/

## Step 1: Maven
You need Apache Maven to run the creation of the .jar files for this pipelines. This is already installed on Oscar. If not already available, use the following: 
https://maven.apache.org/download.cgi

## Step 2: Prepare 
The pre-existing genbank pipeline will work but may cause some errors due to it being written in 2015 and some version changes to MySQL and Java. 

Batch jobs:
I ran the Prepare and Load steps through batch jobs on Oscar, which allowed them to run in the background and not take up much time. I used a time maximum of 100 hours and then the given command for prepare or load. 
Command: 
```
Java -jar target/genbank-loader-1.0.jar —prepare
```
#### Changes to Pom.XML: 
- Change version to 8.0.21
- Added the following repository section to the end of the Pom.XML for Oscar specifically (pom_Oscar.xml) 

#### Changes to Load.java: src/main/java/edu/uvm/ccts/genbank/Load.java
- private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
- Added "?serverTimezone=UTC&allowLoadLocalInfile=true" to the String url JDBC

#### Changes to Record.java: src/main/java/edu/uvm/ccts/genbank/model/Record.java
- Changed populateGInumber to [0] because there is no index 1 anymore
- This change is due to GenBank changing their data organization 

With this, hopefully, prepare will work! If it doesn’t, try changing some of these variables, particularly the set of added strings to the String url JDBC in Load.java. Some of these variables are server dependent. 

## Step 3: Load
Load required a few more changes for me particularly to get this process working. The main issue is that the current set of code in GenBank loader relies on a set of background scripts by UVM that I was able to get access to in order to change some of them. 
Link: https://bitbucket.org/UVM-BIRD/ccts-common/src/master/

In particular, the DB folder within src/ is the scripts that I was able to edit to fix the load process. The DB folder can be copied and placed into your local genbank-loader (within the genbank folder), where I edited the following: 

#### Changes to: MetaGenbankLoader.java: 
- import edu.uvm.ccts.genbank.db.loader.AbstractFTPLoader

#### Changes to DB Loader: 
- Initialize the following files in loader with “package edu.uvm.ccts.genbank.db.loader;”
  - Loader/AbstractFileLoader.java
  - Loader/AbstractFTPLoader.java
  - Loader/AbstractLoader.java
- In AbstractLoader.java, update dbLoad (line 94) by removing the alter statements starting with DBUtil.executeUpdate
```
DBUtil.executeUpdate("set session sql_log_bin = OFF", dataSource); 
DBUtil.executeUpdate("load data local infile '" + filename + "' into table " + table, dataSource);
DBUtil.executeUpdate("set session sql_log_bin = ON", dataSource);
```
#### Changes to overall process: 
- Use truncate from MySQL command line to clean out the data tables you are going to update
- However, this requires additional permission for the “drop” command from the DB team
- Once you have gotten permissions, use truncate on the command line and not in the script as I had some issues with that and allows the manual operations used later on to add onto pre existing tables with data

Batch job Command: 
```
java -jar target/genbank-loader-1.0.jar --load -h pursamydbcit.services.brown.edu -u vramanan -p <password>
```
*I had to use my password on the command line to get this work with the batch job

Changes to the file loading: 
- I was able to load the following tables easily: Basic, keywords, journals and dbxrefs
- Authors and annotations were too large to load so I split them into 5 and 10 files respectively and manually loaded them after that. 
- I do not advise trying to mass load them at once. I tried it twice and each time it crashed due to communication failures just a day before they were completed (approximately 11-12 days)

#### Splitting authors and annotations: 
These were the results of the “wc -l” command on the authors and annotations files to find their line count: 
- Authors: 3,322,431,281
- Annotations: 6,712,480,648
This is the split command for annotations – again use a batch job rather than command line
```
Split -a 1 -l 700,000,000 annotations.txt annotations
```
Apply same logic to authors

Once split, make sure to *not* truncate authors and annotations as you are adding on each individual file to the previously loaded table
I made a flowchart so I could quickly go through the motions for each manual update: 
1. Rm <previousfile>
2. Mv <nextfile> <nextfile.txt> 
3. Adds “.txt” extension
4. Update FeatureTableParser.java with new name of file
  - src/main/java/edu/uvm/ccts/genbank/FeatureTableParser.java
5. Comment out the defining variables of the files you don’t need
6. Change the name of the annotations or authors file you are using with, for example “authorsa.txt” (a is first file of the 5, followed by bcde)
7. Maven package
8. Update the batch file script
9, Run script
  
Use the following command to check the row count of the tables to see the updates as you’re going allowing: 
```
Select table_rows "Rows Count" from information_schema.tables where table_name="annotations" and table_schema="genbank";
```
Try not to use the “select count(*) from <table>” because that’s a long linear time
