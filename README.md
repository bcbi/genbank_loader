# GenBank Loader #

GenBank Loader is a Java-based command-line utility that may be used to download the latest [GenBank](http://www.ncbi.nlm.nih.gov/genbank/) data from the [National Center for Biotechnology Information](http://www.ncbi.nlm.nih.gov/), and to load those publicly available DNA sequences into a local relational database to facilitate advanced analysis. 

[ORIGINAL CODE HERE, written by Matthew Storer](https://bitbucket.org/UVM-BIRD/genbank-loader/src/master/)

This is an updated version, edited by Vivek Ramanan in 2021, for the Brown Center of Biomedical Informatics (BCBI). 
I have added comments with ** before them so they hopefully make running the process easier. 

## Building GenBank Loader ##

Building the GenBank Loader is accomplished by running the following from the project's base directory:

    $ mvn package

## Preparation ##

Two preparatory steps must be taken before GenBank Loader may be used.  These are creating the local database into which GenBank data will be ultimately stored, and ensuring your system has sufficient disk space available to store everything.

### 1. Create Local Database ###

Before the GenBank Loader can be used, the database into which GenBank DNA sequences will be stored must be created.  GenBank Loader is pre-configured to use MySQL as its database back-end.  If you don't already have it installed, you should first download and install the [MySQL Community Server](https://dev.mysql.com/downloads/mysql/).

Once MySQL is installed, run the following command from the project's base directory:

    $ mysql -uroot < createdb.sql

This will create the _genbank_ database, with appropriate default values to integrate with the GenBank Loader's default settings.

** You should need to use this command because you will be using the BCBI DB pursamydbcit. 

### 2. Ensure You Have Sufficient Free Disk Space ###

The GenBank Loader's intermediate files and target database tables can take _hundreds of gigabytes_ of disk space.  Please be sure that you have at least **500 gigabytes** of disk space available before starting this process!

## Usage ##

To use the GenBank Loader and to see a list of command-line switches, run the program without any arguments:

    $ java -jar genbank-loader-1.0.jar 
    GenBank Loader
    --------------
    Copyright 2015 The University of Vermont and State Agricultural College.  All rights reserved.
    
    usage: genbank-loader [-d <string>] [-h <string>] --load | --prepare [-p <string>]  [-u <string>]
     -d,--db <string>     the database name (default 'genbank')
     -h,--host <string>   the database host (default 'localhost')
        --load            only load prepared files into the target database
     -p,--pass <string>   the database user password (default 'genbank')
        --prepare         only prepare database files for import
     -u,--user <string>   the database user name (default 'genbank')

## 3. Prepare GenBank Files For Import ##

The GenBank Loader has two primary modes of operation: `prepare` and `load`.  The preparation stage involves downloading GenBank files and transforming them such that they may be loaded into the target database.  The loading stage involves taking those prepared files and importing them into the target database.
 
To download and prepare files for import, run the following command:

    $ java -Xmx1000m -jar genbank-loader-1.0.jar --prepare
    GenBank Loader
    --------------
    Copyright 2015 The University of Vermont and State Agricultural College.  All rights reserved.
    
    INFO  Load - process started at Thu Jun 18 10:09:31 EDT 2015
    INFO  FTPClient - connected to 'ftp.ncbi.nlm.nih.gov'
    INFO  FTPClient - disconnected from 'ftp.ncbi.nlm.nih.gov'
    INFO  FTPClient - connected to 'ftp.ncbi.nlm.nih.gov'
    INFO  FTPClient - disconnected from 'ftp.ncbi.nlm.nih.gov'
    INFO  AbstractFTPLoader - [2]  start
    INFO  AbstractFTPLoader - [1]  start
    INFO  FTPClient - connected to 'ftp.ncbi.nlm.nih.gov'
    INFO  FTPClient - connected to 'ftp.ncbi.nlm.nih.gov'
    INFO  AbstractFTPLoader - [2]  (1/3749, 0%)  processing 'gbbct1.seq.gz'
    INFO  AbstractFTPLoader - [1]  (2/3749, 0%)  processing 'gbbct10.seq.gz'
    INFO  AbstractFTPLoader - [1]  (3/3749, 0%)  processing 'gbbct100.seq.gz'
    INFO  AbstractFTPLoader - [2]  (4/3749, 0%)  processing 'gbbct101.seq.gz'
    ...
    INFO  AbstractFTPLoader - [2]  (3748/3749, 99%)  processing 'complete.999.genomic.gbff.gz'
    INFO  AbstractFTPLoader - [1]  (3749/3749, 100%)  processing 'complete.wgs_mstr.gbff.gz'
    INFO  AbstractFTPLoader - [1]  done.
    INFO  FTPClient - disconnected from 'ftp.ncbi.nlm.nih.gov'
    INFO  AbstractFTPLoader - [2]  done.
    INFO  FTPClient - disconnected from 'ftp.ncbi.nlm.nih.gov'
    INFO  AbstractLoader - processing 3749 files across 2 threads took 13 hours, 24 minutes, 19 seconds
    INFO  Load - preparing GenBank files finished at Thu Jun 18 23:33:54 EDT 2015 (took 13 hours, 24 minutes, 23 seconds).

Downloaded files will be stored in a temporary folder while they are being processed, and are deleted after processing is completed.  Resultant files that will be imported into the local database are stored in the _out_ folder in the current working directory:

    $ ls out
    annotations.txt		authors.txt		basic.txt		dbxrefs.txt		journals.txt		keywords.txt

Each of these files is associated with a corresponding table in the target database.

## Running Prepare on Oscar ##

#### Batch Job Command:

I ran the Prepare and Load steps through batch jobs on Oscar, which allowed them to run in the background and not take up much time. I used a time maximum of 100 hours and then the given command for prepare or load. 

```
Java -jar target/genbank-loader-1.0.jar —prepare
```
#### Changes to Pom.XML: 
- This file holds information on Maven, MySQL, and the Oracle MySQL connection
- Change version to 8.0.21
- Added the following repository section to the end of the Pom.XML for Oscar specifically (pom_Oscar.xml) 

#### Changes to Load.java: src/main/java/edu/uvm/ccts/genbank/Load.java
- This holds information on the links to download and access the MySQL database
- private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
- Added "?serverTimezone=UTC&allowLoadLocalInfile=true" to the String url JDBC

#### Changes to Record.java: src/main/java/edu/uvm/ccts/genbank/model/Record.java
- This file goes through the individual Genbank records and processes them into files
- Changed populateGInumber to [0] because there is no index 1 anymore
- This change is due to GenBank changing their data organization 

With this, hopefully, prepare will work! If it doesn’t, try changing some of these variables, particularly the set of added strings to the String url JDBC in Load.java. Some of these variables are server dependent. 

#### Prepare Stage is Resumable ####

During the preparation stage, thousands of files are downloaded over potentially many hours.  It is possible that during this time, the process might be interrupted (perhaps by an `OutOfMemoryError`, lost network connection, user error, etc.).  Should any such interruption occur, it is important to know that the download and processing of data files may be resumed where it left off with an extremely remote chance of data corruption.

This is because metadata is generated about each successfully processed file (allowing the system to know where to resume operations), and because data generated from each downloaded file is processed independently, the results from which are appended to the master target files for database import only after all individual file processing has completed.

Corruption of the master files can occur **only** if the GenBank Loader is interrupted during the few milliseconds required to append individual file's data to its respective master file.  This is extremely unlikely to occur, as while the processing of an individual file may take many seconds, appending those data to the master file takes just a few milliseconds.

#### Important Note Regarding Memory and Performance ####

GenBank Loader is a multi-threaded process that can leverage the cores of your CPU to download and prepare GenBank files in parallel, which can dramatically improve performance.

When GenBank Loader executes its `prepare` function, it determines how much memory it has been allocated, and creates as many threads as it can safely use without negatively impacting other user and system processes.  GenBank Loader will use at least one core, but may use up to (_N_-2) cores, where _N_ is the total number of cores in your system's CPU.

GenBank Loader's preparation stage requires **400 megabytes of Java heap memory per thread** to properly execute, and may otherwise suffer severely degraded performance, or even fail with an `OutOfMemoryError`.  It is therefore suggested to allocate _at least 2.5 times_ the required memory per thread to the JVM, but more is better and will translate to improved performance, especially on systems with many CPU cores.

To allocate the minimum suggested memory to the GenBank Loader process, use the JVM option `-Xmx1000m` as in the example above.
 
See [Oracle's Java SE Documentation](http://docs.oracle.com/javase/7/docs/technotes/tools/windows/java.html) for details about `-Xmx` and other JVM options.

## 4. Load Prepared Files Into Local Database ##

To load the prepared GenBank files into the local database, execute the following:

    $ java -jar genbank-loader-1.0.jar --load
    GenBank Loader
    --------------
    Copyright 2015 The University of Vermont and State Agricultural College.  All rights reserved.
    
    INFO  Load - process started at Wed Jun 17 16:55:32 EDT 2015
    INFO  AbstractLoader - populating database from './out' -
    INFO  AbstractLoader -  loading './out/basic.txt' into table 'basic'
    INFO  DataSource - getConnection : establishing connection to 'genbank'
    INFO  AbstractLoader -  building indexes for table 'basic'
    INFO  AbstractLoader -  loading './out/keywords.txt' into table 'keywords'
    INFO  AbstractLoader -  building indexes for table 'keywords'
    INFO  AbstractLoader -  loading './out/dbxrefs.txt' into table 'dbxrefs'
    INFO  AbstractLoader -  building indexes for table 'dbxrefs'
    INFO  AbstractLoader -  loading './out/journals.txt' into table 'journals'
    INFO  AbstractLoader -  building indexes for table 'journals'
    INFO  AbstractLoader -  loading './out/authors.txt' into table 'authors'
    INFO  AbstractLoader -  building indexes for table 'authors'
    INFO  AbstractLoader -  loading './out/annotations.txt' into table 'annotations'
    INFO  AbstractLoader -  building indexes for table 'annotations'
    INFO  AbstractLoader - finished populating database.  took 12 hours, 6 minutes, 59 seconds
    INFO  Load - populating database with release '207' finished at Thu Jun 18 05:02:35 EDT 2015 (took 12 hours, 7 minutes, 3 seconds).

Note that as soon as the `load` process completes, you may safely delete intermediate database import files in the _out_ folder.

## Load in Oscar ##

Load required a few more changes for me particularly to get this process working. The main issue is that the current set of code in GenBank loader relies on a set of background scripts by UVM that I was able to get access to in order to change some of them. 
Link: https://bitbucket.org/UVM-BIRD/ccts-common/src/master/

In particular, the DB folder within src/ is the scripts that I was able to edit to fix the load process. The DB folder can be copied and placed into your local genbank-loader (within the genbank folder), where I edited the following: 

#### Changes to: MetaGenbankLoader.java: 
- import edu.uvm.ccts.genbank.db.loader.AbstractFTPLoader

#### Changes to DB Loader: 
- Initialize the following files in loader with “package edu.uvm.ccts.genbank.db.loader;”
  - Loader/AbstractFileLoader.java
  - Loader/AbstractFTPLoader.java
  - Loader/AbstractLoader.java: This code has the MySQL commands that loads the data
- In AbstractLoader.java, update dbLoad (line 94) by removing the alter statements starting with DBUtil.executeUpdate
```
DBUtil.executeUpdate("set session sql_log_bin = OFF", dataSource); 
DBUtil.executeUpdate("load data local infile '" + filename + "' into table " + table, dataSource);
DBUtil.executeUpdate("set session sql_log_bin = ON", dataSource);
```
- FeatureTableParser.java holds the variable names for each of the files to be processed - if you want to remove one or change a filename, do so here

#### Changes to overall process: 
- Use truncate from MySQL command line to clean out the data tables you are going to update
- However, this requires additional permission for the “drop” command from the DB team
- Once you have gotten permissions, use truncate on the command line and not in the script as I had some issues with that and allows the manual operations used later on to add onto pre existing tables with data

#### Batch job Command: 
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

#### Flowchart for Updating Authors and Annotations
I made a flowchart so I could quickly go through the motions for each manual update: 
1. Rm <previousfile>
2. Mv <nextfile> <nextfile.txt> 
3. Adds “.txt” extension
4. Update FeatureTableParser.java with new name of file
5. Remove "truncate" or "delete" from AbstractLoader.java in dbLoad
6. Comment out the defining variables of the files you don’t need
7. Change the name of the annotations or authors file you are using with, for example “authorsa.txt” (a is first file of the 5, followed by bcde)
8. Maven package
9. Update the batch file script
10. Run script

#### Checking MySQL for Updates ####
    
Use the following command to check the row count of the tables to see the updates as you’re going allowing: 
```
Select table_rows "Rows Count" from information_schema.tables where table_name="annotations" and table_schema="genbank";
```
Try not to use the “select count(*) from <table>” because that’s a long linear time


## License and Copyright ##

GenBank Loader is Copyright 2015 [The University of Vermont and State Agricultural College](https://www.uvm.edu/).  All rights reserved.

GenBank Loader is licensed under the terms of the [GNU General Public License (GPL) version 3](https://www.gnu.org/licenses/gpl-3.0-standalone.html).
