#!/bin/bash

#SBATCH -n 5
#SBATCH --mem=100G
#SBATCH -t 100:00:00

# This program uploads the GenBank DB to MySQL DB server

module load mysql/8.0.13
module load java
module load maven/3.8.1

# Prepare
java -jar target/genbank-loader-1.0.jar —-prepare

# Load all tables except authors and annotations
java -jar target/genbank-loader-1.0.jar --load -h pursamydbcit.services.brown.edu -u <ADDUSERNAME> -p <ADDPASSWORD>

# Split authors and annotations using following example:
split -a 1 -l 700,000,000 annotations.txt annotations

# Load authors and annotations individually