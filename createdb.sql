--
-- Copyright 2015 The University of Vermont and State Agricultural
-- College, Vermont Oxford Network.  All rights reserved.
--
-- Written by Matthew B. Storer <matthewbstorer@gmail.com>
--
-- This file is part of GenBank Loader.
--
-- GenBank Loader is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
--
-- GenBank Loader is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with GenBank Loader.  If not, see <http://www.gnu.org/licenses/>.
--

create database if not exists genbank;
create user if not exists 'genbank'@'localhost' identified by 'genbank';

grant all on genbank.* to 'genbank'@'localhost';

create table if not exists genbank.basic (
  partitionKey tinyint unsigned not null,
  locus varchar(20) not null,
  year int not null,
  month int not null,
  version int,
  giNumber int,
  definition text not null,
  primary key (partitionKey, locus),
  index year(year),
  index month(month),
  index giNumber(giNumber)
) engine InnoDB,
  character set latin1
  partition by hash(partitionKey)
  partitions 5;

create table if not exists genbank.keywords (
  partitionKey tinyint unsigned not null,
  locus varchar(20) not null,
  keyword varchar(100) not null,
  index locus(locus),
  index keyword(keyword)
) engine InnoDB,
  character set latin1
  partition by hash(partitionKey)
  partitions 5;

create table if not exists genbank.dbxrefs (
  partitionKey tinyint unsigned not null,
  locus varchar(20) not null,
  databaseName varchar(100) not null,
  databaseId varchar(100),
  index locus(locus),
  index databaseName(databaseName),
  index databaseId(databaseId)
) engine InnoDB,
  character set latin1
  partition by hash(partitionKey)
  partitions 10;

create table if not exists genbank.journals (
  partitionKey tinyint unsigned not null,
  locus varchar(20) not null,
  journal varchar(500) not null,
  citation text not null,
  pmid int,
  index locus(locus),
  index journal(journal),
  index pmid(pmid)
) engine InnoDB,
  character set latin1
  partition by hash(partitionKey)
  partitions 10;

create table if not exists genbank.authors (
  partitionKey tinyint unsigned not null,
  locus varchar(20) not null,
  author varchar(100) not null,
  index locus(locus),
  index author(author)
) engine InnoDB,
  character set latin1
  partition by hash(partitionKey)
  partitions 50;

create table if not exists genbank.annotations (
  partitionKey tinyint unsigned not null,
  locus varchar(20) not null,
  name varchar(100) not null,
  indexedValue varchar(100),
  value longtext not null,
  index locus(locus),
  index annotationName(name),
  index indexedValue(indexedValue)
) engine InnoDB,
  character set latin1
  partition by hash(partitionKey)
  partitions 50;
