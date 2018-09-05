CREATE KEYSPACE IF NOT EXISTS marmaray WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE marmaray;

CREATE TABLE IF NOT EXISTS devtable4 ( astr text  PRIMARY KEY ) ;
