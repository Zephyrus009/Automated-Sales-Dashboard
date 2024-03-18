DROP TABLE IF EXISTS laureate;
CREATE TABLE laureate (
    id int,firstname varchar(255),surname varchar(255),born varchar(255),died varchar(255),
    bornCountry varchar(255),bornCountryCode varchar(10),bornCity varchar(255),diedCountry varchar(255),
    diedCountryCode varchar(255),diedCity varchar(255),gender varchar(255),year int,category varchar(255),overallMotivation varchar,
    share int,motivation varchar,name varchar(255),city varchar(255),country varchar(255)
);

COPY laureate
FROM '/mnt/laureate.csv'
DELIMITER ','
CSV HEADER;

DROP TABLE IF EXISTS prize;
CREATE TABLE prize (
    year int,category varchar(255),overallMotivation varchar,id int,
    firstname varchar(255),surname varchar(255),motivation varchar,share int
);

COPY prize
FROM '/mnt/prize.csv'
DELIMITER ','
CSV HEADER;