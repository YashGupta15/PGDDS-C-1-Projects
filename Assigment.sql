
##---------------------------------Creating Schema "assignment" and importing csv files--------------------------
# Dropping the tables if already exist.
drop table if exists bajaj_auto,TCS,TVS_Motors,Eicher_motors,Hero_Motocorps,Infosys;

# Creating the table for loading the data
create table Bajaj_Auto(
	Market_date varchar(50),
    Open_Price float(10,2),
    High_Price float(10,2),
    Low_Price float(10,2),
    Close_Price float(10,2),
    VWAP_Price varchar(50),
    Num_Shares float(10,2),
    Num_Trades float(10,2),
    Total_Turnover varchar(50),
    Delivered_Quantity varchar(50),
    Pert_Delivered varchar(50),
    Spread_High_Low float(10,2),
    spread_Close_Open float(10,2)
);

# Replicating the same structure to rest of the tables
create table Hero_Motocorps like Bajaj_Auto;
create table Infosys like Bajaj_Auto;
create table TCS like Bajaj_Auto;
create table TVS_Motors like Bajaj_Auto;
create table Eicher_motors like Bajaj_Auto;

# Importing the data for all the tables

#Loading Bajaj Auto data
LOAD DATA INFILE 'C:\Assignment\Bajaj Auto.csv' INTO TABLE  Bajaj_Auto
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
(Market_date,Open_Price,High_Price,Low_Price,Close_Price,VWAP_Price,Num_Shares,Num_Trades,Total_Turnover,@Delivered_Quantity,@Pert_Delivered,Spread_High_Low,spread_Close_Open)
SET Delivered_Quantity=nullif(@Delivered_Quantity,''),
	Pert_Delivered=nullif(@Pert_Delivered,''),
Market_date=STR_TO_DATE(Market_date,'%d-%M-%Y');

#Loading Hero Motocorp data 
LOAD DATA INFILE 'C:\Assignment\Hero Motocorp.csv' INTO TABLE  Hero_Motocorps
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(Market_date,Open_Price,High_Price,Low_Price,Close_Price,VWAP_Price,Num_Shares,Num_Trades,Total_Turnover,@Delivered_Quantity,@Pert_Delivered,Spread_High_Low,spread_Close_Open)
SET Delivered_Quantity=nullif(@Delivered_Quantity,''),
	Pert_Delivered=nullif(@Pert_Delivered,''),
Market_date=STR_TO_DATE(Market_date,'%d-%M-%Y');

#Loading Infosys data
LOAD DATA INFILE 'C:\Assignment\Infosys.csv' INTO TABLE  Infosys
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(Market_date,Open_Price,High_Price,Low_Price,Close_Price,VWAP_Price,Num_Shares,Num_Trades,Total_Turnover,@Delivered_Quantity,@Pert_Delivered,Spread_High_Low,spread_Close_Open)
SET Delivered_Quantity=nullif(@Delivered_Quantity,''),
	Pert_Delivered=nullif(@Pert_Delivered,''),
Market_date=STR_TO_DATE(Market_date,'%d-%M-%Y');


#Loading TCS data
LOAD DATA INFILE 'C:\Assignment\TCS.csv' INTO TABLE  TCS
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(Market_date,Open_Price,High_Price,Low_Price,Close_Price,VWAP_Price,Num_Shares,Num_Trades,Total_Turnover,@Delivered_Quantity,@Pert_Delivered,Spread_High_Low,spread_Close_Open)
SET Delivered_Quantity=nullif(@Delivered_Quantity,''),
	Pert_Delivered=nullif(@Pert_Delivered,''),
Market_date=STR_TO_DATE(Market_date,'%d-%M-%Y');


#Loading TVS Motors data
LOAD DATA INFILE 'C:\Assignment\TVS Motors.csv' INTO TABLE  TVS_Motors
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(Market_date,Open_Price,High_Price,Low_Price,Close_Price,VWAP_Price,Num_Shares,Num_Trades,Total_Turnover,@Delivered_Quantity,@Pert_Delivered,Spread_High_Low,spread_Close_Open)
SET Delivered_Quantity=nullif(@Delivered_Quantity,''),
	Pert_Delivered=nullif(@Pert_Delivered,''),
Market_date=STR_TO_DATE(Market_date,'%d-%M-%Y');

#Loading Eicher Motors data
LOAD DATA INFILE 'C:\Assignment\Eicher Motors.csv' INTO TABLE Eicher_motors
FIELDS TERMINATED BY ','
LINES TERMINATED BY  '\r\n'
IGNORE 1 LINES
(Market_date,Open_Price,High_Price,Low_Price,Close_Price,VWAP_Price,Num_Shares,Num_Trades,Total_Turnover,@Delivered_Quantity,@Pert_Delivered,Spread_High_Low,spread_Close_Open)
SET Delivered_Quantity=nullif(@Delivered_Quantity,''),
	Pert_Delivered=nullif(@Pert_Delivered,''),
Market_date=STR_TO_DATE(Market_date,'%d-%M-%Y');

#Dropping the tables if already existing
DROP TABLE IF EXISTS bajaj1,Eicher1,Hero1,Infosys1,TCS1,TVS1;


#Creating the table structure for Moving Average loading data
create table bajaj1(
	`Date` date,
    `Close Price` float(10,2),
    `20 Day MA` float(10,2),
    `50 Day MA` float(10,2)
);

#Replicating the same structure to remaining tables
create table Eicher1 like bajaj1;
create table Hero1 like bajaj1;
create table Infosys1 like bajaj1;
create table TCS1 like bajaj1;
create table TVS1 like bajaj1;

#-------------------------------------R1.Creating "bajaj1" table for all 6 stocks-------------------------------------------------------------
# Creating Bajaj1 Moving Average data from Bajaj Auto data
INSERT INTO bajaj1 (`Date`,`Close Price`,`20 Day MA`,`50 Day MA`)
(
SELECT
Market_date, 
Close_Price,
avg(Close_Price) over (order by Market_date rows 19 preceding) ,
avg(Close_Price) over (order by Market_date rows 49 preceding)  
FROM Bajaj_Auto
);

# Creating Hero1 Moving Average data from Hero motocorp Auto data
INSERT INTO Hero1 (`Date`,`Close Price`,`20 Day MA`,`50 Day MA`)
(
SELECT
Market_date, 
Close_Price,
avg(Close_Price) over (order by Market_date rows 19 preceding) ,
avg(Close_Price) over (order by Market_date rows 49 preceding)
FROM Hero_Motocorps
);


# Creating Eicher1 Moving Average data from Eicher motors data
INSERT INTO Eicher1 (`Date`,`Close Price`,`20 Day MA`,`50 Day MA`)(
SELECT 
Market_date, 
Close_Price,
avg(Close_Price) over (order by Market_date rows 19 preceding) ,
avg(Close_Price) over (order by Market_date rows 49 preceding)
FROM Eicher_motors
);


# Creating Infosys1 Moving Average data from Infosys data
INSERT INTO Infosys1 (`Date`,`Close Price`,`20 Day MA`,`50 Day MA`)(
SELECT
Market_date, 
Close_Price,
avg(Close_Price) over (order by Market_date rows 19 preceding) ,
avg(Close_Price) over (order by Market_date rows 49 preceding)
FROM Infosys
);


# Creating TCS1 Moving Average data from TCS data
INSERT INTO TCS1 (`Date`,`Close Price`,`20 Day MA`,`50 Day MA`)(
SELECT 
Market_date, 
Close_Price,
avg(Close_Price) over (order by Market_date rows 19 preceding) ,
avg(Close_Price) over (order by Market_date rows 49 preceding)
FROM TCS
);


# Creating TVS1 Moving Average data from TVS Motors data
INSERT INTO TVS1 (`Date`,`Close Price`,`20 Day MA`,`50 Day MA`)
(
SELECT 
Market_date, 
Close_Price,
avg(Close_Price) over (order by Market_date rows 19 preceding) ,
avg(Close_Price) over (order by Market_date rows 49 preceding)
FROM TVS_Motors
);

##----------------------------- R2.Creating Master Table for all 6 stocks--------------------------------------

#Dropping the Master table if already existing
DROP TABLE IF EXISTS Master_Table;

# Creating Master table
create table Master_Table(
	Date date,
    Bajaj float(10,2),
    TCS float(10,2),
    TVS float(10,2),
    Infosys float(10,2),
    Eicher float(10,2),
    Hero float(10,2)
);


#Inserting into MasterTable of different stocks with all the close prices
INSERT INTO Master_Table(
With Master_Table_Temp as(
		select B.Market_date as Date ,
        B.Close_Price as Bajaj ,
        T.Close_Price as TCS,
		TV.Close_Price as TVS,
		I.Close_Price as Infosys,
		E.Close_Price as Eicher,
		H.Close_Price as Hero
        from bajaj_auto  B
        inner join TCS  T on T.Market_date=B.Market_date
        inner join TVS_Motors  TV on TV.Market_date=B.Market_date
        inner join Infosys  I on I.Market_date=B.Market_date
        inner join Eicher_motors  E on E.Market_date=B.Market_date
        inner join Hero_Motocorps  H on H.Market_date=B.Market_date
        )
        select * from Master_Table_temp
        );

##--------------------------R3.Creating table for generating buy and sell signal for all 6 stocks---------------------------------------------------------
#Dropping the tables if already existing
DROP TABLE IF EXISTS bajaj2,Eicher2,Hero2,Infosys2,TCS2,TVS2;

#Creating the table structure for SIGNAL loading data
create table bajaj2(
	`Date` date,
    `Close Price` float(10,2),
    `Signal` varchar(50)
);

#Replicating the same structure to remaining tables
create table Eicher2 like bajaj2;
create table Hero2 like bajaj2;
create table Infosys2 like bajaj2;
create table TCS2 like bajaj2;
create table TVS2 like bajaj2;

# Creating BAJAJ2 table with the Signal using temporary table and LAG function
INSERT INTO bajaj2(`Date`,`Close Price`,`Signal`)
(
With temp AS
(
select *,
ROW_NUMBER() OVER (ORDER BY Date) as RowNumber,
CASE
        WHEN `20 Day MA`>`50 Day MA` and LAG(`20 Day MA`) over() < LAG(`50 Day MA`) over()  THEN 'BUY'
        WHEN `20 Day MA`<`50 Day MA` and LAG(`20 Day MA`) over() > LAG(`50 Day MA`) over()  THEN 'SELL'
		ELSE 'HOLD'
END as `Signal`
from bajaj1
)
select `Date`,`Close Price`,`Signal`
from temp where Rownumber>49
);


# Creating EICHER2 table with the Signal using temporary table and LAG function
INSERT INTO Eicher2(`Date`,`Close Price`,`Signal`)
(
With temp AS
(
select *,
ROW_NUMBER() OVER (ORDER BY Date) as RowNumber,
CASE
        WHEN `20 Day MA`>`50 Day MA` and LAG(`20 Day MA`) over() < LAG(`50 Day MA`) over()  THEN 'BUY'
        WHEN `20 Day MA`<`50 Day MA` and LAG(`20 Day MA`) over() > LAG(`50 Day MA`) over()  THEN 'SELL'
		ELSE 'HOLD'
END as `Signal`
from Eicher1
)
select `Date`,`Close Price`,`Signal`
from temp where RowNumber>49 
);



# Creating Hero2 table with the Signal using temporary table and LAG function
INSERT INTO Hero2(`Date`,`Close Price`,`Signal`)
(
With temp AS
(
select *,
ROW_NUMBER() OVER (ORDER BY Date) as RowNumber,
CASE
        WHEN `20 Day MA`>`50 Day MA` and LAG(`20 Day MA`) over() < LAG(`50 Day MA`) over()  THEN 'BUY'
        WHEN `20 Day MA`<`50 Day MA` and LAG(`20 Day MA`) over() > LAG(`50 Day MA`) over()  THEN 'SELL'
		ELSE 'HOLD'
END as `Signal`
from Hero1
)
select `Date`,`Close Price`,`Signal`
from temp where RowNumber>49 
);



# Creating Infosys table with the Signal using temporary table and LAG function
INSERT INTO Infosys2(`Date`,`Close Price`,`Signal`)
(
With temp AS
(
select *,
ROW_NUMBER() OVER (ORDER BY Date) as RowNumber,
CASE
        WHEN `20 Day MA`>`50 Day MA` and LAG(`20 Day MA`) over() < LAG(`50 Day MA`) over()  THEN 'BUY'
        WHEN `20 Day MA`<`50 Day MA` and LAG(`20 Day MA`) over() > LAG(`50 Day MA`) over()  THEN 'SELL'
		ELSE 'HOLD'
END as `Signal`
from Infosys1
)
select `Date`,`Close Price`,`Signal`
from temp where RowNumber>49 
);



# Creating TCS2 table with the Signal using temporary table and LAG function
INSERT INTO TCS2(`Date`,`Close Price`,`Signal`)
(
With temp AS
(
select *,
ROW_NUMBER() OVER (ORDER BY Date) as RowNumber,
CASE
        WHEN `20 Day MA`>`50 Day MA` and LAG(`20 Day MA`) over() < LAG(`50 Day MA`) over()  THEN 'BUY'
        WHEN `20 Day MA`<`50 Day MA` and LAG(`20 Day MA`) over() > LAG(`50 Day MA`) over()  THEN 'SELL'
		ELSE 'HOLD'
END as `Signal`
from TCS1
)
select `Date`,`Close Price`,`Signal`
from temp where RowNumber>49 
);



# Creating TVS2 table with the Signal using temporary table and LAG function
INSERT INTO TVS2(`Date`,`Close Price`,`Signal`)
(
With temp AS
(
select *,
ROW_NUMBER() OVER (ORDER BY Date) as RowNumber,
CASE
        WHEN `20 Day MA`>`50 Day MA` and LAG(`20 Day MA`) over() < LAG(`50 Day MA`) over()  THEN 'BUY'
        WHEN `20 Day MA`<`50 Day MA` and LAG(`20 Day MA`) over() > LAG(`50 Day MA`) over()  THEN 'SELL'
		ELSE 'HOLD'
END as `Signal`
from TVS1
)
select `Date`,`Close Price`,`Signal`
from temp where RowNumber>49 
);


##--------------------------------------- R4. User Define Function for bajaj stock signal------------------------------------------------------------
#Dropping function if already exists
DROP FUNCTION IF EXISTS Bajaj_Stock;

# Creating function for Baja Stock
Create function Bajaj_Stock_signal(D date)
returns varchar(10) deterministic
return (select `Signal` from bajaj2 where Date=D);

# Testing the function
SELECT Bajaj_Stock_signal('2015-08-24') AS Signal1;

