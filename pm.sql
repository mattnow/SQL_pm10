--TWORZENIE BAZY DANYCH

CREATE DATABASE pm;

--TWORZENIE TABELI
CREATE TABLE pm10(
    id int not null AUTO_INCREMENT,
    data datetime not null,
    pomiar int,
    miasto varchar(15),
    PRIMARY KEY (id)
);

--MODYFIKACJA TABELI
ALTER TABLE pm10
MODIFY COLUMN pomiar float;

--WCZYTYWANIE DANYCH DO TABELI
LOAD DATA LOCAL INFILE '/tmp/pm10.csv'
INTO TABLE pm10
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(data,pomiar,miasto);



--ANALIZY

--1
CREATE VIEW srednia_miasta AS
SELECT miasto, round(avg(pomiar),2) sredni_pomiar FROM pm10
GROUP BY miasto ORDER BY 2 DESC;

SELECT data, round(avg(pomiar),2) sredni_pomiar FROM pm10
GROUP BY data;

--2
CREATE VIEW srednia_miesiac AS
SELECT case
    when month(data) = 1 then "Styczeń" 
    when month(data) = 2 then "Luty"
    when month(data) = 3 then "Marzec" end miesiac,
    round(avg(pomiar),2) sredni_pomiar FROM pm10
GROUP BY 1;

--3
CREATE VIEW minmax AS
SELECT miasto, min(pomiar) minimalne_zanieczyszczenie, max(pomiar) maksymalne_zanieczyszczenie, 
round(min(pomiar)/50*100,2) pr_normy_min, round(max(pomiar)/50*100,2) pr_normy_max FROM pm10
GROUP BY 1;

--4
CREATE VIEW ile_dni AS
SELECT miasto, count(dzienny_pomiar) ile_dni_srednio_ponad_norme FROM 
(
    SELECT miasto, date(data) dzien, round(avg(pomiar),2) dzienny_pomiar FROM pm10
    GROUP BY miasto, dzien HAVING dzienny_pomiar > 50
)x
GROUP BY miasto ORDER BY 2 DESC;

--5
CREATE VIEW ile_dni_2 AS
SELECT miesiac,miasto, count(dzienny_pomiar) ile_dni_w_normie, 
sum(count(dzienny_pomiar))over (PARTITION BY miasto rows unbounded preceding) skumulowana_suma 
FROM
(
SELECT miasto,
    case
    when month(data) = 1 then "Styczeń" 
    when month(data) = 2 then "Luty"
    when month(data) = 3 then "Marzec" end miesiac,
    date(data) dzien,
    round(avg(pomiar),2) dzienny_pomiar 
FROM pm10
GROUP BY miasto, miesiac, dzien HAVING dzienny_pomiar < 50
)x
WHERE miasto in ('Zabrze', 'Katowice', 'Warszawa')
GROUP BY miesiac, miasto;

--EKSPORTY
SELECT 'Miasto', 'Średni Pomiar'
UNION ALL
SELECT * FROM srednia_miasta
INTO OUTFILE '/var/lib/mysql-files/srednia_miasta.csv'
FIELDS ENCLOSED BY '"'
TERMINATED BY ','
LINES TERMINATED BY '\r\n';

SELECT 'Miesiąc', 'Miasto', 'Ile dni z powietrzem w normie', 'Suma skumulowana dni dla miast'
UNION ALL
SELECT * FROM ile_dni_2
INTO OUTFILE '/var/lib/mysql-files/ile_dni_2.csv'
FIELDS ENCLOSED BY '"'
TERMINATED BY ','
LINES TERMINATED BY '\r\n';
