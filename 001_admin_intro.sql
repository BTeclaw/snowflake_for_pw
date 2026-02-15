/*
Za pomoca roli Security Admin, tworzymy zestaw rol do naszej bazy danych.
Zrobimy prosta hierarchie:
- Admin bazy podlega pod SYSADMIN (Dobra praktyka, zapewnia jasny dostep administratorski do ról)
- Rola __WRITER bedzie sluzyla do operacji DML na obiektach - Podlega pod __ADMIN
- Rola __READER bedzie mogla jedynie odczytywac - Podlega pod __WRITER

                    SYSADMIN
                        |
                    BLAZEJ__ADMIN
                        |
                    BLAZEJ__WRITER
                        |
                    BLAZEJ__READER

*/
USE ROLE SECURITYADMIN;

CREATE ROLE BLAZEJ__ADMIN;
CREATE ROLE BLAZEJ__WRITER;
CREATE ROLE BLAZEJ__READER;


GRANT ROLE BLAZEJ__ADMIN TO ROLE SYSADMIN;
GRANT ROLE BLAZEJ__WRITER TO ROLE BLAZEJ__ADMIN;
GRANT ROLE BLAZEJ__READER TO ROLE BLAZEJ__WRITER;


/*
    Utworzmy teraz obiekty na ktorych bedziemy mogli operowac.
    - Baza danych
        - Nadamy wlasnosc na Bazie roli __ADMIN
    - Schemat w Bazie -> STAGE
    - Schemat w Bazie -> CORE

    - Warehouse rozmiaru XSmall dla roli __READER
    - Warehouse rozmiaru Small dla roli __WRITER
        - Nadamy wlasnosc na Warehouse do roli __ADMIN
*/
USE ROLE SYSADMIN;
CREATE DATABASE BLAZEJ_DB;
GRANT OWNERSHIP ON DATABASE BLAZEJ_DB TO ROLE BLAZEJ__ADMIN;

-- Tworzenie schematu za pomoca roli __ADMIN po nadaniu wlasnosci na Bazie
USE ROLE BLAZEJ__ADMIN;
CREATE SCHEMA BLAZEJ_DB.STAGE;
CREATE SCHEMA BLAZEJ_DB.CORE;

-- Stworzenie warehouse'u
USE ROLE SYSADMIN;
CREATE WAREHOUSE BLAZEJ_READER__XS_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

CREATE WAREHOUSE BLAZEJ_WRITER__S_WH
    WAREHOUSE_SIZE = SMALL
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

GRANT OWNERSHIP ON WAREHOUSE BLAZEJ_READER__XS_WH TO ROLE BLAZEJ__ADMIN;
GRANT OWNERSHIP ON WAREHOUSE BLAZEJ_WRITER__S_WH TO ROLE BLAZEJ_ADMIN;

USE ROLE BLAZEJ__ADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE BLAZEJ_READER__XS_WH TO ROLE BLAZEJ__READER;
GRANT ALL PRIVILEGES ON WAREHOUSE BLAZEJ_WRITER__S_WH TO ROLE BLAZEJ__WRITER;

/*
    Nadamy nastepnie uprawnienia dla poszczegolnych roli:
    - __READER
        - Odczyt na przyszlych tabelach i widokach w schematach STAGE i CORE
        - Odczyt na przyszlych Stage w schemacie STAGE
        - Odczyt na przyszlych External Tables w schemacie STAGE
    - __WRITER
        - Tworzenie tabel i widokow w schemacie CORE
        - Write na przyszlych tabelach w STAGE i CORE
        - Write na przyszlych Stage w schemacie STAGE
    https://docs.snowflake.com/en/user-guide/security-access-control-privileges
*/

GRANT SELECT ON FUTURE TABLES IN SCHEMA BLAZEJ_DB.STAGE TO ROLE BLAZEJ__READER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA BLAZEJ_DB.STAGE TO ROLE BLAZEJ__READER;

GRANT READ ON FUTURE STAGES IN SCHEMA BLAZEJ_DB.STAGE TO ROLE BLAZEJ__READER;

GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA BLAZEJ_DB.STAGE TO ROLE BLAZEJ__READER;


GRANT CREATE TABLE, CREATE VIEW IN SCHEMA BLAZEJ_DB.CORE TO ROLE BLAZEJ__WRITER;



    