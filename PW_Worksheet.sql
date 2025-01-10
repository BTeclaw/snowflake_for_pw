//RESET ENV
//DROP DATABASE BLAZEJ_DEV;

USE ROLE SYSADMIN;

//Stworzenie podstawowych obiektów 
CREATE DATABASE BLAZEJ_DEV;
CREATE SCHEMA BLAZEJ_DEV.NOTEBOOKS;
CREATE SCHEMA BLAZEJ_DEV.INGEST;
CREATE WAREHOUSE BLAZEJ_WH_XS
    WAREHOUSE_SIZE = XSMALL
    WAREHOUSE_TYPE = STANDARD
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

// https://docs.snowflake.com/en/user-guide/data-load-considerations-stage
// https://docs.snowflake.com/en/sql-reference/sql/create-stage
// Snowflake umożliwia tworzenie STAGE
// STAGE dzieli się na kilka typów
// Internal Stage - Stage stanowiący Cloud Storage w pełni zarządzany przez Snowflake

CREATE OR REPLACE STAGE BLAZEJ_DEV.INGEST.MY_INTERNAL_STAGE
COMMENT = 'Przykladowy internal Stage';
LS @BLAZEJ_DEV.INGEST.MY_INTERNAL_STAGE;

//https://docs.snowflake.com/en/sql-reference/sql/copy-into-location
// Przyklad ladowania do Stage z poziomu Snowflake, wiecej o COPY INTO pozniej ...

//Mozemy rozladowac wynik kwerendy do pliku typu CSV
COPY INTO @BLAZEJ_DEV.INGEST.MY_INTERNAL_STAGE FROM
(SELECT 1 AS ID, 'test' AS MSG)
FILE_FORMAT = (TYPE = CSV);

LS @BLAZEJ_DEV.INGEST.MY_INTERNAL_STAGE;
// User Stage - Internal stage specjalnego przeznaczenia, każdy użytkownik posiada swój
// Działa podobnie jak Internal Stage - ale ma do niego dostep tylko uzytkownik.
// Nie mogą zostać usuniete ani zmienione
// Oznaczane za pomocą tyldy
LS @~;

// Table Stage - Każda tabela "pod spodem" posiada swój STAGE, uzyteczne jezeli chcemy szybko ladowac dane
CREATE OR REPLACE TABLE BLAZEJ_DEV.INGEST.TABLE_WITH_STAGE(
    ID NUMBER,
    MSG TEXT
);

// Dostęp znak '%' przed nazwą tabeli
LS @BLAZEJ_DEV.INGEST.%TABLE_WITH_STAGE;

COPY INTO @BLAZEJ_DEV.INGEST.%TABLE_WITH_STAGE FROM
(SELECT 1 AS ID, 'test' AS MSG)
FILE_FORMAT = (TYPE = CSV);

LS @BLAZEJ_DEV.INGEST.%TABLE_WITH_STAGE;


// External Stage - Stanowi "okno" do zewnetrznej chmury (AWS/GCP/Azure) by umozliwic interakcje z plikami - najczesciej spotykany (Przynajmniej u nas ;) ) w praktyce.
//  https://docs.snowflake.com/en/user-guide/data-load-local-file-system
//  https://docs.snowflake.com/en/user-guide/data-load-s3
//  https://docs.snowflake.com/en/user-guide/data-load-gcs
//  https://docs.snowflake.com/en/user-guide/data-load-azure


// https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration
// https://docs.snowflake.com/en/user-guide/data-load-s3-config
//     https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
// https://docs.snowflake.com/en/user-guide/data-load-gcs-config
//https://docs.snowflake.com/en/user-guide/data-load-azure
USE ROLE ACCOUNTADMIN;
// Wstępnie tylko ACCOUNTADMIN ma prawo do tworzenia integracji!!!
CREATE OR REPLACE STORAGE INTEGRATION EXAMPLE_AWS_INTEGRATION
    TYPE = EXTERNAL_STAGE // Jedyny dostepny typ
    ENABLED = FALSE // Nie dziala dopoki nie wykonamy komendy ALTER
    STORAGE_ALLOWED_LOCATIONS = ('s3://some-bucket') // Lista lokacji do których możemy się autoryzować integracją
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/dummy-role' // Rola AWS IAM, po naszej stronie przez którą Integracja będzie się autoryzować w AWS
    STORAGE_PROVIDER = 'S3';

DESC INTEGRATION EXAMPLE_AWS_INTEGRATION;
    

// https://docs.snowflake.com/en/user-guide/data-load-s3-create-stage
// Obiekt typu Stage, mozemy przegladac otwarte zbiory danych od Snowflake, udostepniane w ramach tutorial quickstart
CREATE STAGE BLAZEJ_DEV.INGEST.QUICKSTART_STAGE
    URL = 's3://sfquickstarts/';
LS @BLAZEJ_DEV.INGEST.QUICKSTART_STAGE;
//Obiekt typu Stage umożliwiający interakcje z AWS S3 
CREATE STAGE BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE
    URL = 's3://sfquickstarts/data-engineering-with-snowpark-python/';

//Sprawdzenie zawartośći Stage/ S3 
LIST @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE;

// Odpytywanie pliku/plików ze Stage bezpośrednio wymaga wzmianki o kolumnach i formacie (jeżeli takowy nie został wymieniony w definicji STAGE)
SELECT * FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/customer;     

//Obiekt typu FILE FORMAT pozwalajacy na czytanie plików w formacie Parquet z kompresją snappy
// https://docs.snowflake.com/en/sql-reference/sql/create-file-format
CREATE FILE FORMAT IF NOT EXISTS BLAZEJ_DEV.INGEST.SNAPPY_PARQUET
    TYPE = PARQUET
    COMPRESSION = SNAPPY;

// Odpytanie się o zawartość plików opisujących encje "customer" wraz z metadanymi
// https://docs.snowflake.com/en/user-guide/querying-stage
SELECT
    METADATA$FILENAME, // Nazwa pliku z którego pochodzi dany rekord
    METADATA$FILE_ROW_NUMBER, // numer wiersza dla danego rekordu
    METADATA$FILE_CONTENT_KEY, // Suma kontrolna dla zawartosci pliku z ktorego pochodzi rekord
    METADATA$FILE_LAST_MODIFIED, // Datetime opisujacy ostatnia date modyfikiacji pliku (typ: TIMESTAMP_NTZ)
    METADATA$START_SCAN_TIME, // Datetime operaci odczytania dla danego rekordu w pliku (typ: TIMESTAMP_LTZ)
    $1 // Zawartosc rekordu odczytanego z pliku
FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/customer
(FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET');


//Utworzenie EXTERNAL TABLE bez definicji kolumn
//Snowflake automatycznie udostepnia kolumne "VALUE" która zawiera odczytany rekord w formacie JSON
// https://docs.snowflake.com/en/sql-reference/sql/create-external-table
CREATE EXTERNAL TABLE BLAZEJ_DEV.INGEST.TB_CUSTOMERS_EXT_TAB 
LOCATION = @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/customer
FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET;

SELECT * FROM BLAZEJ_DEV.INGEST.TB_CUSTOMERS_EXT_TAB LIMIT 10;

//Utworzenie EXTERNAL TABLE z jednoczesna transformacja danych (umieszczenie wartości zagnieżdzonych z VALUE do kolumn i przerzutowanie ich do typów)
CREATE OR REPLACE EXTERNAL TABLE BLAZEJ_DEV.INGEST.TB_CUSTOMERS_EXT_TAB (
        BIRTHDAY_DATE DATE AS (value:BIRTHDAY_DATE::DATE),
        CHILDREN_COUNT TEXT AS (value:CHILDREN_COUNT::TEXT),
        CITY TEXT AS (value:CITY::TEXT),
        COUNTRY TEXT AS (value:COUNTRY::TEXT),
        CUSTOMER_ID INTEGER AS (value:CUSTOMER_ID::INTEGER),
        E_MAIL TEXT AS (value:E_MAIL::TEXT),
        FIRST_NAME TEXT AS (value:FIRST_NAME::TEXT),
        LAST_NAME TEXT AS (value:LAST_NAME::TEXT),
        MARITAL_STATUS TEXT AS (value:MARITAL_STATUS::TEXT),
        PHONE_NUMBER TEXT AS (value:PHONE_NUMBER::TEXT),
        POSTAL_CODE TEXT AS (value:POSTAL_CODE::TEXT),
        PREFERRED_LANGUAGE TEXT AS (value:PREFERRED_LANGUAGE::TEXT),
        SIGN_UP_DATE DATE AS (value:SIGN_UP_DATE::DATE)
    )
LOCATION = @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/customer
FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET;


SELECT * FROM BLAZEJ_DEV.INGEST.TB_CUSTOMERS_EXT_TAB LIMIT 10;

//Wyłączamy kolumnę VALUE ze zbioru zwracanych kolumn
SELECT * EXCLUDE VALUE FROM BLAZEJ_DEV.INGEST.TB_CUSTOMERS_EXT_TAB LIMIT 10;


//Załadowanie danych z pliku bezpośrednio do tabeli przy pomocy COPY INTO

CREATE TABLE IF NOT EXISTS BLAZEJ_DEV.INGEST.TB_CUSTOMERS (
    BIRTHDAY_DATE DATE,
    CHILDREN_COUNT TEXT,
    CITY TEXT,
    COUNTRY TEXT,
    CUSTOMER_ID INTEGER,
    E_MAIL TEXT,
    FIRST_NAME TEXT,
    LAST_NAME TEXT,
    MARITAL_STATUS TEXT,
    PHONE_NUMBER TEXT,
    POSTAL_CODE TEXT,
    PREFERRED_LANGUAGE TEXT,
    SIGN_UP_DATE DATE
);
SELECT * FROM BLAZEJ_DEV.INGEST.TB_CUSTOMERS LIMIT 10;

// Zaladowanie danych z pliku do tabeli
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
COPY INTO BLAZEJ_DEV.INGEST.TB_CUSTOMERS FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/customer
    FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE; // Automatycznie dopasuje kolumny istniejace w pliku parquet do kolumn w tabeli w Snowflake (bez wzgledu na wielkosc liter)
    //UWAGA! Wymaga utowrzenia tabeli w ktorej nazwy kolumn są odwzorowane 1:1 do pliku źródłowego

SELECT * FROM BLAZEJ_DEV.INGEST.TB_CUSTOMERS LIMIT 10;
SELECT COUNT(*) FROM BLAZEJ_DEV.INGEST.TB_CUSTOMERS;

//Próba załadowania drugi raz tego samego pliku
COPY INTO BLAZEJ_DEV.INGEST.TB_CUSTOMERS FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/customer
    FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
// Źródło: https://docs.snowflake.com/en/user-guide/data-load-considerations-load
// Copy Into nie załadowuje ponownie plików
// Copy Into załadowuje do metadanych tabeli informacje o stanie plików które zostały załadowane
// Metadane odnośnie konkretnych plików wygasają po 64 dniach
// Wygaśnięcie opiera się o kolumnę LAST_MODIFIED z metadanych stage'owanych plików
// LAST_MODIFIED oznacz kiedy plik został zastageowany, lub zmodyfikowany, cokolwiek jest późniejsze
// Po czasie 64 dni, , jest oznaczony jako plik niepewny i będzie pomijany w loadzie
// Używając flagi LOAD_UNCERTAIN_FILES na true możemy spowodować załadwoanie takich plików
// Jezeli chcemy załadować pliki pomimo tego, że były już załadowane możemy użyć flagi FORCE

SELECT * FROM BLAZEJ_DEV.INGEST.TB_CUSTOMERS LIMIT 10;
SELECT COUNT(*) FROM BLAZEJ_DEV.INGEST.TB_CUSTOMERS;

COPY INTO BLAZEJ_DEV.INGEST.TB_CUSTOMERS FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/customer
    FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE;

SELECT COUNT(*) FROM BLAZEJ_DEV.INGEST.TB_CUSTOMERS;

//Liczba wpisów została podwojona
//TRUNCATE usuwa metadana tabeli, dotyczace loadu, więc możemy wykorzystać COPY INTO żeby ponowic Initial Load
TRUNCATE TABLE BLAZEJ_DEV.INGEST.TB_CUSTOMERS;
COPY INTO BLAZEJ_DEV.INGEST.TB_CUSTOMERS FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/customer
    FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


//Przykład INFER SCHEMA
// https://docs.snowflake.com/en/sql-reference/functions/infer_schema
//Infer schema to funkcja która zwraca definicje kolumn odczytane ze zbioru plików
//Może zostać wykorzystana w przypadku gdy chcemy zdefiniować table docelową dla plików,
//których schemat nie do końca znamy

//Przykład dla customers
SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/customer',
        FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET'
    )
);

//Przykład dla Trucks
SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/truck',
        FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET'
    )
);


//Tworzenie tabeli za pomocą infered schema
//Wymaga podania subquery, posiadającego wartośc zwracaną z INFER_SCHEMA jako array lub jako STRING

//Ponizsza kwerenda najpierw tworzy nam z kazdego wiersza atrybuty typu OBJECT (JSON) przy pomocy funkcji OBJECT_CONSTRUCT(*)
//Nastepnie agregowane jest to do zmiennej typu ARRAY za pomocą funkcji ARRAY_AGG
SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/truck',
        FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET'
    )
);

CREATE OR REPLACE TABLE BLAZEJ_DEV.INGEST.TB_TRUCKS
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/truck',
                FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET'
            )
        )
    );
DESC TABLE BLAZEJ_DEV.INGEST.TB_TRUCKS;

COPY INTO BLAZEJ_DEV.INGEST.TB_TRUCKS FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/truck
    FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
SELECT COUNT(*) FROM BLAZEJ_DEV.INGEST.TB_TRUCKS;

SELECT * FROM BLAZEJ_DEV.INGEST.TB_TRUCKS LIMIT 10;

//Utworeznie reszty tabeli dla danych FrostByte 
//Country
CREATE OR REPLACE TABLE BLAZEJ_DEV.INGEST.TB_COUNTRY
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/country',
                FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET'
            )
        )
    );

COPY INTO BLAZEJ_DEV.INGEST.TB_COUNTRY FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/country
    FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;    

//Location
CREATE OR REPLACE TABLE BLAZEJ_DEV.INGEST.TB_LOCATION
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/location',
                FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET'
            )
        )
    );

COPY INTO BLAZEJ_DEV.INGEST.TB_LOCATION FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/location
    FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

//Menu
CREATE OR REPLACE TABLE BLAZEJ_DEV.INGEST.TB_MENU
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/menu',
            FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET'
        )
    )
);

COPY INTO BLAZEJ_DEV.INGEST.TB_MENU FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/menu
    FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;    

//Franchise
CREATE OR REPLACE TABLE BLAZEJ_DEV.INGEST.TB_FRANCHISE
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/franchise',
            FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET'
        )
    )
);

COPY INTO BLAZEJ_DEV.INGEST.TB_FRANCHISE FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/franchise
    FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;    


// Przyklad z danymi spartycjowanymi po dacie
//Order header
//Zwykłe INFER_SCHEMA bez podawania wielkości próbki
SELECT *
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/order_header',
            FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET'
        )
    );

//Infer schema z ustawionym limitem plików z których schemat tabeli będzie odczytywany
SELECT *
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/order_header',
            FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET',
            MAX_FILE_COUNT => 5
        )
    );

//Podanie listy plików do odczytania schematu tabeli
SELECT *
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/order_header',
            FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET',
            FILES => ('year=2019/data_01a91b48-0605-6a9c-0000-711101079122_007_0_0.snappy.parquet', 'year=2020/data_01a91b48-0605-6a9c-0000-711101079122_207_4_1.snappy.parquet')
        )
    );

CREATE OR REPLACE TABLE BLAZEJ_DEV.INGEST.TB_ORDER_HEADER
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/order_header',
            FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET',
            FILES => ('year=2019/data_01a91b48-0605-6a9c-0000-711101079122_007_0_0.snappy.parquet', 'year=2020/data_01a91b48-0605-6a9c-0000-711101079122_207_4_1.snappy.parquet')
        )
    )
);

COPY INTO BLAZEJ_DEV.INGEST.TB_ORDER_HEADER FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/order_header
    FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

//Order detail

CREATE OR REPLACE TABLE BLAZEJ_DEV.INGEST.TB_ORDER_DETAIL
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/order_detail',
            FILE_FORMAT => 'BLAZEJ_DEV.INGEST.SNAPPY_PARQUET',
            MAX_FILE_COUNT => 5
        )
    )
);

COPY INTO BLAZEJ_DEV.INGEST.TB_ORDER_DETAIL FROM @BLAZEJ_DEV.INGEST.TASTYBYTES_RAW_STAGE/pos/order_detail
    FILE_FORMAT = BLAZEJ_DEV.INGEST.SNAPPY_PARQUET
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


// Kolumna w TB_MENU oryginalnie jest typu OBJECT, INFER_SCHEMA okreslil go jako TEXT/VARCHAR/STRING
SELECT * FROM BLAZEJ_DEV.INGEST.TB_MENU LIMIT 10;
SELECT MENU_ITEM_HEALTH_METRICS_OBJ FROM BLAZEJ_DEV.INGEST.TB_MENU LIMIT 10;
DESC TABLE BLAZEJ_DEV.INGEST.TB_MENU;

SELECT PARSE_JSON(MENU_ITEM_HEALTH_METRICS_OBJ) FROM BLAZEJ_DEV.INGEST.TB_MENU LIMIT 10;
//Bezpieczna wersja to TRY_PARSE_JSON

// Przyklad eksplodowania JSON za pomocą funkcji FLATTEN
CREATE OR REPLACE TABLE HEALTH_METRIX_EXPLODED 
AS
SELECT SEQ, KEY, PATH, INDEX, VALUE, THIS 
FROM BLAZEJ_DEV.INGEST.TB_MENU menu,
TABLE(FLATTEN(
    INPUT => PARSE_JSON(menu.MENU_ITEM_HEALTH_METRICS_OBJ)
));

SELECT * FROM BLAZEJ_DEV.INGEST.HEALTH_METRIX_EXPLODED LIMIT 10;

// Przykład lineage'u

CREATE SCHEMA BLAZEJ_DEV.TRANSFORMED;
CREATE OR REPLACE TABLE BLAZEJ_DEV.TRANSFORMED.TB_CUSTOMER_AGE (CUSTOMER_ID NUMBER, AGE NUMBER)
AS
SELECT CUSTOMER_ID, POSTAL_CODE, DATEDIFF('year', CURRENT_DATE(), BIRTHDAY_DATE) AS AGE
FROM BLAZEJ_DEV.INGEST.TB_CUSTOMERS;

CREATE OR REPLACE TABLE BLAZEJ_DEV.TRANSFORMED.TB_AVG_AGE_PER_CODE ( POSTAL_CODE TEXT, AVG_AGE NUMBER)
AS
SELECT POSTAL_CODE, AVG(AGE) AS AVG_AGE FROM BLAZEJ_DEV.TRANSFORMED.TB_CUSTOMER_AGE cust_age
INNER JOIN BLAZEJ_DEV.INGEST.TB_CUSTOMERS cust ON cust_age.customer_id = cust.customer_id
GROUP BY POSTAL_CODE;

