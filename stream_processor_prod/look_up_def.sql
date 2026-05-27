DROP TABLE IF EXISTS mid_country_dictionary;
CREATE TABLE mid_country_dictionary (
    mid_code INT,
    country_name STRING,
    country_code STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://ais_postgres_lookup:5432/ais_metadata', 
    'table-name' = 'mid_country_codes',
    'username' = 'flinkuser',
    'password' = 'flinkpassword',
    'lookup.cache.max-rows' = '5000',  
    'lookup.cache.ttl' = '1 hour'
);
DROP TABLE IF EXISTS nav_status_dictionary;
CREATE TABLE nav_status_dictionary (
    status_code INT,
    status_code_description STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://ais_postgres_lookup:5432/ais_metadata', 
    'table-name' = 'nav_codes',
    'username' = 'flinkuser',
    'password' = 'flinkpassword',
    'lookup.cache.max-rows' = '100',  
    'lookup.cache.ttl' = '1 hour'
);

DROP TABLE IF EXISTS fix_type_dictionary;
CREATE TABLE fix_type_dictionary (
    fix_type_code INT,
    fix_type_description STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://ais_postgres_lookup:5432/ais_metadata', 
    'table-name' = 'fix_types',
    'username' = 'flinkuser',
    'password' = 'flinkpassword',
    'lookup.cache.max-rows' = '100',  
    'lookup.cache.ttl' = '1 hour'
);

DROP TABLE IF EXISTS ship_type_dictionary;
CREATE TABLE ship_type_dictionary (
    type_code INT,
    type_description STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://ais_postgres_lookup:5432/ais_metadata', 
    'table-name' = 'ship_types',
    'username' = 'flinkuser',
    'password' = 'flinkpassword',
    'lookup.cache.max-rows' = '100',  
    'lookup.cache.ttl' = '1 hour'
);