create external table delays_raw ( 
    FL_DATE string, 
    ORIGIN string, 
    ORIGIN_CITY string,
    ORIGIN_STATE string,
    DEST string, 
    DEST_CITY string,
    DEST_STATE string, 
    DEP_DELAY float, 
    ARR_DELAY float, 
    CARRIER_DELAY float, 
    WEATHER_DELAY float, 
    NAS_DELAY float, 
    SECURITY_DELAY float, 
    LATE_AIRCRAFT_DELAY float
)
row format delimited 
fields terminated by ',' 
lines terminated by '\n' 
stored as textfile 
location '/user/data/rawflightdelaydata/'


create table delays as
    select    
		FL_DATE as flight_date,
		substring(ORIGIN, 2, length(ORIGIN) - 2) as origin_airport_code,     
		substring(ORIGIN_CITY, 2) as origin_city,    
		substring(ORIGIN_STATE, 2, length(ORIGIN_STATE) - 2)  as origin_state,     
		substring(DEST, 2, length(DEST) - 2) as dest_airport_code,     
		substring(DEST_CITY,2) as dest_city,     
		substring(DEST_STATE, 2, length(DEST_STATE) - 2)  as dest_state,     
		DEP_DELAY as dep_delay,     
		ARR_DELAY as arr_delay,     
		CARRIER_DELAY as carrier_delay,     
		WEATHER_DELAY as weather_delay,     
		NAS_DELAY as nas_delay,     
		SECURITY_DELAY as security_delay,     
		LATE_AIRCRAFT_DELAY as late_aircraft_delay 
    from delays_raw;


INSERT OVERWRITE DIRECTORY '/user/output' select 
	origin_airport_code, 
	origin_city, 
    avg(weather_delay) 
from delays 
group by origin_airport_code, origin_city;