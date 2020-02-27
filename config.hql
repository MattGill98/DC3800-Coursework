DROP TABLE IF EXISTS temp_cities;
CREATE TABLE temp_cities
(city string, city_ascii string, lat string, lng string, country string, iso2 string, iso3 string, admin_name string, capital string, population string, id string)
row format delimited fields terminated by ',' lines terminated by '\n'
stored as textfile
tblproperties ("skip.header.line.count"="1");

load data local inpath "worldcities.csv" overwrite into table temp_cities;

DROP TABLE cities;
CREATE TABLE cities
(city string, lat decimal(9,6), lng decimal(9,6), country string)
row format delimited fields terminated by ',' lines terminated by '\n'
stored as textfile;

INSERT INTO TABLE cities
SELECT city_ascii, lat, lng, country
FROM temp_cities;
DROP TABLE temp_cities;



DROP TABLE IF EXISTS temp_data;
CREATE TABLE temp_data
(station string, time string, report string, source string, AWND string, BackupDirection string, BackupDistance string, BackupDistanceUnit string, BackupElements string, BackupElevation string, BackupElevationUnit string, BackupEquipment string, BackupLatitude string, BackupLongitude string, BackupName string, CDSD string, CLDD string, DSNW string, DailyAverageDewPointTemperature string, DailyAverageDryBulbTemperature string, DailyAverageRelativeHumidity string, DailyAverageSeaLevelPressure string, DailyAverageStationPressure string, DailyAverageWetBulbTemperature string, DailyAverageWindSpeed string, DailyCoolingDegreeDays string, DailyDepartureFromNormalAverageTemperature string, DailyHeatingDegreeDays string, DailyMaximumDryBulbTemperature string, DailyMinimumDryBulbTemperature string, DailyPeakWindDirection string, DailyPeakWindSpeed string, DailyPrecipitation string, DailySnowDepth string, DailySnowfall string, DailySustainedWindDirection string, DailySustainedWindSpeed string, DailyWeather string, HDSD string, HTDD string, HeavyFog string, HourlyAltimeterSetting string, HourlyDewPointTemperature string, HourlyDryBulbTemperature string, HourlyPrecipitation string, HourlyPresentWeatherType string, HourlyPressureChange string, HourlyPressureTendency string, HourlyRelativeHumidity string, HourlySeaLevelPressure string, HourlySkyConditions string, HourlyStationPressure string, HourlyVisibility string, HourlyWetBulbTemperature string, HourlyWindDirection string, HourlyWindGustSpeed string, HourlyWindSpeed string, MonthlyAverageRH string, MonthlyDaysWithGT001Precip string, MonthlyDaysWithGT010Precip string, MonthlyDaysWithGT32Temp string, MonthlyDaysWithGT90Temp string, MonthlyDaysWithLT0Temp string, MonthlyDaysWithLT32Temp string, MonthlyDepartureFromNormalAverageTemperature string, MonthlyDepartureFromNormalCoolingDegreeDays string, MonthlyDepartureFromNormalHeatingDegreeDays string, MonthlyDepartureFromNormalMaximumTemperature string, MonthlyDepartureFromNormalMinimumTemperature string, MonthlyDepartureFromNormalPrecipitation string, MonthlyDewpointTemperature string, MonthlyGreatestPrecip string, MonthlyGreatestPrecipDate string, MonthlyGreatestSnowDepth string, MonthlyGreatestSnowDepthDate string, MonthlyGreatestSnowfall string, MonthlyGreatestSnowfallDate string, MonthlyMaxSeaLevelPressureValue string, MonthlyMaxSeaLevelPressureValueDate string, MonthlyMaxSeaLevelPressureValueTime string, MonthlyMaximumTemperature string, MonthlyMeanTemperature string, MonthlyMinSeaLevelPressureValue string, MonthlyMinSeaLevelPressureValueDate string, MonthlyMinSeaLevelPressureValueTime string, MonthlyMinimumTemperature string, MonthlySeaLevelPressure string, MonthlyStationPressure string, MonthlyTotalLiquidPrecipitation string, MonthlyTotalSnowfall string, MonthlyWetBulb string, NormalsCoolingDegreeDay string, NormalsHeatingDegreeDay string, REM string, REPORT_TYPE string, SOURCE2 string, ShortDurationEndDate005 string, ShortDurationEndDate010 string, ShortDurationEndDate015 string, ShortDurationEndDate020 string, ShortDurationEndDate030 string, ShortDurationEndDate045 string, ShortDurationEndDate060 string, ShortDurationEndDate080 string, ShortDurationEndDate100 string, ShortDurationEndDate120 string, ShortDurationEndDate150 string, ShortDurationEndDate180 string, ShortDurationPrecipitationValue005 string, ShortDurationPrecipitationValue010 string, ShortDurationPrecipitationValue015 string, ShortDurationPrecipitationValue020 string, ShortDurationPrecipitationValue030 string, ShortDurationPrecipitationValue045 string, ShortDurationPrecipitationValue060 string, ShortDurationPrecipitationValue080 string, ShortDurationPrecipitationValue100 string, ShortDurationPrecipitationValue120 string, ShortDurationPrecipitationValue150 string, ShortDurationPrecipitationValue180 string, Sunrise string, Sunset string, TStorms string, WindEquipmentChangeDate string)
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
tblproperties ("skip.header.line.count"="1");

load data local inpath "climatedata.csv" overwrite into table temp_data;

DROP TABLE IF EXISTS data;
CREATE TABLE data
(station string, time timestamp, hourlypresentweathertype string, hourlydrybulbtemperature int, hourlysealevelpressure decimal(5,3), hourlyrelativehumidity smallint, hourlywinddirection smallint, hourlywindspeed smallint)
row format delimited fields terminated by ',' lines terminated by '\n'
stored as textfile;

INSERT INTO TABLE data
SELECT substr(station, length(station) - 4), from_unixtime(unix_timestamp(time, "yyyy-MM-dd'T'HH:mm:ss")), HourlyPresentWeatherType, HourlyDryBulbTemperature, HourlySeaLevelPressure, HourlyRelativeHumidity, HourlyWindDirection, HourlyWindSpeed
FROM temp_data WHERE report != "SOD  ";
DROP TABLE temp_data;

CREATE TABLE temp_stations
(WBAN string, WMO string, callsign string, climatedivisioncode tinyint, ClimateDivisionStateCode tinyint, ClimateDivisionStationCode tinyint, name String, state String, location string, latitude decimal(9,6), longitude decimal(9,6), groundheight decimal(9,6), stationheight int, barometer int, timezone string)
row format delimited fields terminated by '|' lines terminated by '\n'
stored as textfile
tblproperties ("skip.header.line.count"="1");

load data local inpath "stations.txt" overwrite into table temp_stations;


DROP TABLE IF EXISTS stations;
CREATE TABLE stations
(WBAN string, name String, state String, location string, latitude decimal(9,6), longitude decimal(9,6), groundheight decimal(5,1))
row format delimited fields terminated by '|' lines terminated by '\n'
stored as textfile;

INSERT INTO TABLE stations
SELECT WBAN, name, state, location, latitude, longitude, groundheight
FROM temp_stations;

DROP table temp_stations;
