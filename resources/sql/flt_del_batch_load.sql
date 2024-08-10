SELECT `year`,`month`,dayofmonth,CAST (CAST(CONCAT(`year`,'-',`month`,'-',dayofmonth) AS DATE) AS string) AS fltdt,dayofweek,deptime,
CAST(CONCAT(substring(deptime,1,2),"00") AS string) AS dephour, crsdeptime,arrtime,crsarrtime,uniquecarrier,flightnum,tailnum,
actualelapsedtime,crselapsedtime,airtime,arrdelay,depdelay,origin,dest,distance,taxiin,taxiout,cancelled,cancellationcode,diverted,
carrierdelay,weatherdelay,nasdelay,securitydelay,lateaircraftdelay,isarrdelayed,isdepdelayed FROM genericTempView