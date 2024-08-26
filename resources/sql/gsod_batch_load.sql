SELECT station,CAST(rep_date as date) AS rep_date,CAST(latitude AS double) AS latitude,CAST(longitude AS double) AS longitude,name,CAST(temp AS double) AS temp
FROM genericTempTable