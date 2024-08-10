MERGE INTO iceberg.dwh_iceberg.t_nyctaxi_ic tgt
USING (
    SELECT * FROM (
        SELECT *,
            row_number() OVER (
                PARTITION BY vendor_id,pickup_time,pickup_location_id,dropoff_time,dropoff_location_id ORDER BY pickup_time
            ) AS row_num
        FROM nyctaxi.t_nyctaxi_o
	WHERE year_mon='2019_01')
    WHERE row_num = 1) stg
    ON
    tgt.vendor_id = stg.vendor_id AND
    tgt.pickup_time = stg.pickup_time AND
    tgt.pickup_location_id = stg.pickup_location_id AND
    tgt.dropoff_time = stg.dropoff_time AND
    tgt.dropoff_location_id = stg.dropoff_location_id AND
    tgt.pickup_time >= DATE '2019-01-01' AND tgt.pickup_time < DATE '2019-02-01' AND
    stg.pickup_time >= DATE '2019-01-01' AND stg.pickup_time < DATE '2019-02-01'
    WHEN NOT MATCHED THEN INSERT (
    vendor_id, pickup_time, pickup_location_id, dropoff_time, dropoff_location_id, passenger_count, trip_distance, ratecode_id,
    payment_type, total_amount, fare_amount, tip_amount, tolls_amount, mta_tax, improvement_surcharge, congestion_surcharge, airport_fee,extra_surcharges, store_and_forward_flag
    )
VALUES (
stg.vendor_id,stg.pickup_time,stg.pickup_location_id,stg.dropoff_time,stg.dropoff_location_id,stg.passenger_count,stg.trip_distance,stg.ratecode_id,
stg.payment_type,stg.total_amount,stg.fare_amount,stg.tip_amount,stg.tolls_amount,stg.mta_tax,stg.improvement_surcharge,stg.congestion_surcharge,stg.airport_fee,stg.extra_surcharges,
stg.store_and_forward_flag
)