SELECT 
  hashed_id,
  estimated_pickup_latitude,
  estimated_pickup_longitude,
  estimated_dropoff_latitude,
  estimated_dropoff_longitude
FROM `data-cloud-production.pathao_ride.rides` 
WHERE
    city_id = 1
    AND created_at >= TIMESTAMP('2025-11-01 00:00:00+00')
    AND created_at < TIMESTAMP('2025-12-01 00:00:00+00')
    AND status = 'COMPLETED';