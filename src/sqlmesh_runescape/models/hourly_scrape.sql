MODEL (
    name sqlmesh_example.hourly_scrape,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column ts
    ),
    -- kind FULL,
    start '2024-12-12',
    cron '@hourly',
    grain (item_id, ts),
    description 'Scrapes hourly price data from RuneScape API using UTC time as an integer.'
);

-- {% set url = "https://prices.runescape.wiki/api/v1/osrs/1h?timestamp=" %}
  
SELECT
    ts,
    key::INT AS id,
    json_extract(json_data, CONCAT('$.', key, '.avgHighPrice'))::INTEGER AS avg_high_price,
    json_extract(json_data, CONCAT('$.', key, '.highPriceVolume'))::INTEGER AS high_price_volume,
    json_extract(json_data, CONCAT('$.', key, '.avgLowPrice'))::INTEGER AS avg_low_price,
    json_extract(json_data, CONCAT('$.', key, '.lowPriceVolume'))::INTEGER AS low_price_volume
FROM (
    SELECT to_timestamp("timestamp") as ts, data::JSON AS json_data, 
           UNNEST(json_keys(data::JSON)) AS key
    FROM read_json_auto('https://prices.runescape.wiki/api/v1/osrs/1h?timestamp=' || epoch(TIMESTAMP '{{ start_ds }}'))
);