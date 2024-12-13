MODEL (
    name sqlmesh_example.runescape_mapping,
    kind FULL,
    cron '@daily',
    grain id,
    audits (assert_positive_order_ids),
  );

  SELECT
    examine, id, members, lowalch, 'limit', 'value', highalch, icon, 'name'
  FROM 
    read_json_auto('https://prices.runescape.wiki/api/v1/osrs/mapping')
  