create table way (
       id bigint primary key);

SELECT addgeometrycolumn('way', 'geom', 4326, 'LINESTRING', 2);

create table boundary(id bigint primary key, name text, admin_level integer);

create table building(id bigint primary key, name text, street text, housenumber text, is_rel boolean default false);

create table relation_ways(rel_id bigint, way_id bigint);
CREATE INDEX relation_ways_rel_id on relation_ways(rel_id);

