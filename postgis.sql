create table way (
       id bigint primary key);

SELECT addgeometrycolumn('way', 'geom', 4326, 'LINESTRING', 2);

create table boundary(id bigint primary key, name text, admin_level integer);

create table building(id bigint primary key, name text, street text, housenumber text, is_rel boolean default false);

create table relation_ways(rel_id bigint, way_id bigint);
CREATE INDEX relation_ways_rel_id on relation_ways(rel_id);

create sequence st_seq;
create table stringtable (
       id bigint not null default nextval('st_seq'),
       s text,
       primary key (id));
CREATE INDEX stringtable_s on stringtable (s);

create table node (
       id bigint primary key);
select addgeometrycolumn('node', 'point', 4326, 'POINT', 2);

create table node_tags (
       node_id bigint,
       key_id bigint,
       val_id bigint);
create index node_tags_node_id on node_tags (node_id);
