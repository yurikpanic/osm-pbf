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

create table way (
       id bigint primary key);

create table way_tags (
       way_id bigint,
       key_id bigint,
       val_id bigint);
create index way_tags_way_id on way_tags (way_id);
create index way_tags_key_id on way_tags(key_id);
create index way_tags_key_id_val_id on way_tags(key_id, val_id);

create table way_refs (
       way_id bigint,
       node_id bigint,
       seq serial8);
create index way_refs_way_id on way_refs (way_id);

create table relation (
       id bigint primary key);

create table relation_tags (
       relation_id bigint,
       key_id bigint,
       val_id bigint);
create index relation_tags_relation_id on relation_tags (relation_id);
create index relation_tags_key_id on relation_tags(key_id);
create index relation_tags_key_id_val_id on relation_tags(key_id, val_id);

create table relation_members (
       relation_id bigint,
       member_id bigint,
       member_type integer,
       seq serial8);
create index relation_members_relation_id on relation_members (relation_id);

create table boundary(id bigint primary key, name text, name_ru text, name_en text, admin_level integer);

create table building(id bigint primary key, name text, street text, housenumber text, is_rel boolean default false);


begin;
select relation_id into temp boundary_ids from relation_tags where relation_tags.key_id = (SELECT id from stringtable where s = 'boundary') and relation_tags.val_id = (SELECT id from stringtable where s = 'administrative');
SELECT relation_id, s into temp boundary_names from relation_tags left join stringtable on (relation_tags.val_id = stringtable.id) where relation_id in (select * from boundary_ids) and key_id = (SELECT id from stringtable where s = 'name');
SELECT relation_id, s into temp boundary_names_en from relation_tags left join stringtable on (relation_tags.val_id = stringtable.id) where relation_id in (select * from boundary_ids) and key_id = (SELECT id from stringtable where s = 'name:en');
SELECT relation_id, s into temp boundary_names_ru from relation_tags left join stringtable on (relation_tags.val_id = stringtable.id) where relation_id in (select * from boundary_ids) and key_id = (SELECT id from stringtable where s = 'name:ru');
SELECT relation_id, s into temp boundary_admin_levels from relation_tags left join stringtable on (relation_tags.val_id = stringtable.id) where relation_id in (select * from boundary_ids) and key_id = (SELECT id from stringtable where s = 'admin_level');

insert into boundary (select boundary_ids.relation_id, boundary_names.s, boundary_names_ru.s, boundary_names_en.s, boundary_admin_levels.s::integer
                             from boundary_ids
                             left join boundary_names on (boundary_ids.relation_id = boundary_names.relation_id)
                             left join boundary_names_ru on (boundary_ids.relation_id = boundary_names_ru.relation_id)
                             left join boundary_names_en on (boundary_ids.relation_id = boundary_names_en.relation_id)
                             left join boundary_admin_levels on (boundary_ids.relation_id = boundary_admin_levels.relation_id and boundary_admin_levels.s ~ '^[0-9]*$'));

drop table boundary_ids;
drop table boundary_names;
drop table boundary_names_ru;
drop table boundary_names_en;
drop table boundary_admin_levels;
end;

begin;
select relation_id into temp building_ids from relation_tags where relation_tags.key_id = (SELECT id from stringtable where s = 'building') and relation_tags.val_id = (SELECT id from stringtable where s = 'yes');
SELECT relation_id, s into temp building_names from relation_tags left join stringtable on (relation_tags.val_id = stringtable.id) where relation_id in (select * from building_ids) and key_id = (SELECT id from stringtable where s = 'name');
SELECT relation_id, s into temp building_streets from relation_tags left join stringtable on (relation_tags.val_id = stringtable.id) where relation_id in (select * from building_ids) and key_id = (SELECT id from stringtable where s = 'addr:street');
SELECT relation_id, s into temp building_housenumbers from relation_tags left join stringtable on (relation_tags.val_id = stringtable.id) where relation_id in (select * from building_ids) and key_id = (SELECT id from stringtable where s = 'addr:housenumber');

insert into building (select building_ids.relation_id, building_names.s, building_streets.s, building_housenumbers.s, true
                             from building_ids
                             left join building_names on (building_ids.relation_id = building_names.relation_id)
                             left join building_streets on (building_ids.relation_id = building_streets.relation_id)
                             left join building_housenumbers on (building_ids.relation_id = building_housenumbers.relation_id));

drop table building_ids;
drop table building_names;
drop table building_streets;
drop table building_housenumbers;
end;


begin;
select way_id into temp building_ids from way_tags where way_tags.key_id = (SELECT id from stringtable where s = 'building') and way_tags.val_id = (SELECT id from stringtable where s = 'yes');
SELECT way_id, s into temp building_names from way_tags left join stringtable on (way_tags.val_id = stringtable.id) where way_id in (select * from building_ids) and key_id = (SELECT id from stringtable where s = 'name');
SELECT way_id, s into temp building_streets from way_tags left join stringtable on (way_tags.val_id = stringtable.id) where way_id in (select * from building_ids) and key_id = (SELECT id from stringtable where s = 'addr:street');
SELECT way_id, s into temp building_housenumbers from way_tags left join stringtable on (way_tags.val_id = stringtable.id) where way_id in (select * from building_ids) and key_id = (SELECT id from stringtable where s = 'addr:housenumber');

insert into building (select building_ids.way_id, building_names.s, building_streets.s, building_housenumbers.s, false
                             from building_ids
                             left join building_names on (building_ids.way_id = building_names.way_id)
                             left join building_streets on (building_ids.way_id = building_streets.way_id)
                             left join building_housenumbers on (building_ids.way_id = building_housenumbers.way_id));

drop table building_ids;
drop table building_names;
drop table building_streets;
drop table building_housenumbers;
end;

create table way_geom (
       id bigint primary key);
select addgeometrycolumn('way_geom', 'geom', 4326, 'LINESTRING', 2);

create or replace function create_way_geom() returns integer as $proc$
declare
    wid integer;
    cnt integer;
begin
    cnt := 0;
    for wid in select id from way loop
        insert into way_geom
               (select wid as id, st_makeline(foo.point) as geom
                       from (SELECT point
                                    from way_refs
                                    left join node
                                    on (node_id = node.id)
                                    where way_id = wid
                                    order by seq)
                             as foo);
        cnt := cnt + 1;
    end loop;
    update way_geom set geom = null where geom is not null and st_npoints(geom) < 2;
    return cnt;
end
$proc$ language plpgsql;


create table boundary_poly (
    id bigint primary key);
select addgeometrycolumn('boundary_poly', 'geom', 4326, 'MULTIPOLYGON', 2);
create index boundary_poly_geom on boundary_poly using gist(geom);

create or replace function create_boundary_polies() returns integer as $proc$
declare
    bid integer;
    rel_cnt integer;
    ins_cnt integer;
begin
    ins_cnt := 0;
    for bid in select id from boundary loop
        raise info 'id %', bid;
        select into rel_cnt count(relation_id) from relation_members left join way_geom on (member_id = way_geom.id) where member_type = 1 and relation_id = bid and geom is null;
        if rel_cnt = 0 then
            insert into boundary_poly
                   (select bid, st_collect(bar.geom)
                           from (SELECT (st_dump(st_polygonize(foo.geom))).geom
                                        from (SELECT geom
                                                     from relation_members
                                                     left join way_geom
                                                     on (member_id = way_geom.id)
                                                     where relation_id = bid and member_type = 1
                                                     order by seq)
                                              as foo)
                                 as bar);
            ins_cnt := ins_cnt + 1;
        else
            raise info '% has empty ways', bid;
        end if;
    end loop;
    return ins_cnt;
end
$proc$ language plpgsql;

create table building_poly (
    id bigint primary key);
select addgeometrycolumn('building_poly', 'geom', 4326, 'MULTIPOLYGON', 2);
create index building_poly_geom on building_poly using gist(geom);

create or replace function create_building_polies() returns integer as $proc$
declare
    bid integer;
    rel_cnt integer;
    ins_cnt integer;
begin
    ins_cnt := 0;
    for bid in select id from building where is_rel loop
        raise info 'rel id %', bid;
        select into rel_cnt count(relation_id) from relation_members left join way_geom on (member_id = way_geom.id) where member_type = 1 and relation_id = bid and geom is null;
        if rel_cnt = 0 then
            insert into building_poly
                   (select bid, st_collect(bar.geom)
                           from (SELECT (st_dump(st_polygonize(foo.geom))).geom
                                        from (SELECT geom
                                                     from relation_members
                                                     left join way_geom
                                                     on (member_id = way_geom.id)
                                                     where relation_id = bid and member_type = 1
                                                     order by seq)
                                              as foo)
                                 as bar);
            ins_cnt := ins_cnt + 1;
        else
            raise info '% has empty ways', bid;
        end if;
    end loop;
    
    for bid in select building.id from building left join way_geom on (building.id = way_geom.id) where not is_rel and geom is not null loop
        raise info 'way id %', bid;
        insert into building_poly
               (select bid, st_asewkt(st_collect(foo.geom))
                       from (SELECT (st_dump(st_polygonize(geom))).geom as geom
                                    from way_geom
                                    where id = bid)
                             as foo);
    end loop;
    
    return ins_cnt;
end
$proc$ language plpgsql;
