create table if not exists zhounan_bigdata_hotwb_source_4_20190418 as
select mid, count(uid) as expo, dt from bigdata_hotwb_source_4
where dt>=20190412 and dt<=20190417
group by mid,dt;

-- drop table if exists zhounan_uid_f012_list_20190418;
-- drop table if exists zhounan_uid_f012_list_dt_20190418;
-- drop table if exists zhounan_ods_tblog_content_20190418;
-- drop table if exists zhounan_hotwb_mid_uid_tag_20190418;
-- drop table if exists zhounan_uid_field_id_name_20190418;
-- drop table if exists zhounan_post_weibo_field_id_20190418;

create table if not exists zhounan_uid_f012_list_20190418 as
select distinct uid
from cz_user_session_f01234
where dt>=20190412 and dt<=20190417
and user_level in ('F0','F1','F2');


create table if not exists zhounan_uid_f012_list_dt_20190418 as
select distinct uid,user_level,dt
from cz_user_session_f01234
where dt>=20190412 and dt<=20190417
and user_level in ('F0','F1','F2');


create table if not exists zhounan_ods_tblog_content_20190418 as
select a.mid,a.uid,a.dt
from zhounan_uid_f012_list_20190418 b
join
(
    select mid,uid,dt
    from ods_tblog_content
    where dt>=20190412 and dt<=20190417
    and cast(filter as int)&4=0
    and visible='0'
) a
on(a.uid=b.uid);


create table if not exists zhounan_hotwb_mid_uid_tag_20190418 as
select t1.mid, t1.uid, concat_ws(',',collect_set(t1.tag_id)) as tag_id
from zhounan_uid_f012_list_20190418 t2
join
(
    select mid,uid,tag_id
    from mblog_newtags_recall_history
    where dt>=20190318 and dt<=20190417
    and tag_id is not null and length(tag_id)>7
    union all
    select mid,uid,to_id as tag_id
    from new_mblog_mannual_mark_result
    where dt>=20190318 and dt<=20190417
    and to_id_type=2
    and to_id is not null and length(to_id)>7
) t1
on(t1.uid=t2.uid)
group by t1.mid,t1.uid;


create table if not exists zhounan_uid_field_id_name_20190418 as
select tt3.uid, concat_ws(',',collect_set(tt4.tag_id)) as field_id,tt3.field_name,tt5.is_f01,tt5.is_top6
from
(
    select tt1.uid,tt1.field_id,tt2.field_name
    from zhounan_uid_f012_list_20190418 tt11
    join
    (
        select distinct uid,field_id from mds_user_label_operation where dt>=20190318 and dt<=20190417
    ) tt1
    on(tt11.uid=tt1.uid)
    join
    (
        select label_id,label_name as field_name from mds_user_label_tag where label_name is not null
    ) tt2
    on (tt1.field_id=tt2.label_id)
) tt3
join
operation_field_name_mapped_newtag_id tt4
on(tt3.field_name=tt4.field_name)
join
wzd_vertical_domain_field_name tt5
on(tt3.field_name=tt5.field_name)
group by tt3.uid,tt3.field_name,tt5.is_f01,tt5.is_top6;


create table if not exists zhounan_post_weibo_field_id_20190418 as
select c.mid,c.uid, c.field_id, c.field_name, c.is_f01, d.tag_id,c.dt
from
(
    select a.mid, a.uid, b.field_id, b.field_name, a.dt, b.is_f01
    from zhounan_ods_tblog_content_20190418 a
    join zhounan_uid_field_id_name_20190418 b
    on(a.uid=b.uid)
) c
left join zhounan_hotwb_mid_uid_tag_20190418 d
on(c.mid=d.mid and c.uid=d.uid);


select a.field_name, a.dt, count(distinct if(a.is_f01=1 and b.user_level in ('F0','F1'), a.mid,if(a.is_f01!=1,a.mid, null))),
count(distinct if(a.tag_id is not null, if(a.is_f01=1 and b.user_level in ('F0','F1'), a.mid,if(a.is_f01!=1,a.mid, null)),null)),
count(distinct if(a.tag_id like '%1042015:newTagCategory%', if(a.is_f01=1 and b.user_level in ('F0','F1'), a.mid,if(a.is_f01!=1,a.mid, null)),null))
from zhounan_post_weibo_field_id_20190418 a
join zhounan_uid_f012_list_dt_20190418 b
on(a.uid = b.uid and a.dt=b.dt)
group by a.field_name,a.dt;

select a.field_name, a.dt, count(distinct if(a.is_f01=1 and c.user_level in ('F0','F1'), a.mid,if(a.is_f01!=1,a.mid, null))),
count(distinct if(b.is_high_quality=1, if(a.is_f01=1 and c.user_level in ('F0','F1'), a.mid,if(a.is_f01!=1,a.mid, null)),null))
from zhounan_post_weibo_field_id_20190418 a
join material_base_simple b
on(a.mid=b.mid and a.uid=b.uid and a.dt=b.dt)
join zhounan_uid_f012_list_dt_20190418 c
on(a.uid = c.uid and a.dt=c.dt)
group by a.field_name,a.dt;


select c.field_name, a.dt, sum(if(c.is_f01=1, if(a.user_flevel in ('F0','F1'), b.expo, 0), if(a.user_flevel in ('F0','F1', 'F2'),b.expo, 0)))
from material_base_simple a
join zhounan_bigdata_hotwb_source_4_20190418 b
on(a.mid=b.mid and a.dt=b.dt)
join zhounan_uid_field_id_name_20190418 c
on(a.uid=c.uid)
group by c.field_name,a.dt;

select c.field_name, b.dt, sum(if(c.is_f01=1, if(a.user_flevel in ('F0','F1'), b.expo, 0), if(a.user_flevel in ('F0','F1', 'F2'),b.expo, 0)))
from 
(
    select mid,uid, user_flevel from material_base_simple where dt>=20190412 and dt<=20190417 and is_high_quality=1
) a
join 
(
    select mid,expo,dt from zhounan_bigdata_hotwb_source_4_20190418 where dt=20190417
) b
on(a.mid=b.mid)
join zhounan_uid_field_id_name_20190418 c
on(a.uid=c.uid)
group by c.field_name,b.dt;

select c.field_name, sum(if(c.is_f01=1, if(a.user_flevel in ('F0','F1'), b.expo, 0), if(a.user_flevel in ('F0','F1', 'F2'),b.expo, 0)))
from 
(
    select mid,uid, user_flevel from material_base_simple where dt=20190412 and is_high_quality=1
) a
join
(
    select mid,expo from zhounan_bigdata_hotwb_source_4_20190418 where dt>=20190412 and dt<=20190417
) b
on(a.mid=b.mid)
join zhounan_uid_field_id_name_20190418 c
on(a.uid=c.uid)
group by c.field_name;

select c.field_name, sum(if(c.is_f01=1, if(a.user_flevel in ('F0','F1'), b.expo, 0), if(a.user_flevel in ('F0','F1', 'F2'),b.expo, 0)))
from 
(
    select mid,uid, user_flevel from material_base_simple where dt>=20190412 and dt<=20190417 and is_high_quality=1
) a
join
(
    select mid,expo from zhounan_bigdata_hotwb_source_4_20190418 where dt>=20190412 and dt<=20190417
) b
on(a.mid=b.mid)
join zhounan_uid_field_id_name_20190418 c
on(a.uid=c.uid)
group by c.field_name;





select d.first_tag, d.interest_tag as source,
c.cnt,c.dt
from
(
    SELECT  source, count(distinct mid) as cnt, dt
    FROM    material_base_simple
    lateral view explode(split(concat_ws(',',first_tag,second_tag,third_tag),',')) kkk AS source
    where dt>=20190412 and dt<=20190417 and source is not null and length(source)>0
    group by source,dt
) c
join material_vertical_domain_interest d
on(c.source=d.interest_tag and d.dt=20190410)
union all
select c.source as first_tag,'all' as source,
c.cnt,c.dt
from
(
    SELECT  source, count(distinct mid) as cnt, dt
    FROM    material_base_simple
    lateral view explode(split(concat_ws(',',first_tag,second_tag,third_tag),',')) kkk AS source
    where dt>=20190412 and dt<=20190417 and source is not null and length(source)>0
    group by source,dt
) c
where c.source like '1042015:newTagCategory_%';
