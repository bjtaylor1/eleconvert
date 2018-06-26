
alter table roads add id int identity(1,1) primary key clustered

alter table roads add elevation bigint

alter table roads add batch int
update roads set batch = id/1000 
alter table roads alter column batch int null
create index idx_batch on roads(batch)

select batch, count(*) from roads group by batch

select distinct id/1000 from roads

select * from roads where elevation is not null

