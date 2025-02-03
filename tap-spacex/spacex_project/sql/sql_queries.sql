/*selecting data*/
select * from dbt_spacex.launches;

---
/* Filtering rows */
select *
from dbt_spacex.launches
where success = true;

---
/* Ordering results */

select *
from dbt_spacex.launches
order by launches_date desc;

---
/*Counting by rocket*/

select  rocket
,count(*) as launch_count from de_db.public.launches
group by rocket;


---
/* Success rates*/

select rocket,
avg(case
when success = true then 1
else 0
end) as success_rate
from dbt_spacex.launches
group by rocket;

---
/*Launches per year*/

select year(launch_date) as launch_year,
count(*) as launches_number
from dbt_spacex.launches
group by launch_year;

---
/*Joining tables*/

select
    l.id,
    l.launch_date,
    r.name as rocket_name,
    r.active
from dbt_spacex.launches l
join dbt_spacex.rockets r on l.rocket = r.id;

---
/*Subquery*/

select *

from dbt_spacex.launches
where launch_date = (
    select max(launch_date) from dbt_spacex.launches
);


---
/*CTE(common table expression*/

with recent_launches as (

    select *
    from dbt_spacex.launches
    where launch_date > '2020-01-01'
)
select * from recent_launches;

---
/*create a view*/

create or replace view dbt_spacex.succesful_launches as
select *
from dbt_spacex.launches
where success = true;

select * from dbt_spacex.succesful_launches;
