CREATE OR REPLACE PROCEDURE get_ratings_insights() 
AS $$
BEGIN
insert into insights (as_of_date, platform, seven_day_average, seven_day_trend, historical_average, historical_trend)

with
temp1 as (
select
count(*) as count,
case
when count = 0 then '2021-01-01'
else max(as_of_date)
end as max_date
from
insights),

temp2 as (
select
*
from
ingestion as a, temp1
where as_of_date > date(dateadd(day, -7, temp1.max_date))
),

temp3 as (
select
  as_of_date,
  platform,
  avg(average) over (order by as_of_date rows between 7 preceding and 1 preceding) as seven_day_average,
  case 
   when average > seven_day_average then 'customer ratings increased'
   when average = seven_day_average then 'customer ratings did not change'
   else 'customer ratings decreased'
  end as seven_day_trend,
  sum(average) over (order by as_of_date rows between 1 preceding and 1 preceding) as historical_average,
  case
  	when average > historical_average then 'customer ratings increased'
    when average = historical_average then 'customer ratings did not change'
    else 'customer ratings decreased'
  end as historical_trend
  from temp2
  where platform = 'ios'
),

temp4 as (
select
  as_of_date,
  platform,
  avg(average) over (order by as_of_date rows between 7 preceding and 1 preceding) as seven_day_average,
  case 
   when average > seven_day_average then 'customer ratings increased'
   when average = seven_day_average then 'customer ratings did not change'
   else 'customer ratings decreased'
  end as seven_day_trend,
  sum(average) over (order by as_of_date rows between 1 preceding and 1 preceding) as historical_average,
  case
  	when average > historical_average then 'customer ratings increased'
    when average = historical_average then 'customer ratings did not change'
    else 'customer ratings decreased'
  end as historical_trend
  from temp2
  where platform = 'android'
),

temp5 as (
select * from temp3, temp1 where as_of_date > temp1.max_date
union all
select * from temp4, temp1 where as_of_date > temp1.max_date)

select as_of_date, platform, seven_day_average, seven_day_trend, historical_average, historical_trend from temp5;
END
$$ LANGUAGE plpgsql;             
