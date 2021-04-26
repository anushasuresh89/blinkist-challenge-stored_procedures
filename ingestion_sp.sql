-- move data from staging to ingestion
CREATE OR REPLACE PROCEDURE public.stg_to_ingestion() 
LANGUAGE plpgsql AS $$
BEGIN 
insert into ingestion 
select 
* 
from stg_ingestion 
where as_of_date not in (select distinct(as_of_date) from ingestion where platform = 'ios') 
and platform = 'ios' 
union all 
select 
* 
from 
stg_ingestion 
where as_of_date not in (select distinct(as_of_date) from ingestion where platform = 'android') 
and platform = 'android'; 
END $$


-- truncate the table once the data has been processed
CREATE OR REPLACE PROCEDURE public.clean_staging() 
LANGUAGE plpgsql AS $$
BEGIN 
truncate stg_ingestion;
END $$

-- call themm back to back
CREATE OR REPLACE PROCEDURE ingest_and_clean_staging() 
LANGUAGE plpgsql AS $$
BEGIN 
call stg_to_ingestion();
call clean_staging();
END $$
