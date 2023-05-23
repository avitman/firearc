
  
    

        create or replace transient table FIREARC.CORE.fct_alarm_box_last_aggr
         as
        (


select * 
from FIREARC.raw.stg_fire_incidents 

qualify row_number() over( partition by alarm_box_number order by incident_datetime desc) = 1
        );
      
  