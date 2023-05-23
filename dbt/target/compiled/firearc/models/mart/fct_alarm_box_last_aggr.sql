


select * 
from FIREARC.CORE.stg_fire_incidents 

  where incident_datetime > (select max(incident_datetime) from FIREARC.CORE.fct_alarm_box_last_aggr)

qualify row_number() over( partition by alarm_box_number order by incident_datetime desc) = 1