
  
    

        create or replace transient table FIREARC.CORE.fct_incident_sla
         as
        (


select 
 incident_borough,
 date_trunc('month', incident_datetime) as year_month,
 count(distinct starfire_incident_id) as incident_count,
 avg(dispatch_response_seconds_qy) as avg_dispatch_response_seconds,
 avg(incident_response_seconds_qy) as avg_incident_response_seconds,
 avg(incident_travel_tm_seconds_qy) as avg_incident_travel_tm_seconds
from firearc.raw.fire_incidents
where valid_incident_rspns_time_indc = 'Y'

group by 1,2
order by 1
        );
      
  