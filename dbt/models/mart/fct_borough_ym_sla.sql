{{ 
    config(
        tags=['monthly','fire_incidents'],
        materialized='incremental'              
    )
}}


select 
 incident_borough,
 date_trunc('month', incident_datetime) as year_month,
 count(distinct starfire_incident_id) as incident_count,
 avg(dispatch_response_seconds_qy) as avg_dispatch_response_seconds,
 avg(incident_response_seconds_qy) as avg_incident_response_seconds,
 avg(incident_travel_tm_seconds_qy) as avg_incident_travel_tm_seconds
from {{ ref('stg_fire_incidents') }} 
where valid_incident_rspns_time_indc = 'Y'
{% if is_incremental() %}
  and  date_trunc('month', incident_datetime) > (select max(year_month) from {{ this }})
{% endif %}
group by 1,2
order by 1,2
