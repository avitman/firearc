{{ 
    config(
        tags=['daily','fire_incidents'],
        materialized='incremental',
        unique_key='alarm_box_number'                 
    )
}}


select * 
from {{ ref('stg_fire_incidents') }} 
{% if is_incremental() %}
  where incident_datetime > (select max(incident_datetime) from {{ this }})
{% endif %}
qualify row_number() over( partition by alarm_box_number order by incident_datetime desc) = 1