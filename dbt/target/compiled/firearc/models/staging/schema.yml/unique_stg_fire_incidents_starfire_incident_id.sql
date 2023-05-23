
    
    

select
    starfire_incident_id as unique_field,
    count(*) as n_records

from FIREARC.CORE.stg_fire_incidents
where starfire_incident_id is not null
group by starfire_incident_id
having count(*) > 1


