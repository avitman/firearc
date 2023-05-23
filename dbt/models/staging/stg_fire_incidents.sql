with source as (

    select * from {{ source('raw', 'fire_incidents') }}

),

renamed as (

    select
        starfire_incident_id,
        incident_datetime,
        alarm_box_borough,
        alarm_box_number,
        alarm_box_location,
        incident_borough,
        alarm_source_description_tx,
        alarm_level_index_description,
        highest_alarm_level,
        incident_classification,
        incident_classification_group,
        dispatch_response_seconds_qy,
        first_assignment_datetime,
        first_activation_datetime,
        incident_close_datetime,
        valid_dispatch_rspns_time_indc,
        valid_incident_rspns_time_indc,
        incident_response_seconds_qy,
        incident_travel_tm_seconds_qy,
        engines_assigned_quantity,
        ladders_assigned_quantity,
        other_units_assigned_quantity,
        zipcode,
        policeprecinct,
        citycouncildistrict,
        communitydistrict,
        communityschooldistrict,
        congressionaldistrict,
        first_on_scene_datetime

    from source

)

select * from renamed