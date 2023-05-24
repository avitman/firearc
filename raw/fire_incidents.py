import requests
import snowflake.connector

# Step 1: Access the API
api_url = "https://data.cityofnewyork.us/resource/8m42-w767.json"

# Step 2: Retrieve all the data
limit = 1000
offset = 0
all_data = []

while True:
    response = requests.get(api_url, params={"$limit": limit, "$offset": offset})
    data = response.json()

    if not data:
        break

    all_data.extend(data)
    offset += limit

# Step 3: Parse and transform the data
transformed_data = []
for item in all_data:
    transformed_item = {
        "starfire_incident_id": item["starfire_incident_id"],
        "incident_datetime": item["incident_datetime"],
        "alarm_box_borough": item["alarm_box_borough"],
        "alarm_box_number": item["alarm_box_number"],
        "alarm_box_location": item["alarm_box_location"],
        "incident_borough": item["incident_borough"],
        "alarm_source_description_tx": item["alarm_source_description_tx"],
        "alarm_level_index_description": item["alarm_level_index_description"],
        "highest_alarm_level": item["highest_alarm_level"],
        "incident_classification": item["incident_classification"],
        "incident_classification_group": item["incident_classification_group"],
        "dispatch_response_seconds_qy": item["dispatch_response_seconds_qy"],
        "first_assignment_datetime": item["first_assignment_datetime"],
        "first_activation_datetime": item["first_activation_datetime"],
        "incident_close_datetime": item["incident_close_datetime"],
        "valid_dispatch_rspns_time_indc": item["valid_dispatch_rspns_time_indc"],
        "valid_incident_rspns_time_indc": item["valid_incident_rspns_time_indc"],
        "incident_response_seconds_qy": item["incident_response_seconds_qy"],
        "incident_travel_tm_seconds_qy": item["incident_travel_tm_seconds_qy"],
        "engines_assigned_quantity": item["engines_assigned_quantity"],
        "ladders_assigned_quantity": item["ladders_assigned_quantity"],
        "other_units_assigned_quantity": item["other_units_assigned_quantity"],
        "zipcode": item["zipcode"],
        "policeprecinct": item["policeprecinct"],
        "citycouncildistrict": item["citycouncildistrict"],
        "communitydistrict": item["communitydistrict"],
        "communityschooldistrict": item["communityschooldistrict"],
        "congressionaldistrict": item["congressionaldistrict"],
        "first_on_scene_datetime": item["first_on_scene_datetime"]
    }
    transformed_data.append(transformed_item)



# Step 5: Load the data into Snowflake
# In our task, we operate under the assumption of utilizing AWS Secret Manager to manage environment variables.
conn = snowflake.connector.connect(
    user='$(DBT_USER)',
    password='$(DBT_PASSWORD)',
    account='$(SF_ACCOUNT)',
    warehouse='compute_wh',
    database='firearc',
    schema='raw'
)

cursor = conn.cursor()

create_table_sql = '''
CREATE TABLE IF NOT EXISTS fire_incidents (
    starfire_incident_id NUMBER(38,0) PRIMARY KEY,
    incident_datetime TIMESTAMP_NTZ(9),
    alarm_box_borough VARCHAR(16777216),
    alarm_box_number NUMBER(38,0),
    alarm_box_location VARCHAR(16777216),
    incident_borough VARCHAR(16777216),
    alarm_source_description_tx VARCHAR(16777216),
    alarm_level_index_description VARCHAR(16777216),
    highest_alarm_level VARCHAR(16777216),
    incident_classification VARCHAR(16777216),
    incident_classification_group VARCHAR(16777216),
    dispatch_response_seconds_qy NUMBER(38,0),
    first_assignment_datetime TIMESTAMP_NTZ(9),
    first_activation_datetime TIMESTAMP_NTZ(9),
    incident_close_datetime TIMESTAMP_NTZ(9),
    valid_dispatch_rspns_time_indc VARCHAR(16777216),
    valid_incident_rspns_time_indc VARCHAR(16777216),
    incident_response_seconds_qy NUMBER(38,0),
    incident_travel_tm_seconds_qy NUMBER(38,0),
    engines_assigned_quantity NUMBER(38,0),
    ladders_assigned_quantity NUMBER(38,0),
    other_units_assigned_quantity NUMBER(38,0),
    zipcode NUMBER(38,0),
    policeprecinct NUMBER(38,0),
    citycouncildistrict NUMBER(38,0),
    communitydistrict NUMBER(38,0),
    communityschooldistrict NUMBER(38,0),
    congressionaldistrict NUMBER(38,0),
    first_on_scene_datetime TIMESTAMP_NTZ(9)
)
'''

cursor.execute(create_table_sql)

# Step 4: Iterate over each JSON object and perform incremental loading
for item in all_data:
    # Check if the record already exists in the table
    select_query = f"SELECT COUNT(*) FROM fire_incidents WHERE starfire_incident_id = '{item['starfire_incident_id']}'"
    result = cursor.execute(select_query)
    count = result.fetchone()[0]

    if count == 0:
        # Insert the record if it doesn't exist
        insert_query = f"INSERT INTO fire_incidents (starfire_incident_id, incident_datetime, alarm_box_borough, alarm_box_location, alarm_box_number, zipcode, incident_borough, alarm_source_description_tx, alarm_level_index_description, highest_alarm_level, incident_classification, incident_classification_group, dispatch_response_seconds_qy, first_assignment_datetime, first_activation_datetime, incident_close_datetime, valid_dispatch_rspns_time_indc, valid_incident_rspns_time_indc, incident_response_seconds_qy, incident_travel_tm_seconds_qy, engines_assigned_quantity, ladders_assigned_quantity, other_units_assigned_quantity, policeprecinct, citycouncildistrict, communitydistrict, communityschooldistrict, congressionaldistrict, first_on_scene_datetime) VALUES ('{item['starfire_incident_id']}', '{item['incident_datetime']}', '{item['alarm_box_borough']}', '{item['alarm_box_location']}', '{item['alarm_box_number']}', '{item['zipcode']}', '{item['incident_borough']}', '{item['alarm_source_description_tx']}', '{item['alarm_level_index_description']}', '{item['highest_alarm_level']}', '{item['incident_classification']}', '{item['incident_classification_group']}', '{item['dispatch_response_seconds_qy']}', '{item['first_assignment_datetime']}', '{item['first_activation_datetime']}', '{item['incident_close_datetime']}', '{item['valid_dispatch_rspns_time_indc']}', '{item['valid_incident_rspns_time_indc']}', '{item['incident_response_seconds_qy']}', '{item['incident_travel_tm_seconds_qy']}', '{item['engines_assigned_quantity']}', '{item['ladders_assigned_quantity']}', '{item['other_units_assigned_quantity']}', '{item['policeprecinct']}', '{item['citycouncildistrict']}', '{item['communitydistrict']}', '{item['communityschooldistrict']}', '{item['congressionaldistrict']}', '{item['first_on_scene_datetime']}')"
        cursor.execute(insert_query)

    else:
        # Update the record if it already exists
        update_query = f"UPDATE fire_incidents SET incident_datetime = '{item['incident_datetime']}', alarm_box_borough = '{item['alarm_box_borough']}', alarm_box_number = '{item['alarm_box_number']}', alarm_box_location = '{item['alarm_box_location']}', incident_borough = '{item['incident_borough']}', alarm_source_description_tx = '{item['alarm_source_description_tx']}', alarm_level_index_description = '{item['alarm_level_index_description']}', highest_alarm_level = '{item['highest_alarm_level']}', incident_classification = '{item['incident_classification']}', incident_classification_group = '{item['incident_classification_group']}', dispatch_response_seconds_qy = '{item['dispatch_response_seconds_qy']}', first_assignment_datetime = '{item['first_assignment_datetime']}', first_activation_datetime = '{item['first_activation_datetime']}', incident_close_datetime = '{item['incident_close_datetime']}', valid_dispatch_rspns_time_indc = '{item['valid_dispatch_rspns_time_indc']}', valid_incident_rspns_time_indc = '{item['valid_incident_rspns_time_indc']}', incident_response_seconds_qy = '{item['incident_response_seconds_qy']}', incident_travel_tm_seconds_qy = '{item['incident_travel_tm_seconds_qy']}', engines_assigned_quantity = '{item['engines_assigned_quantity']}', ladders_assigned_quantity = '{item['ladders_assigned_quantity']}', other_units_assigned_quantity = '{item['other_units_assigned_quantity']}', zipcode = '{item['zipcode']}', policeprecinct = '{item['policeprecinct']}', citycouncildistrict = '{item['citycouncildistrict']}', communitydistrict = '{item['communitydistrict']}', communityschooldistrict = '{item['communityschooldistrict']}', congressionaldistrict = '{item['congressionaldistrict']}', first_on_scene_datetime = '{item['first_on_scene_datetime']}' WHERE starfire_incident_id = '{item['starfire_incident_id']}'"
        cursor.execute(update_query)

# Insert transformed data into Snowflake table
cursor.executemany(insert_sql, transformed_data)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
