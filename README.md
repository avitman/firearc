### What's in this repo?

1. Python raw data flows
2. dbt project 

### Data Flow Description

## Raw:

Using a Python process to upload incremental with upsert strategy the fire incident dispatch data API into Snowflake.

## dbt:

Utilizing dbt for data transformation enhances performance, enables version control through Github, ensures high-quality data through robust quality assurance processes, documents data catalog using the documentation portal, and maintains modularity for data lineage.

The models layers:

Staging: 
The staging layer serves as a temporary storage area where data can be cleaned, validated, and prepared for further processing using materialized views.
Mart: 
Data marts are designed to serve the needs of specific business units or user groups, providing them with tailored and efficient access to the data they require. These marts are typically created by filtering, aggregating, and transforming the data from the raw or staging layer, with a focus on delivering actionable insights and facilitating analytical workflows.

The following models were created:
stg_fire_incidents (daily, view)
fct_alarm_box_first_aggr (daily, incremental with upsert strategy)
The first incident per alarm box.
fct_alarm_box_last_aggr (daily, incremental with upsert strategy)
The last incident per alarm box.
fct_borough_ym_sla (monthly, incremental with append only strategy)
SLA metrics for each borough and month of a given year, without any historical modifications.


Fire Incidents DAG:

![image](https://github.com/avitman/firearc/assets/49658823/3fdbafaa-a35c-4f75-8344-379327caa74d)


## Airflow (in other repo):
We utilize Airflow to perform the orchestration.
Schedules – Daily at 08:00 UTC.
Slack/Email alerts on: 
Failed job.
Table wasn’t updated in the past day.




