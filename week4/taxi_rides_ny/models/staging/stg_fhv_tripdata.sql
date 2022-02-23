{{ config(materialized='view') }}


select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    *

from {{ source('staging','external_fhv_tripdata') }}
where dispatching_base_num is not null

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}


/* With deduplication:
{{ config(materialized='view') }}

with tripdata as
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rownumber
  from {{ source('staging','external_fhv_tripdata') }}
  where dispatching_base_num is not null 
)



select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    *

from tripdata
where rownumber = 1
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
*/