with bronze as (
    select
        trim(cast(area_nm as varchar)) as area_nm,
        trim(cast(area_cd as varchar)) as area_cd,
        lower(trim(cast(area_congest_lvl as varchar))) as area_congest_lvl,
        cast(area_congest_msg as varchar) as area_congest_msg,
        try_cast(nullif(trim(area_ppltn_min), '') as integer) as area_ppltn_min,
        try_cast(nullif(trim(area_ppltn_max), '') as integer) as area_ppltn_max,
        try_cast(nullif(trim(male_ppltn_rate), '') as decimal(5, 2)) as male_ppltn_rate,
        try_cast(nullif(trim(female_ppltn_rate), '') as decimal(5, 2)) as female_ppltn_rate,
        try_cast(nullif(trim(ppltn_rate_0), '') as decimal(5, 2)) as ppltn_rate_0,
        try_cast(nullif(trim(ppltn_rate_10), '') as decimal(5, 2)) as ppltn_rate_10,
        try_cast(nullif(trim(ppltn_rate_20), '') as decimal(5, 2)) as ppltn_rate_20,
        try_cast(nullif(trim(ppltn_rate_30), '') as decimal(5, 2)) as ppltn_rate_30,
        try_cast(nullif(trim(ppltn_rate_40), '') as decimal(5, 2)) as ppltn_rate_40,
        try_cast(nullif(trim(ppltn_rate_50), '') as decimal(5, 2)) as ppltn_rate_50,
        try_cast(nullif(trim(ppltn_rate_60), '') as decimal(5, 2)) as ppltn_rate_60,
        try_cast(nullif(trim(ppltn_rate_70), '') as decimal(5, 2)) as ppltn_rate_70,
        try_cast(nullif(trim(resnt_ppltn_rate), '') as decimal(5, 2)) as resnt_ppltn_rate,
        try_cast(nullif(trim(non_resnt_ppltn_rate), '') as decimal(5, 2)) as non_resnt_ppltn_rate,
        cast(replace_yn as varchar) as replace_yn,
        cast(ppltn_time as varchar) as ppltn_time,
        cast(fcst_yn as varchar) as fcst_yn,
        cast(ingested_at as varchar) as ingested_at
    from {{ source('bronze', 'bronze_seoul_ppltn') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by area_nm, ppltn_time
            order by ingested_at desc
        ) as row_num
    from bronze
    where area_nm is not null
        and area_cd is not null
        and ppltn_time is not null
)

select
    area_nm,
    area_cd,
    area_congest_lvl,
    area_congest_msg,
    area_ppltn_min,
    area_ppltn_max,
    male_ppltn_rate,
    female_ppltn_rate,
    ppltn_rate_0,
    ppltn_rate_10,
    ppltn_rate_20,
    ppltn_rate_30,
    ppltn_rate_40,
    ppltn_rate_50,
    ppltn_rate_60,
    ppltn_rate_70,
    resnt_ppltn_rate,
    non_resnt_ppltn_rate,
    replace_yn,
    ppltn_time,
    fcst_yn,
    ingested_at
from ranked
where row_num = 1
