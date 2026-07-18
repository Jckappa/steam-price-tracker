with ranked as (

    select
        steam_appid,
        game_name,
        country_code,
        snapshot_date,
        is_free,
        initial_price,
        final_price,
        discount_percent,
        currency,
        api_success,
        ingested_at,

        row_number() over (
            partition by steam_appid, country_code, snapshot_date
            order by ingested_at desc
        ) as rn

    from {{ source('steam_prices', 'raw_price_snapshots') }}

)

select
    steam_appid,
    game_name,
    country_code,
    snapshot_date,
    is_free,

    initial_price as initial_price_minor,
    final_price   as final_price_minor,
    
    concat(
        cast(steam_appid as string), '|',
        country_code, '|',
        cast(snapshot_date as string)
    ) as snapshot_key,

    cast(initial_price as numeric) / 100 as list_price,
    cast(final_price   as numeric) / 100 as sale_price,

    discount_percent,
    currency,
    api_success,
    ingested_at

from ranked
where rn = 1