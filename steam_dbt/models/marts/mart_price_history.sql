select
    s.steam_appid,
    g.game_name,
    g.category,
    g.developer,
    g.publisher,

    s.country_code,
    s.snapshot_date,

    s.is_free,
    s.list_price,
    s.sale_price,
    s.discount_percent,
    s.currency

from {{ ref('stg_price_snapshots') }} as s

left join {{ ref('games') }} as g
    on s.steam_appid = g.steam_appid

where s.api_success = true