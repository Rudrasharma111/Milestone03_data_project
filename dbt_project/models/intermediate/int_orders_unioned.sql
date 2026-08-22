with unioned as (
    select * from {{ ref('stg_orders_batch') }}
    union all
    select * from {{ ref('stg_orders_stream') }}
),
deduped as (
    select *
    from unioned
    qualify row_number() over (
        partition by order_id
        order by loaded_at desc
    ) = 1
)

select * from deduped
