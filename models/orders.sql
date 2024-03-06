{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

order_payments as (

    select
        order_id,

        {# /* sqlmesh had some trouble with the jinja for loop that was here. */ #}

        sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount,
        sum(case when payment_method = 'coupon' then amount else 0 end) as coupon_amount,
        sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
        sum(case when payment_method = 'gift_card' then amount else 0 end) as gift_card_amount,
        sum(amount) as total_amount

    from payments

    group by order_id

),

final as (

    select
        orders.order_id::int,
        orders.customer_id::int,
        orders.order_date::date,
        orders.status::str,

        order_payments.credit_card_amount::int,
        order_payments.coupon_amount::int,
        order_payments.bank_transfer_amount::int,
        order_payments.gift_card_amount::int,
        order_payments.total_amount::int

    from orders


    left join order_payments
        on orders.order_id = order_payments.order_id

)

select * from final
