 with
    customers as (select * from {{ ref('stg_customers') }}),
    orders as (select * from {{ ref('stg_orders') }}),
    customer_orders as (
        select
            c.customer_id,
            c.email,
            c.country,
            c.city,
            min(o.order_approved_at) as first_order_date,
            max(o.order_approved_at) as most_recent_order_date,
            count(o.order_id) as total_orders
        from orders o
        inner join customers c on o.customer_id = c.customer_id
        group by 1, 2, 3, 4
    )
select * from customer_orders