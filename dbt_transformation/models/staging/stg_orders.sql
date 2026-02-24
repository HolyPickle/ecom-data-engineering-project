select
    order_id,
    customer_id,
    status as order_status,
    order_approval_date,
    order_devlivery_date
from {{ source('raw_data', 'orders') }}