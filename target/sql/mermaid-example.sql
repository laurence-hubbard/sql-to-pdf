use analytics;

create table enriched_orders as
select
  o.order_id,
  c.customer_id,
  p.product_id
from raw.orders o
join raw.customers c on o.customer_id = c.customer_id
join raw.products p on o.product_id = p.product_id;

insert into table reporting.order_summary
select *
from enriched_orders;
