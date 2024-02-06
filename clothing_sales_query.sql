USE DATABASE Clothing;
USE SCHEMA Sales;

select * 
from information_schema.tables
where table_schema = 'SALES';

select * 
from information_schema.columns
where table_schema = 'SALES';

select * 
from sales_records
limit 10;

select count(*) from sales_records;
select count(*) cnt from sales_records group by order_id having cnt > 1;
select count(distinct order_id) cnt from sales_records;

-- distribution (mostly even)
select distinct region
from sales_records;

select region, count(*) cnt
from sales_records
group by region;

select sales_channel, count(*) cnt
from sales_records
group by sales_channel;

select order_priority, count(*) cnt
from sales_records
group by order_priority;

-- earliest and latest order date
select * 
from sales_records
where order_date in (select min(order_date) min_order_date
                     from sales_records
                     union
                     select max(order_date) max_order_date
                     from sales_records
                     );

-- profits of more than average profit by year-month
with sale_profit as (
    select region, country, item_type, order_date, total_profit
    from sales_records    
),
profit_by_country as (
    select region, country, item_type, concat(year(order_date), '-', month(order_date)) year_month, sum(total_profit) total_profit
    from sale_profit
    group by region, country, item_type, concat(year(order_date), '-', month(order_date))
)
select region, country, item_type, year_month, total_profit
from profit_by_country
where total_profit > (select avg(total_profit)
                     from profit_by_country)
order by total_profit desc;

-- cumulative yearly profits for region's clothes sales
with sale_profit as (
    select region, item_type, year(order_date) as year, sum(total_revenue - unit_cost) as profit
    from sales_records 
    group by region, item_type, year(order_date)
), sale_profit_cumsum as (
    select region, item_type, year, 
        sum(profit) over (partition by region, item_type order by year asc) as profit_cumulative
    from sale_profit
)
select region, year, profit_cumulative, 
    round((profit_cumulative - lag(profit_cumulative) over (partition by region order by year)) 
            / profit_cumulative, 2) as percent_increase_yoy
from sale_profit_cumsum
where item_type = 'Clothes'
order by region, year;

-- latest sale for each region
select region, item_type, sales_channel, order_date, units_sold, total_profit
from (select region, item_type, sales_channel, order_date, units_sold, total_profit,
      dense_rank() over (partition by region, item_type order by order_date desc) dr
      from sales_records)
where dr = 1
order by region, item_type, order_date;

-- 2nd order within 2 days
select s1.region, s1.country, s1.item_type
from sales_records s1
join sales_records s2 on s1.country=s2.country and s1.order_id!=s2.order_id
where s1.sales_channel = 'Online' and s1.order_date - s2.order_date between 0 and 2;

-- top 2 items with most orders
with sorted_orders as (
    select region, country, item_type, order_date,
        rank() over (partition by region, country, item_type order by order_date) order_num
    from sales_records
    where region in ('North America','Europe')
)
select region, country, item_type, order_date,
    order_date - lag(order_date, 1) over (partition by region, country, item_type order by order_date) second_order_days_diff
from sorted_orders
where order_num <= 2;

-- shipping days average by year
select country, year(ship_date) ship_year, round(avg(ship_date - order_date)) shipping_days_yearly_avg
from sales_records
group by country, year(ship_date)
order by shipping_days_yearly_avg desc;

-- shipping process
with shipping_time as (
    select region, country, item_type,
        case when ship_date - order_date < 11 then 'early'
             when ship_date - order_date between 11 and 20 then 'on-time'
             else 'late' 
        end shipping_process
    from sales_records
)
select shipping_process, count(*) cnt
from shipping_time
group by shipping_process order by cnt desc;

