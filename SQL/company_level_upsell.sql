/*
Tracks every upsold customer's EC ordering over 365 days from CR order date.
Cohorts are based on the quarter of the CR order.

Need to add netsuite bookings data for top paying customers.
 */

-- use database da_prod_db;
-- use schema analyst_reporting;
--
-- create or replace view vw_company_upsells_weekly as

with total_spend as (
    /*
     Start with defining the top companies from 2022. To do this, we find the total spend in 2022
     and create an ordered list of accounts by spend. The account
     is considered a top account if the cumulative sum of spend is less than the cutoff value.
     */
    select
        sum(dfb.net_amount_in_usd)                                              as total_amt
        , round(total_amt * 0.8)                                                as cutoff_num
    from
        da_prod_db.datacore.fact_booking dfb
    where
        dfb.date >= '2022-01-01'
        and dfb.date < '2023-01-01'
    )
   , company_spend_last_yr as (
    select
        initcap(dfb.end_customer)                                               as company_name
        , sum(dfb.net_amount_in_usd)                                            as company_net_amt
    from
        da_prod_db.datacore.fact_booking dfb
    where
        dfb.date >= '2022-01-01'
        and dfb.date < '2023-01-01'
    group by
        1
    )
   , top_paying_companies as (
    select
        company_spend_last_yr.company_name
        , company_spend_last_yr.company_net_amt
        , total_spend.cutoff_num
        , sum(company_spend_last_yr.company_net_amt) over
        (order by company_spend_last_yr.company_net_amt desc)                   as running_amount
        , iff(running_amount <= total_spend.cutoff_num
        , true
        , false)                                                                as is_top_paying_company
    from
        company_spend_last_yr
        join total_spend
            on true
    )


   , bookings_order_type as (
    select
        fact_booking.buck_sales_order_id                                        as buck_order_id
        , to_date(fact_booking.date)                                            as order_date
        , first_value(fact_booking.item_product_family) over
            (partition by fact_booking.buck_sales_order_id
            order by item_product_family desc)                                  as order_family
        , fact_booking.commerce_id
        , fact_booking.item_product_line
        , fact_booking.item_name
        , initcap(fact_booking.end_customer)                                    as company_name
        , coalesce(top_paying_companies.is_top_paying_company, false)           as is_top_paying_company
        , fact_booking.net_amount_in_usd
    from
        da_prod_db.datacore.fact_booking
        left join top_paying_companies
            on fact_booking.end_customer = top_paying_companies.company_name
    where
        fact_booking.net_amount_in_usd is not null
        and fact_booking.date_shipped is not null
        and fact_booking.buck_sales_order_id is not null
    )
   , bookings as (
    select
        bot.buck_order_id
        , bot.order_family
        , bot.company_name
        , bot.is_top_paying_company
        , listagg(distinct case when bot.commerce_id not like '%delivery-group%'
            then bot.commerce_id end,  ', ')                                    as commerce_id_list
        , listagg(distinct bot.item_product_line, ', ')                         as product_lines
        , iff(product_lines ilike '%gko%', true, false)                         as is_gko_order
        , min(bot.order_date)                                                   as order_date
        , count(distinct bot.commerce_id)                                       as number_sales_order_items
        , sum(bot.net_amount_in_usd)                                            as net_amount
    from
        bookings_order_type                                                     bot
    group by
        1
        , 2
        , 3
        , 4
    )
   , sales_order_item_rollup as (
    select
        fnoi.buck_order_id
        , fnoi.end_company_name                                                 as company_name
        , fnoi.customer_tier
        , row_number() over (partition by fnoi.buck_order_id
            order by fnoi.order_date)                                           as rn
    from
        da_prod_db.datacore.fact_netsuite_order_items                           fnoi
    where
        fnoi.ship_date is not null
        and fnoi.ship_attention is not null -- 29 rows where ship date is not null and ship attention is null after 2022-01-01
    qualify
        rn = 1
    )
   , customer_orders as (
    /*
     We define the order type as CR or EC. We also add in relevant account info.
     */
    select
        soi.company_name
        , fbk.is_top_paying_company
        , fbk.buck_order_id
        , sfo.id                                                                as salesforce_order_id
        , mode(sfa.id) over (partition by soi.company_name)                     as salesforce_account_id
        , mode(sfa.name) over (partition by soi.company_name)                   as salesforce_account_name
        , mode(sfa.segmentf__c) over (partition by soi.company_name)            as segment
        , mode(sfa.industry) over (partition by soi.company_name)               as industry
        , mode(sfa.institution_type__c) over (partition by soi.company_name)    as institution_type
        , mode(sfa.numberofemployees) over (partition by soi.company_name)      as number_of_employees
        , mode(soi.customer_tier) over (partition by soi.company_name)          as company_tier
        , mode(sfa.type__c) over (partition by soi.company_name)                as company_type
        , fbk.order_date
        , iff(fbk.order_family = 'Engineered Cells', 'EC', 'CR')                as order_type
        , fbk.commerce_id_list
        , fbk.product_lines
        , fbk.number_sales_order_items
        , fbk.net_amount
        , min(fbk.order_date) over (partition by soi.company_name)              as first_order_at
        , min(iff(order_type = 'CR'
                  , fbk.order_date
                  , null)) over (partition by soi.company_name)                 as first_cr_ordered_at
        , max(iff(fbk.is_gko_order
                  , fbk.order_date
                  , null)) over (partition by soi.company_name)                 as last_gko_order_at
        , min(iff(order_type = 'EC'
                  , fbk.order_date
                  , null)) over (partition by soi.company_name)                 as first_ec_ordered_at
    from
        sales_order_item_rollup                                                 soi
    left join bookings                                                          fbk
        on soi.buck_order_id = fbk.buck_order_id
    left join stitch.stitch_salesforce_prod."ORDER"                             sfo
        on soi.buck_order_id = sfo.buck_id__c
    left join stitch.stitch_salesforce_prod.account                             sfa
        on sfo.accountid = sfa.id
    )
   , customer_daily_rollup as (
    select
        to_date(customer_orders.order_date)                                     as order_date
        , customer_orders.company_name
        , customer_orders.is_top_paying_company
        , customer_orders.segment
        , customer_orders.industry
        , customer_orders.institution_type
        , customer_orders.number_of_employees
        , customer_orders.company_tier
        , customer_orders.company_type
        , customer_orders.first_order_at
        , customer_orders.first_cr_ordered_at
        , customer_orders.last_gko_order_at
        , customer_orders.first_ec_ordered_at
        , year(customer_orders.first_ec_ordered_at)                             as ec_purchase_yr
        , max(customer_orders.order_type)                                       as order_type
        , listagg(customer_orders.buck_order_id, ', ')                          as list_sales_orders
        , count(customer_orders.buck_order_id)                                  as number_of_orders
        , listagg(distinct customer_orders.commerce_id_list, ', ')              as list_commerce_id
        , listagg(distinct customer_orders.product_lines, ', ')                 as list_product_lines
        , sum(customer_orders.number_sales_order_items)                         as number_of_items
        , sum(customer_orders.net_amount)                                       as total_order_amount
    from
        customer_orders
    where
        customer_orders.first_order_at >= '2020-01-01'
    group by
        1
        , 2
        , 3
        , 4
        , 5
        , 6
        , 7
        , 8
        , 9
        , 10
        , 11
        , 12
        , 13
    )
   , customer_spine as (
    select
        to_date(dim_dates.date)                                                 as dte
        , cust.company_name
        , ord.is_top_paying_company
        , to_varchar(date_part(year
        , cust.first_order_at))
        ||
          ' Q'
        ||
          to_varchar(date_part(quarter
              , cust.first_order_at))                                           as company_quarter_cohort
        , ord.segment
        , ord.industry
        , ord.institution_type
        , ord.number_of_employees
        , ord.company_tier
        , ord.company_type
        , cust.first_cr_ordered_at
        , cust.last_gko_order_at
        , cust.first_ec_ordered_at
        , cust.ec_purchase_yr
        , iff(ord.order_date is not null, dte, null)                            as order_date
        , ord.order_type
        , ord.list_sales_orders
        , iff(ord.order_date is not null, ord.number_of_orders, 0)              as num_orders
        , ord.list_commerce_id
        , ord.list_product_lines
        , iff(ord.order_date is not null, ord.number_of_items, 0)               as num_items
        , iff(ord.order_date is not null, ord.total_order_amount, 0)            as total_amount
    from
        da_prod_db.analyst_reporting.dim_dates
        cross join (
            select distinct
                customer_daily_rollup.company_name
                , customer_daily_rollup.first_order_at
                , customer_daily_rollup.first_cr_ordered_at
                , customer_daily_rollup.last_gko_order_at
                , customer_daily_rollup.first_ec_ordered_at
                , customer_daily_rollup.ec_purchase_yr
            from
                customer_daily_rollup
            ) as cust
        left join (
            select distinct
                customer_daily_rollup.company_name
                , customer_daily_rollup.is_top_paying_company
                , customer_daily_rollup.segment
                , customer_daily_rollup.industry
                , customer_daily_rollup.institution_type
                , customer_daily_rollup.number_of_employees
                , customer_daily_rollup.company_tier
                , customer_daily_rollup.company_type
                , customer_daily_rollup.order_date
                , customer_daily_rollup.order_type
                , customer_daily_rollup.list_sales_orders
                , customer_daily_rollup.number_of_orders
                , customer_daily_rollup.list_commerce_id
                , customer_daily_rollup.list_product_lines
                , customer_daily_rollup.number_of_items
                , customer_daily_rollup.total_order_amount
            from
                customer_daily_rollup
            ) as ord
            on to_date(dim_dates.date) = ord.order_date
            and cust.company_name = ord.company_name
    where
        (to_date(dim_dates.date) between
            to_date(cust.first_order_at)
            and current_date())
    )
   , upsell_tracking as (
    /*
    Measures upsell metric per customer.
     */
    select
        customer_spine.dte
        , customer_spine.company_name
        , coalesce(is_top_paying_company, lag(is_top_paying_company)
            ignore nulls over (partition by customer_spine.company_name
                order by dte))                                                  as is_top_paying_company
        , customer_spine.company_quarter_cohort
        , coalesce(segment, lag(segment)
            ignore nulls over (partition by customer_spine.company_name
                order by dte))                                                  as segment
        , coalesce(industry, lag(industry)
            ignore nulls over (partition by customer_spine.company_name
                order by dte))                                                  as industry
        , coalesce(institution_type, lag(institution_type)
            ignore nulls over (partition by customer_spine.company_name
                order by dte))                                                  as institution_type
        , coalesce(number_of_employees, lag(number_of_employees)
            ignore nulls over (partition by customer_spine.company_name
                order by dte))                                                  as number_of_employees
        , coalesce(company_tier, lag(company_tier)
            ignore nulls over (partition by customer_spine.company_name
                order by dte))                                                  as company_tier
        , coalesce(company_type, lag(company_type)
            ignore nulls over (partition by customer_spine.company_name
                order by dte))                                                  as company_type
        , customer_spine.first_cr_ordered_at
        , customer_spine.last_gko_order_at                                      as upsell_target_date
        , customer_spine.first_ec_ordered_at
        , customer_spine.order_date
--         , customer_spine.order_type
        , customer_spine.list_sales_orders
        , customer_spine.num_orders
        , customer_spine.list_commerce_id
        , customer_spine.list_product_lines
        , customer_spine.num_items
        , iff(customer_spine.order_type = 'EC'
                  and customer_spine.first_ec_ordered_at >=
                      customer_spine.first_cr_ordered_at
                  and year(customer_spine.order_date) = ec_purchase_yr
            , true
            , false)                                                            as is_upsell
        , iff(is_upsell, customer_spine.dte, null)                              as upsell_date
        , case
              when customer_spine.first_ec_ordered_at is null
                  and customer_spine.list_product_lines ilike '%gko%'
                  then true
              when customer_spine.first_ec_ordered_at is not null
                  then false
          end                                                                   as is_upsell_target
        , customer_spine.total_amount
    from
        customer_spine
    )
   , cumulative_upsells as (
    select
        upsell_tracking.dte
        , year(upsell_tracking.dte)                                             as yr
        , upsell_tracking.company_name
        , upsell_tracking.is_top_paying_company
        , upsell_tracking.company_quarter_cohort
        , upsell_tracking.segment
        , upsell_tracking.industry
        , upsell_tracking.institution_type
        , upsell_tracking.number_of_employees
        , upsell_tracking.company_tier
        , upsell_tracking.company_type
        , upsell_tracking.first_cr_ordered_at
        , upsell_tracking.upsell_target_date
        , upsell_tracking.first_ec_ordered_at
        , upsell_tracking.order_date
        , upsell_tracking.list_sales_orders
        , upsell_tracking.num_orders
        , upsell_tracking.list_commerce_id
        , upsell_tracking.list_product_lines
        , upsell_tracking.num_items
        , upsell_tracking.is_upsell
        , upsell_tracking.upsell_date
        , coalesce(upsell_tracking.is_upsell_target
            , lag(upsell_tracking.is_upsell_target) ignore nulls
            over (partition by upsell_tracking.company_name
                order by dte))                                                  as is_upsell_target
        , upsell_tracking.total_amount
        , sum(iff(upsell_tracking.order_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , upsell_tracking.num_orders
            , 0))
            over (partition by upsell_tracking.company_name, yr
                order by upsell_tracking.dte)                                   as cumulative_order_count
        , sum(iff(upsell_tracking.order_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , upsell_tracking.num_items
            , 0))
            over (partition by upsell_tracking.company_name, yr
                order by upsell_tracking.dte)                                   as cumulative_item_count
        , sum(iff(upsell_tracking.upsell_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , 1
            , 0))
            over (partition by upsell_tracking.company_name, yr
                order by upsell_tracking.dte)                                   as cumulative_upsell_count
        , sum(iff(upsell_tracking.order_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , total_amount
            , 0))
            over (partition by upsell_tracking.company_name, yr
                order by upsell_tracking.dte)                                   as cumulative_order_amt
        , sum(iff(upsell_tracking.upsell_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , total_amount
            , 0))
            over (partition by upsell_tracking.company_name, yr
                order by upsell_tracking.dte)                                   as cumulative_upsell_amt
--         , cumulative_upsell_amt * 1.25                                          as target_amt
    from
        upsell_tracking
    )
    , final as (
    select
        cumulative_upsells.company_name
        , date_trunc('week', cumulative_upsells.dte)                            as wk
        , weekiso(cumulative_upsells.dte)                                       as wk_num
        , cumulative_upsells.yr
        , coalesce(cumulative_upsells.is_top_paying_company, false)             as is_top_paying_company
        , cumulative_upsells.company_quarter_cohort
        , cumulative_upsells.segment
        , cumulative_upsells.industry
        , cumulative_upsells.institution_type
        , cumulative_upsells.number_of_employees
        , cumulative_upsells.company_tier
        , cumulative_upsells.company_type
        , cumulative_upsells.first_cr_ordered_at
        , cumulative_upsells.upsell_target_date
        , cumulative_upsells.first_ec_ordered_at
        , cumulative_upsells.is_upsell_target
        , max(date_trunc('week', cumulative_upsells.order_date))                as order_wk
        , max(date_trunc('week', cumulative_upsells.upsell_date))               as upsell_wk
        , listagg(cumulative_upsells.list_sales_orders, ', ')                   as list_sales_orders
        , sum(cumulative_upsells.num_orders)                                    as number_of_orders
        , listagg(cumulative_upsells.list_commerce_id, ', ')                    as list_commerce_id
        , listagg(distinct cumulative_upsells.list_product_lines, ', ')         as product_lines
        , sum(cumulative_upsells.num_items)                                     as number_of_items
        , sum(cumulative_upsells.total_amount)                                  as total_amt
        , max(cumulative_upsells.cumulative_order_count)                        as cumulative_order_count
        , max(cumulative_upsells.cumulative_item_count)                         as cumulative_item_count
        , max(cumulative_upsells.cumulative_order_amt)                          as cumulative_order_amt
        , max(cumulative_upsells.cumulative_upsell_count)                       as cumulative_upsell_count
        , max(cumulative_upsells.cumulative_upsell_amt)                         as cumulative_upsell_amt
    from
        cumulative_upsells
    group by
        1
        , 2
        , 3
        , 4
        , 5
        , 6
        , 7
        , 8
        , 9
        , 10
        , 11
        , 12
        , 13
        , 14
        , 15
        , 16
    )
--    , target_values as (
--     select
--         base.wk
--         , base.wk_num
--         , base.yr
--         , iff(yr =
--               year(dateadd(year, -1, current_date))
--         , sum(base.cumulative_upsell_amt)
--         , null)                                                                 as cumulative_upsell_amount
--         , cumulative_upsell_amount * 1.25                                       as target_amount
--     from
--         base
--     group by
--         1
--         , 2
--         , 3
--     )
--    , final as (
--     select
--         base.full_site
--         , base.company_name
--         , base.wk
--         , base.wk_num
--         , base.yr
--         , base.is_top_paying_company
--         , base.customer_quarter_cohort
--         , base.segment
--         , base.industry
--         , base.institution_type
--         , base.number_of_employees
--         , base.order_wk
--         , base.upsell_wk
--         , base.number_of_orders
--         , base.total_amt
--         , base.sales_order_numbers
--         , base.number_of_items
--         , base.order_count
--         , base.cumulative_order_amt
--         , base.upsell_count
--         , base.cumulative_upsell_amt
--         , target_values.target_amount
--     from
--         base
--         left join target_values
--             on base.wk_num = target_values.wk_num
--     where target_values.target_amount is not null
--     )
select * from final