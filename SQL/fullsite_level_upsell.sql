/*
Tracks every upsold customer's EC ordering over 365 days from CR order date.
Cohorts are based on the quarter of the CR order.

Query is at the full_site level.
 */

use database da_prod_db;
use schema analyst_reporting;

create or replace view vw_fullsite_upsells_weekly as

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
    )
   , bookings as (
    select
        bot.buck_order_id
        , bot.order_family
        , bot.company_name
        , bot.is_top_paying_company
        , min(bot.order_date)                                                   as order_date
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
        foi.buckaneer_sales_order_id
        , first_value(foi.salesforce_sales_order_id) ignore nulls over
            (partition by foi.buckaneer_sales_order_id
            order by foi.full_site)                                             as salesforce_order_id
        , first_value(foi.salesforce_account_id) ignore nulls over
            (partition by foi.buckaneer_sales_order_id
            order by foi.full_site)                                             as salesforce_account_id
        , first_value(foi.full_site) ignore nulls over
            (partition by foi.buckaneer_sales_order_id
            order by foi.full_site)                                             as full_site
        , count(foi.barb_sales_order_item_commerce_id) over
            (partition by foi.buckaneer_sales_order_id
            order by foi.chosen_order_created_at)                               as num_items_in_order
        , row_number() over (partition by foi.buckaneer_sales_order_id
            order by foi.chosen_order_created_at)                               as rn
    from
        da_prod_db.datacore.fact_order_items                                    foi
    qualify
        rn = 1
    )
   , customer_orders as (
    /*
     We define the order type as CR or EC. We also add in relevant account info.
     */
    select
        soi.full_site
        , fbk.company_name
        , fbk.is_top_paying_company
        , fbk.buck_order_id
        , soi.salesforce_order_id
        , soi.salesforce_account_id
        , mode(sfa.segment__c) over
            (partition by fbk.company_name)                                     as segment
        , mode(sfa.industry) over
            (partition by fbk.company_name)                                     as industry
        , mode(sfa.institution_type__c) over
            (partition by fbk.company_name)                                     as institution_type
        , mode(sfa.numberofemployees) over
            (partition by fbk.company_name)                                     as number_of_employees
        , fbk.order_date
        , iff(fbk.order_family = 'Engineered Cells', 'EC', 'CR')                as order_type
        , soi.num_items_in_order
        , fbk.net_amount
        , min(fbk.order_date) over (partition by soi.full_site)                 as first_order_at_pst
        , min(iff(order_type = 'CR'
                  , fbk.order_date
                  , null)) over (partition by soi.full_site)                    as first_cr_ordered_at_pst
        , min(iff(order_type = 'EC'
                  , fbk.order_date
                  , null)) over (partition by soi.full_site)                    as first_ec_ordered_at_pst
    from
        bookings                                                                fbk
    left join sales_order_item_rollup                                           soi
        on fbk.buck_order_id = soi.buckaneer_sales_order_id
    left join stitch.stitch_salesforce_prod."ORDER"                             sfo
        on fbk.buck_order_id = sfo.buck_id__c
    left join stitch.stitch_salesforce_prod.account                             sfa
        on sfo.accountid = sfa.id
    )
   , customer_daily_rollup as (
    select
        to_date(customer_orders.order_date)                                     as order_date
        , customer_orders.full_site
        , customer_orders.company_name
        , customer_orders.is_top_paying_company
        , customer_orders.segment
        , customer_orders.industry
        , customer_orders.institution_type
        , customer_orders.number_of_employees
        , customer_orders.first_order_at_pst
        , customer_orders.first_cr_ordered_at_pst
        , customer_orders.first_ec_ordered_at_pst
        , year(customer_orders.first_ec_ordered_at_pst)                         as ec_purchase_yr
        , max(customer_orders.order_type)                                       as order_type
        , count(customer_orders.buck_order_id)                                  as number_of_orders
        , listagg(customer_orders.buck_order_id, ', ')                          as list_sales_orders
        , sum(customer_orders.num_items_in_order)                               as number_of_items
        , sum(customer_orders.net_amount)                                       as total_order_amount
    from
        customer_orders
    where
        customer_orders.first_order_at_pst >= '2020-01-01'
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
    )
   , customer_spine as (
    select
        to_date(dim_dates.date)                                                 as dte
        , cust.full_site
        , ord.company_name
        , ord.is_top_paying_company
        , to_varchar(date_part(year
        , cust.first_order_at_pst))
        ||
          ' Q'
        ||
          to_varchar(date_part(quarter
              , cust.first_order_at_pst))                                       as customer_quarter_cohort
        , ord.segment
        , ord.industry
        , ord.institution_type
        , ord.number_of_employees
        , cust.first_cr_ordered_at_pst
        , cust.first_ec_ordered_at_pst
        , cust.ec_purchase_yr
        , iff(ord.order_date is not null, dte, null)                            as order_date
        , ord.order_type
        , iff(ord.order_date is not null, ord.number_of_orders, 0)              as num_orders
        , ord.list_sales_orders
        , iff(ord.order_date is not null, ord.number_of_items, 0)               as num_items
        , iff(ord.order_date is not null, ord.total_order_amount, 0)            as total_amount
    from
        da_prod_db.analyst_reporting.dim_dates
        cross join (
            select distinct
                customer_daily_rollup.full_site
                , customer_daily_rollup.first_order_at_pst
                , customer_daily_rollup.first_cr_ordered_at_pst
                , customer_daily_rollup.first_ec_ordered_at_pst
                , customer_daily_rollup.ec_purchase_yr
            from
                customer_daily_rollup
            ) as cust
        left join (
            select distinct
                customer_daily_rollup.full_site
                , customer_daily_rollup.company_name
                , customer_daily_rollup.is_top_paying_company
                , customer_daily_rollup.segment
                , customer_daily_rollup.industry
                , customer_daily_rollup.institution_type
                , customer_daily_rollup.number_of_employees
                , customer_daily_rollup.order_date
                , customer_daily_rollup.order_type
                , customer_daily_rollup.number_of_orders
                , customer_daily_rollup.number_of_items
                , customer_daily_rollup.list_sales_orders
                , customer_daily_rollup.total_order_amount
            from
                customer_daily_rollup
            ) as ord
            on to_date(dim_dates.date) = ord.order_date
            and cust.full_site = ord.full_site
    where
        (to_date(dim_dates.date) between
            to_date(cust.first_order_at_pst)
            and current_date())
    )
   , upsell_tracking as (
    /*
    Measures upsell metric per customer.
     */
    select
        customer_spine.dte
        , customer_spine.full_site
        , coalesce(customer_spine.company_name, lag(customer_spine.company_name)
            ignore nulls over (partition by customer_spine.full_site
                order by dte))                                                  as company_name
        , coalesce(is_top_paying_company, lag(is_top_paying_company)
            ignore nulls over (partition by full_site order by dte))            as is_top_paying_company
        , customer_spine.customer_quarter_cohort
        , coalesce(segment, lag(segment)
            ignore nulls over (partition by full_site order by dte))            as segment
        , coalesce(industry, lag(industry)
            ignore nulls over (partition by full_site order by dte))            as industry
        , coalesce(institution_type, lag(institution_type)
            ignore nulls over (partition by full_site order by dte))            as institution_type
        , coalesce(number_of_employees, lag(number_of_employees)
            ignore nulls over (partition by full_site order by dte))            as number_of_employees
        , customer_spine.order_date
--         , customer_spine.first_cr_ordered_at_pst
--         , customer_spine.order_type
        , customer_spine.num_orders
        , customer_spine.list_sales_orders
        , customer_spine.num_items
        , iff(customer_spine.order_type = 'EC'
                  and customer_spine.first_ec_ordered_at_pst >=
                      customer_spine.first_cr_ordered_at_pst
                  and year(customer_spine.order_date) = ec_purchase_yr
            , true
            , false)                                                            as is_upsell
        , iff(is_upsell, customer_spine.dte, null)                              as upsell_date
        , customer_spine.total_amount
    from
        customer_spine
    )
   , cumulative_upsells as (
    select
        upsell_tracking.dte
        , year(upsell_tracking.dte)                                             as yr
        , upsell_tracking.full_site
        , upsell_tracking.company_name
        , upsell_tracking.is_top_paying_company
        , upsell_tracking.customer_quarter_cohort
        , upsell_tracking.segment
        , upsell_tracking.industry
        , upsell_tracking.institution_type
        , upsell_tracking.number_of_employees
        , upsell_tracking.order_date
        , upsell_tracking.num_orders
        , upsell_tracking.list_sales_orders
        , upsell_tracking.num_items
        , upsell_tracking.is_upsell
        , upsell_tracking.upsell_date
        , upsell_tracking.total_amount
        , sum(iff(upsell_tracking.order_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , upsell_tracking.num_orders
            , 0))
            over (partition by upsell_tracking.full_site, yr
                order by upsell_tracking.dte)                                   as order_count
        , sum(iff(upsell_tracking.upsell_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , 1
            , 0))
            over (partition by upsell_tracking.full_site, yr
                order by upsell_tracking.dte)                                   as upsell_count
        , sum(iff(upsell_tracking.order_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , total_amount
            , 0))
            over (partition by upsell_tracking.full_site, yr
                order by upsell_tracking.dte)                                   as cumulative_order_amt
        , sum(iff(upsell_tracking.upsell_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , total_amount
            , 0))
            over (partition by upsell_tracking.full_site, yr
                order by upsell_tracking.dte)                                   as cumulative_upsell_amt
--         , cumulative_upsell_amt * 1.25                                          as target_amt
    from
        upsell_tracking
    )
   , target_values as (
    select
        date_trunc('week', cumulative_upsells.dte)                              as wk
        , weekiso(cumulative_upsells.dte)                                       as wk_num
        , iff(year(wk) =
              year(dateadd(year, -1, current_date))
            , max(cumulative_upsells.cumulative_upsell_amt)
            , null)                                                             as cumulative_upsell_amount
        , cumulative_upsell_amount * 1.25                                       as target_amount
    from
        cumulative_upsells
    group by
        1
        , 2
    )
    , final as (
    select
        full_site
        , company_name
        , date_trunc('week', dte)                                               as wk
        , weekiso(dte)                                                          as wk_num
        , yr
        , coalesce(is_top_paying_company, false)                                as is_top_paying_company
        , customer_quarter_cohort
        , segment
        , industry
        , institution_type
        , number_of_employees
        , target_values.target_amount
        , max(date_trunc('week', order_date))                                   as order_wk
        , max(date_trunc('week', upsell_date))                                  as upsell_wk
        , sum(num_orders)                                                       as number_of_orders
        , sum(total_amount)                                                     as total_amt
        , listagg(list_sales_orders, ', ')                                      as sales_order_numbers
        , sum(num_items)                                                        as number_of_items
        , max(order_count)                                                      as order_count
        , max(cumulative_order_amt)                                             as cumulative_order_amt
        , max(upsell_count)                                                     as upsell_count
        , max(cumulative_upsell_amt)                                            as cumulative_upsell_amt
    from
        cumulative_upsells
    left join target_values
        on weekiso(cumulative_upsells.dte) = target_values.wk_num
    where target_values.target_amount is not null
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
    )
select * from final