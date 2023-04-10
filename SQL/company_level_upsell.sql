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
        dfb.end_customer                                                        as company_name
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


   , bookings as (
    select
        fact_booking.end_customer                                               as company_name
        , top_paying_companies.is_top_paying_company
        , try_cast(fact_booking.sales_order_number_st as int)                   as buck_order_id
        , fact_booking.date                                                     as order_date
        , fact_booking.date_shipped                                             as ship_date
        , sum(fact_booking.net_amount_in_usd)                                   as net_amount
    from
        da_prod_db.datacore.fact_booking
        left join top_paying_companies
            on fact_booking.end_customer = top_paying_companies.company_name
    where
        fact_booking.date_shipped is not null
    group by
        1
        , 2
        , 3
        , 4
        , 5
    )
   , company_orders as (
    /*
     We define the order type as CR or EC. We also add in relevant account info.
     */
    select
        fbk.company_name
        , fbk.is_top_paying_company
        , fbk.buck_order_id
        , bod.id                                                                as buck_delivery_group_id
        , sfo.id                                                                as salesforce_order_id
        , mode(sfa.id) over
            (partition by fbk.company_name)                                     as salesforce_account_id
        , mode(sfa.name) over
            (partition by fbk.company_name)                                     as salesforce_account_name
        , mode(sfa.segment__c) over
            (partition by fbk.company_name)                                     as segment
        , mode(sfa.industry) over
            (partition by fbk.company_name)                                     as industry
        , mode(sfa.institution_type__c) over
            (partition by fbk.company_name)                                     as institution_type
        , mode(sfa.numberofemployees) over
            (partition by fbk.company_name)                                     as number_of_employees
        , boo.status                                                            as order_status
        , fbk.order_date
        , fbk.ship_date
        , upper(bpp.family)                                                     as order_type
        , fbk.net_amount                                                        as order_amount
        , min(fbk.order_date) over (partition by fbk.company_name)              as first_order_at_pst
        , min(iff(upper(bpp.family) = 'CR'
                  , fbk.order_date
                  , null)) over (partition by fbk.company_name)                 as first_cr_ordered_at_pst
        , min(iff(upper(bpp.family) = 'EC'
                  , fbk.order_date
                  , null)) over (partition by fbk.company_name)                 as first_ec_ordered_at_pst
        , count(distinct coalesce(boi.parent_ordered_item_id, boi.id)) over
        (partition by boo.id)                                                   as number_of_items_in_order
        , row_number() over (partition by boo.id
        order by fbk.order_date desc)                                           as rn
    from
        bookings                                                                fbk
    left join stitch.stitch_buckaneer_prod.order_order                          boo
        on fbk.buck_order_id = boo.id
        and boo.status not in ('cart', 'canceled', 'declined', 'new')
    left join stitch.stitch_buckaneer_prod.order_deliverygroup                  bod
        on boo.id = bod.order_id
    left join stitch.stitch_buckaneer_prod.order_ordereditem                    boi
        on bod.id = boi.delivery_group_id
    left join stitch.stitch_buckaneer_prod.product_product                      bpp
        on boi.product_id = bpp.id
        and lower(bpp.name) not like '%shipping%' -- exclude shipping addon charges
    left join stitch.stitch_buckaneer_prod.userprofile_address bua
        on boo.shipping_address_id = bua.id
    left join datacore.fact_netsuite_order_items                                noi
        on boo.id = noi.external_id
    left join stitch.stitch_salesforce_prod."ORDER"                             sfo
        on boo.id = sfo.buck_id__c
    left join stitch.stitch_salesforce_prod.account                             sfa
        on sfo.accountid = sfa.id
    where
        bpp.family in ('ec', 'cr')
        qualify
            rn = 1
    )
   , company_daily_rollup as (
    select
        company_orders.company_name
        , company_orders.is_top_paying_company
        , to_date(company_orders.order_date)                                    as order_date
        , company_orders.segment
        , company_orders.industry
        , company_orders.institution_type
        , company_orders.number_of_employees
        , company_orders.first_order_at_pst
        , company_orders.first_cr_ordered_at_pst
        , company_orders.first_ec_ordered_at_pst
        , max(company_orders.order_type)                                        as order_type
        , count(company_orders.buck_order_id)                                   as number_of_orders
        , listagg(company_orders.buck_order_id, ', ')                           as list_sales_orders
        , sum(company_orders.number_of_items_in_order)                          as number_of_items
        , sum(company_orders.order_amount)                                      as total_order_amount
    from
        company_orders
    where
        company_orders.first_order_at_pst >= '2020-01-01'
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
    )
   , company_spine as (
    select
        cust.company_name
        , to_date(dim_dates.date)                                               as dte
        , cust.is_top_paying_company
        , to_varchar(date_part(year
        , cust.first_order_at_pst))
        ||
          ' Q'
        ||
          to_varchar(date_part(quarter
              , cust.first_order_at_pst))                                       as company_quarter_cohort
        , cust.segment
        , cust.industry
        , cust.institution_type
        , cust.number_of_employees
        , cust.first_cr_ordered_at_pst
        , cust.first_ec_ordered_at_pst
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
                company_daily_rollup.company_name
                , company_daily_rollup.is_top_paying_company
                , company_daily_rollup.segment
                , company_daily_rollup.industry
                , company_daily_rollup.institution_type
                , company_daily_rollup.number_of_employees
                , company_daily_rollup.first_order_at_pst
                , company_daily_rollup.first_cr_ordered_at_pst
                , company_daily_rollup.first_ec_ordered_at_pst
            from
                company_daily_rollup
            ) as cust
        left join (
            select distinct
                company_daily_rollup.company_name
                , company_daily_rollup.order_date
                , company_daily_rollup.order_type
                , company_daily_rollup.number_of_orders
                , company_daily_rollup.number_of_items
                , company_daily_rollup.list_sales_orders
                , company_daily_rollup.total_order_amount
            from
                company_daily_rollup
            ) as ord
            on to_date(dim_dates.date) = ord.order_date
            and cust.company_name = ord.company_name
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
        company_spine.company_name
        , company_spine.dte
        , company_spine.is_top_paying_company
        , company_spine.company_quarter_cohort
        , company_spine.industry
        , company_spine.institution_type
        , company_spine.number_of_employees
        , company_spine.order_date
--         , company_spine.first_cr_ordered_at_pst
        , company_spine.order_type
        , company_spine.num_orders
        , company_spine.list_sales_orders
        , company_spine.num_items
--         , company_spine.first_cr_ordered_at_pst
--         , company_spine.first_ec_ordered_at_pst
--         , datediff(year, company_spine.first_ec_ordered_at_pst
--                           , company_spine.first_cr_ordered_at_pst)              as yr_gap
        , case
              when company_spine.order_type = 'EC'
              and company_spine.first_ec_ordered_at_pst >=
                  company_spine.first_cr_ordered_at_pst
                  then true
              when company_spine.order_type = 'EC'
              and datediff(month, company_spine.first_ec_ordered_at_pst
                          , company_spine.first_cr_ordered_at_pst) > 24
              and company_spine.order_date >=
                  company_spine.first_cr_ordered_at_pst
                  then true
              else false
          end                                                                   as is_upsell
        , iff(is_upsell, company_spine.dte, null)                               as upsell_date
        , company_spine.total_amount
--     , conditional_true_event((customer_spine.dte <
--                                  to_date(customer_spine.first_ec_ordered_at)
--                               or customer_spine.first_ec_ordered_at is null)
--             and customer_spine.dte >=
--                 to_date(customer_spine.first_cr_ordered_at)) over
--             (partition by customer_spine.account_id
--                 order by customer_spine.dte)                                      as num_days_before_upsell
    from
        company_spine
    )
   , cumulative_upsells as (
    select
        upsell_tracking.company_name
        , upsell_tracking.dte
        , year(upsell_tracking.dte)                                             as yr
        , upsell_tracking.is_top_paying_company
        , upsell_tracking.company_quarter_cohort
        , upsell_tracking.industry
        , upsell_tracking.institution_type
        , upsell_tracking.number_of_employees
        , upsell_tracking.order_date
        , upsell_tracking.order_type
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
            over (partition by upsell_tracking.company_name, yr
                order by upsell_tracking.dte)                                   as order_count
        , sum(iff(upsell_tracking.upsell_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , 1
            , 0))
            over (partition by upsell_tracking.company_name, yr
                order by upsell_tracking.dte)                                   as upsell_count
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
    from
        upsell_tracking
    )
    , final as (
    select
        company_name
        , date_trunc('week', dte)                                               as wk
        , weekiso(dte)                                                          as wk_num
        , yr
        , is_top_paying_company
        , company_quarter_cohort
        , industry
        , institution_type
        , number_of_employees
        , max(date_trunc('week', order_date))                                   as order_wk
        , max(date_trunc('week', upsell_date))                                  as upsell_wk
        , sum(num_orders)                                                       as number_of_orders
        , sum(total_amount)                                                     as total_amt
        , listagg(list_sales_orders, ', ')                                      as sales_order_numbers
        , sum(num_items)                                                        as number_of_items
--     , is_upsold
        , max(order_count)                                                      as order_count
        , max(cumulative_order_amt)                                             as cumulative_order_amt
        , max(upsell_count)                                                     as upsell_count
        , max(cumulative_upsell_amt)                                            as cumulative_upsell_amt
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
    )
select * from final where company_name = 'Sanofi' order by wk