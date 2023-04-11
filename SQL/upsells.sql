/*
Tracks every upsold customer's EC ordering over 365 days from CR order date.
Cohorts are based on the quarter of the CR order.

Need to add netsuite bookings data for top paying customers.
 */

-- use database da_prod_db;
-- use schema analyst_reporting;
--
-- create or replace view vw_upsells_weekly as

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


   , bookings as (
    select
        initcap(fact_booking.shipping_attention)                                as customer
        , initcap(fact_booking.end_customer)                                    as company_name
        , top_paying_companies.is_top_paying_company
        , try_cast(fact_booking.sales_order_number_st as int)                   as buck_order_id
        , fact_booking.date                                                     as order_date
        , fact_booking.date_shipped                                             as ship_date
        , sum(fact_booking.net_amount_in_usd)                                   as net_amount
    from
        da_prod_db.datacore.fact_booking
        left join top_paying_companies
            on fact_booking.end_customer = top_paying_companies.company_name
    group by
        1
        , 2
        , 3
        , 4
        , 5
        , 6
    )
   , customer_orders as (
    /*
     We define the order type as CR or EC. We also add in relevant account info.
     */
    select
        fbk.customer
        , boo.id                                                                as buck_order_id
        , bod.id                                                                as buck_delivery_group_id
        , sfo.id                                                                as salesforce_order_id
        , fbk.company_name
        , fbk.is_top_paying_company
        , mode(sfa.id) over (partition by fbk.company_name)                     as salesforce_account_id
        , mode(sfa.name) over (partition by fbk.company_name)                   as salesforce_account_name
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
        , fbk.net_amount
        , min(fbk.order_date) over (partition by customer)                      as first_order_at_pst
        , min(iff(upper(bpp.family) = 'CR'
                  , fbk.order_date
                  , null)) over (partition by customer)                         as first_cr_ordered_at_pst
        , min(iff(upper(bpp.family) = 'EC'
                  , fbk.order_date
                  , null)) over (partition by customer)                         as first_ec_ordered_at_pst
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
   , customer_daily_rollup as (
    select
        customer_orders.customer
        , customer_orders.company_name
        , customer_orders.is_top_paying_company
        , to_date(customer_orders.order_date)                                   as order_date
        , customer_orders.segment
        , customer_orders.industry
        , customer_orders.institution_type
        , customer_orders.number_of_employees
        , customer_orders.first_order_at_pst
        , customer_orders.first_cr_ordered_at_pst
        , customer_orders.first_ec_ordered_at_pst
        , year(customer_orders.first_ec_ordered_at_pst)                         as ec_purhase_yr
        , max(customer_orders.order_type)                                       as order_type
        , count(customer_orders.buck_order_id)                                  as number_of_orders
        , listagg(customer_orders.buck_order_id, ', ')                          as list_sales_orders
        , sum(customer_orders.number_of_items_in_order)                         as number_of_items
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
    )
   , customer_spine as (
    select
        to_date(dim_dates.date)                                                 as dte
        , cust.customer
        , ord.is_top_paying_company
        , ord.company_name
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
        , cust.ec_purhase_yr
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
                customer_daily_rollup.customer
--                 , customer_daily_rollup.company_name
--                 , customer_daily_rollup.is_top_paying_company
--                 , customer_daily_rollup.segment
--                 , customer_daily_rollup.industry
--                 , customer_daily_rollup.institution_type
--                 , customer_daily_rollup.number_of_employees
                , customer_daily_rollup.first_order_at_pst
                , customer_daily_rollup.first_cr_ordered_at_pst
                , customer_daily_rollup.first_ec_ordered_at_pst
                , customer_daily_rollup.ec_purhase_yr
            from
                customer_daily_rollup
            ) as cust
        left join (
            select distinct
                customer_daily_rollup.customer
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
            and cust.customer = ord.customer
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
        , customer_spine.customer
--         , customer_spine.is_top_paying_company
        , coalesce(is_top_paying_company, lag(is_top_paying_company)
            ignore nulls over (partition by customer order by dte))             as is_top_paying_company
        , customer_spine.customer_quarter_cohort
--         , customer_spine.company_name
        , coalesce(company_name, lag(company_name) ignore nulls over
            (partition by customer order by dte))                               as current_company
--         , customer_spine.industry
        , coalesce(industry, lag(industry) ignore nulls over
            (partition by customer order by dte))                               as company_industry
--         , customer_spine.institution_type
        , coalesce(institution_type, lag(institution_type) ignore nulls over
            (partition by customer order by dte))                               as company_institution_type
--         , customer_spine.number_of_employees
        , coalesce(number_of_employees, lag(number_of_employees) ignore nulls
            over (partition by customer order by dte))                          as company_size
        , customer_spine.order_date
--         , customer_spine.first_cr_ordered_at_pst
--         , customer_spine.order_type
        , customer_spine.num_orders
        , customer_spine.list_sales_orders
        , customer_spine.num_items
        , case
              when customer_spine.order_type = 'EC'
              and customer_spine.first_ec_ordered_at_pst >=
                  customer_spine.first_cr_ordered_at_pst
              and year(customer_spine.order_date) = ec_purhase_yr
                  then true
              else false
          end                                                                   as is_upsell
        , iff(is_upsell, customer_spine.dte, null)                              as upsell_date
        , customer_spine.total_amount
--     , conditional_true_event((customer_spine.dte <
--                                  to_date(customer_spine.first_ec_ordered_at)
--                               or customer_spine.first_ec_ordered_at is null)
--             and customer_spine.dte >=
--                 to_date(customer_spine.first_cr_ordered_at)) over
--             (partition by customer_spine.account_id
--                 order by customer_spine.dte)                                      as num_days_before_upsell
    from
        customer_spine
    )
   , cumulative_upsells as (
    select
        upsell_tracking.dte
        , year(upsell_tracking.dte)                                             as yr
        , upsell_tracking.customer
        , upsell_tracking.is_top_paying_company
        , upsell_tracking.customer_quarter_cohort
        , upsell_tracking.current_company
        , upsell_tracking.company_industry
        , upsell_tracking.company_institution_type
        , upsell_tracking.company_size
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
            over (partition by upsell_tracking.customer, yr
                order by upsell_tracking.dte)                                   as order_count
        , sum(iff(upsell_tracking.upsell_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , 1
            , 0))
            over (partition by upsell_tracking.customer, yr
                order by upsell_tracking.dte)                                   as upsell_count
        , sum(iff(upsell_tracking.order_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , total_amount
            , 0))
            over (partition by upsell_tracking.customer, yr
                order by upsell_tracking.dte)                                   as cumulative_order_amt
        , sum(iff(upsell_tracking.upsell_date is not null
                  /*and upsell_tracking.upsell_date >= '2023-01-01'*/
            , total_amount
            , 0))
            over (partition by upsell_tracking.customer, yr
                order by upsell_tracking.dte)                                   as cumulative_upsell_amt
--         , sum(iff(upsell_tracking.order_date is not null
--                   /*and upsell_tracking.upsell_date >= '2023-01-01'*/, 1, 0))
--             over (partition by upsell_tracking.customer, yr
--                 order by upsell_tracking.dte)                                   as order_count
--         , sum(iff(upsell_tracking.upsell_date is not null
--                   /*and upsell_tracking.upsell_date >= '2023-01-01'*/, 1, 0))
--             over (partition by upsell_tracking.customer, yr
--                 order by upsell_tracking.dte)                                   as upsell_count
--         , sum(iff(upsell_tracking.upsell_date is not null
--                   /*and upsell_tracking.upsell_date >= '2023-01-01'*/
--             , total_amount
--             , 0))
--             over (partition by upsell_tracking.customer, yr
--                 order by upsell_tracking.dte)                                   as upsell_amt
    from
        upsell_tracking
    )
    , final as (
    select
        customer
        , date_trunc('week', dte)            as wk
        , weekiso(dte)                       as wk_num
        , yr
        , current_company
        , is_top_paying_company
        , customer_quarter_cohort
        , company_industry
        , company_institution_type
        , company_size
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
        , 10
    )
select * from final
