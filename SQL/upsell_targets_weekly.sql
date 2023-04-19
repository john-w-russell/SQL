/*
Tracks every upsold customer's EC ordering over 365 days from CR order date.
Cohorts are based on the quarter of the CR order.
 */

use database da_prod_db;
use schema analyst_reporting;

create or replace view vw_upsell_targets_weekly as

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
        coalesce(fb.buck_sales_order_id
            , try_cast(so.buck_id__c as int)
            , foi.barb_buck_order_id)                                           as buck_order_id
        , to_date(fb.date)                                                      as order_date
        , fb.commerce_id
        , fb.item_name
        , fb.item_product_line
        , fb.item_product_family
        , initcap(fb.shipping_attention)                                        as customer_name
        , initcap(fb.end_customer)                                              as company_name
        , coalesce(top_paying_companies.is_top_paying_company, false)           as is_top_paying_company
        , fb.net_amount_in_usd
    from
        datacore.fact_booking                                                   fb
        left join (
            select
                max(foi2.barb_buck_order_id)                                    as barb_buck_order_id
                , max(foi2.salesforce_sales_order_id)                           as salesforce_sales_order_id
                , foi2.barb_sales_order_item_commerce_id
            from
                datacore.fact_order_items                                       foi2
            group by
                3
            ) as                                                                foi
            on foi.barb_sales_order_item_commerce_id = fb.commerce_id
        left join stitch.stitch_salesforce_prod."ORDER"                         so
            on so.id = foi.salesforce_sales_order_id
        left join top_paying_companies
            on fb.end_customer = top_paying_companies.company_name
    where
        fb.net_amount_in_usd is not null
        and fb.item_product_family in ('CRISPRevolution', 'Engineered Cells')
        and fb.date >= '2020-01-01'
--         and fb.date_shipped is not null
    )
   , sales_order_item_rollup as (
    select
        foi.buckaneer_sales_order_id
        , foi.barb_sales_order_item_commerce_id
        , first_value(foi.salesforce_sales_order_id) ignore nulls over
            (partition by foi.buckaneer_sales_order_id
            order by foi.full_site)                                             as salesforce_order_id
        , first_value(foi.salesforce_account_id) ignore nulls over
            (partition by foi.buckaneer_sales_order_id
            order by foi.full_site)                                             as salesforce_account_id
        , first_value(foi.full_site) ignore nulls over
            (partition by foi.buckaneer_sales_order_id
            order by foi.full_site)                                             as full_site
--         , row_number() over (partition by foi.buckaneer_sales_order_id
--             order by foi.chosen_order_created_at)                               as rn
    from
        da_prod_db.datacore.fact_order_items                                    foi
--     qualify
--         rn = 1
    )
   , customer_orders as (
    /*
     We define the order type as CR or EC. We also add in relevant account info.
     */
    select
        fbk.buck_order_id
        , fbk.commerce_id
--         , fbk.customer_name
        , soi.full_site
        , fbk.company_name
        , fbk.is_top_paying_company
        , sfo.id                                                                as salesforce_order_id
        , mode(sfa.id) over (partition by fbk.company_name)                     as salesforce_account_id
        , mode(sfa.name) over (partition by fbk.company_name)                   as salesforce_account_name
        , mode(sfa.segment__c) over (partition by fbk.company_name)             as segment
        , mode(sfa.industry) over (partition by fbk.company_name)               as industry
        , mode(sfa.institution_type__c) over (partition by fbk.company_name)    as institution_type
        , mode(sfa.numberofemployees) over (partition by fbk.company_name)      as number_of_employees
        , fbk.order_date
        , iff(fbk.item_product_family = 'Engineered Cells', 'EC', 'CR')         as product_type
        , fbk.item_name
        , fbk.item_product_line
        , fbk.net_amount_in_usd
        , min(fbk.order_date) over (partition by full_site)                     as first_order_at_pst
        , min(iff(product_type = 'CR'
                  , fbk.order_date
                  , null)) over (partition by full_site)                        as first_cr_ordered_at_pst
        , min(iff(product_type = 'EC'
                  , fbk.order_date
                  , null)) over (partition by full_site)                        as first_ec_ordered_at_pst
    from
        bookings                                                                fbk
    left join sales_order_item_rollup                                           soi
        on fbk.buck_order_id = soi.buckaneer_sales_order_id
        and fbk.commerce_id = soi.barb_sales_order_item_commerce_id
    left join stitch.stitch_salesforce_prod."ORDER"                             sfo
        on fbk.buck_order_id = sfo.buck_id__c
    left join stitch.stitch_salesforce_prod.account                             sfa
        on sfo.accountid = sfa.id
    )
   , customer_daily_rollup as (
    select
        customer_orders.order_date
--         , customer_orders.customer_name
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
        , count(customer_orders.commerce_id)                                    as number_of_items
        , max(customer_orders.product_type)                                     as product_type
        , listagg(customer_orders.commerce_id || ' - ' ||
                  customer_orders.item_product_line, ', ')                      as list_sales_order_items
        , listagg(customer_orders.item_product_line, ', ')                      as list_product_lines
        , sum(customer_orders.net_amount_in_usd)                                as total_amount
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
        , ord.product_type
        , iff(ord.order_date is not null, ord.number_of_items, 0)               as num_order_items
        , ord.list_sales_order_items
        , ord.list_product_lines
        , iff(ord.order_date is not null, ord.total_amount, 0)                  as total_amount
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
                , customer_daily_rollup.product_type
                , customer_daily_rollup.number_of_items
                , customer_daily_rollup.list_sales_order_items
                , customer_daily_rollup.list_product_lines
                , customer_daily_rollup.total_amount
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
--         , coalesce(full_site, lag(full_site)
--             ignore nulls over (partition by customer_name order by dte))        as full_site
        , coalesce(company_name, lag(company_name)
            ignore nulls over (partition by full_site order by dte))            as company_name
        , coalesce(is_top_paying_company, lag(is_top_paying_company)
            ignore nulls over (partition by full_site order by dte))            as is_top_paying_company
        , customer_spine.customer_quarter_cohort
        , coalesce(segment, lag(segment) ignore nulls over
            (partition by full_site order by dte))                              as company_segment
        , coalesce(industry, lag(industry) ignore nulls over
            (partition by full_site order by dte))                              as company_industry
        , coalesce(institution_type, lag(institution_type) ignore nulls over
            (partition by full_site order by dte))                              as company_institution_type
        , coalesce(number_of_employees, lag(number_of_employees) ignore nulls
            over (partition by full_site order by dte))                         as company_size
        , customer_spine.order_date
        , customer_spine.num_order_items
        , customer_spine.list_sales_order_items
        , customer_spine.list_product_lines
        , iff(customer_spine.product_type = 'EC'
                  and customer_spine.first_ec_ordered_at_pst >=
                      customer_spine.first_cr_ordered_at_pst
                  and year(customer_spine.order_date) = ec_purchase_yr
            , true
            , false)                                                            as is_upsell
        , iff(is_upsell, customer_spine.dte, null)                              as upsell_date
        , case
              when customer_spine.first_ec_ordered_at_pst is null
                  and customer_spine.list_product_lines ilike '%gko%'
                  then true
              when customer_spine.first_ec_ordered_at_pst is not null
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
--         , upsell_tracking.customer_name
        , upsell_tracking.full_site
        , upsell_tracking.company_name
        , upsell_tracking.is_top_paying_company
        , upsell_tracking.customer_quarter_cohort
        , upsell_tracking.company_segment
        , upsell_tracking.company_industry
        , upsell_tracking.company_institution_type
        , upsell_tracking.company_size
        , upsell_tracking.order_date
        , upsell_tracking.num_order_items
        , upsell_tracking.list_sales_order_items
        , upsell_tracking.list_product_lines
        , upsell_tracking.is_upsell
        , upsell_tracking.upsell_date
        , coalesce(upsell_tracking.is_upsell_target
            , lag(upsell_tracking.is_upsell_target) ignore nulls
            over (partition by full_site order by dte))                         as is_upsell_target
        , upsell_tracking.total_amount
        , sum(iff(upsell_tracking.order_date is not null
            , upsell_tracking.num_order_items
            , 0))
            over (partition by upsell_tracking.full_site, yr
                order by upsell_tracking.dte)                                   as order_item_count
        , sum(iff(upsell_tracking.upsell_date is not null
            , 1
            , 0))
            over (partition by upsell_tracking.full_site, yr
                order by upsell_tracking.dte)                                   as upsell_count
        , sum(iff(upsell_tracking.order_date is not null
            , total_amount
            , 0))
            over (partition by upsell_tracking.full_site, yr
                order by upsell_tracking.dte)                                   as cumulative_order_amt
        , sum(iff(upsell_tracking.upsell_date is not null
            , total_amount
            , 0))
            over (partition by upsell_tracking.full_site, yr
                order by upsell_tracking.dte)                                   as cumulative_upsell_amt
    from
        upsell_tracking
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
        , company_segment
        , company_industry
        , company_institution_type
        , company_size
        , is_upsell_target
        , max(date_trunc('week', order_date))                                   as order_wk
        , max(date_trunc('week', upsell_date))                                  as upsell_wk
        , sum(num_order_items)                                                  as number_of_order_items
        , sum(total_amount)                                                     as total_amt
        , listagg(list_sales_order_items, ', ')                                 as sales_order_item_numbers
        , listagg(list_product_lines, ', ')                                     as product_lines
        , sum(num_order_items)                                                  as number_of_items
        , max(order_item_count)                                                 as order_count
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
        , 11
        , 12
    )
select * from final
-- select count(distinct full_site) from final -- 12272
-- select count(distinct full_site) from final where is_upsell_target = true --2805
-- ~23% of all full site end users are upsell targets