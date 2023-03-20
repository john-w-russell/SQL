/*
Tracks every account where the first order is a CR-based order from the day of the first order to current date.
Cohorts are based on the quarter of the first order made.

Need to add netsuite bookings data for top paying customers.
 */

use database da_prod_db;
use schema analyst_reporting;

create or replace view vw_upsells_account_daily_top_companies as

with number_of_items as (
    /*
     Start with defining the top companies from 2022. To do this, we find the total number
     of items sold in 2022 and create an ordered list of accounts by items sold. The account
     is considered a top account if the cumulative sum of items sold is less than the cutoff value.
     */
    select
        count(distinct fsoi.sales_order_item_commerce_id)                       as num_items
        , round(num_items * 0.8)                                                as cutoff_num
    from
        datacore.fact_sales_order_items                                         fsoi
    where
        fsoi.sales_order_created >= '2022-01-01'
        and fsoi.sales_order_created < '2023-01-01'
        and fsoi.chosen_order_actual_shipped_at is not null
        and fsoi.salesforce_sales_order_id is not null
    )
, account_orders as (
    select
        fsoi.sales_order_id
        , fsoi.salesforce_sales_order_id
        , sfo.accountid                                                         as account_id
        , fsoi.sales_order_status
        , convert_timezone('America/Los_Angeles',
                           fsoi.chosen_order_created_at)                        as order_created_at_pst
        , convert_timezone('America/Los_Angeles',
                           fsoi.chosen_order_actual_shipped_at)                 as order_shipped_at_pst
        , iff(fsoi.company_name like 'Cells Synthego'
                  and fsoi.source_system = 'LIMS'
        , true
        , false)                                                                as is_internal
        , count(distinct fsoi.sales_order_item_commerce_id) over
            (partition by fsoi.source_order_reference)                          as number_of_items_in_order
        , row_number() over (partition by fsoi.sales_order_id
                        order by fsoi.sales_order_created desc)                 as rn
    from
        da_prod_db.datacore.fact_sales_order_items                              fsoi
        left join stitch.stitch_salesforce_prod."ORDER"                         sfo
            on fsoi.salesforce_sales_order_id = sfo.id
    where
        fsoi.salesforce_sales_order_id is not null
        and not is_internal
        and fsoi.sales_order_created >= '2022-01-01'
        and fsoi.sales_order_created < '2023-01-01'
        qualify
            rn = 1
    )
, account_order_rollup as (
    select
        sfa.id                                                                  as account_id
        , sfa.name                                                              as account_name
        , sfa.createddate                                                       as account_created_at
        , sfa.billingcity                                                       as account_billing_city
        , sfa.billingstate                                                      as account_billing_state
        , sfa.billingcountry                                                    as account_billing_country
        , sfa.industry
        , sfa.institution_type__c                                               as account_institution_type
        , sfa.numberofemployees
        , iff(sfa.parentid is not null, true, false)                            as has_parent_account
        , iff(sfa.parentid is not null
        , sfa.parentid
        , null)                                                                 as parent_account_id
        , iff(sfa.parentid is not null
        , sfa.parent_account_name__c
        , null)                                                                 as parent_account_name
        , count(account_orders.sales_order_id)                                  as num_total_orders
        , sum(account_orders.number_of_items_in_order)                          as num_items_sold
    from
        stitch.stitch_salesforce_prod.account                                   sfa
        left join account_orders
            on sfa.id = account_orders.account_id
    where
        not sfa.isdeleted
        and sfa.id in (
        select distinct
            account_orders.account_id
        from
            account_orders
        )
    group by
        1,2,3,4,5,6,7,8,9,10,11,12,13
    )
, top_paying_accounts as (
    select
        account_order_rollup.account_id
        , account_order_rollup.account_name
        , account_order_rollup.account_created_at
        , account_order_rollup.account_billing_city
        , account_order_rollup.account_billing_state
        , account_order_rollup.account_billing_country
        , account_order_rollup.industry
        , account_order_rollup.account_institution_type
        , account_order_rollup.numberofemployees
        , account_order_rollup.has_parent_account
        , account_order_rollup.parent_account_id
        , account_order_rollup.parent_account_name
        , account_order_rollup.num_total_orders
        , account_order_rollup.num_items_sold
        , number_of_items.cutoff_num
        , sum(account_order_rollup.num_items_sold) over
        (order by account_order_rollup.num_items_sold desc)                  as running_item_count
        , iff(running_item_count <= number_of_items.cutoff_num, true,
              false)                                                         as is_top_paying_account
    from
        account_order_rollup
        join number_of_items
            on true
    )

   , categorized_orders as (
    /*
     We define the order type as CR or EC.
     */
    select
        fsoi.sales_order_id
        , fsoi.salesforce_sales_order_id
        , sfo.accountid                                                         as account_id
        , fsoi.sales_order_status
        , convert_timezone('America/Los_Angeles',
                           fsoi.chosen_order_created_at)                        as order_created_at_pst
        , convert_timezone('America/Los_Angeles',
                           fsoi.chosen_order_actual_shipped_at)                 as order_shipped_at_pst
        , upper(fsoi.factory_label)                                             as order_type
        , iff(fsoi.company_name like 'Cells Synthego'
                  and fsoi.source_system = 'LIMS'
        , true
        , false)                                                                as is_internal
        , first_value(upper(fsoi.factory_label)) over
            (partition by sfo.accountid
            order by fsoi.chosen_order_created_at)                              as first_order_type
        , min(iff(upper(fsoi.factory_label) = 'CR'
            , order_created_at_pst
            , null)) over (partition by sfo.accountid)                          as first_cr_ordered_at_pst
        , min(iff(upper(fsoi.factory_label) = 'EC'
            , order_created_at_pst
            , null)) over (partition by sfo.accountid)                          as first_ec_ordered_at_pst
        , count(distinct fsoi.sales_order_item_commerce_id) over
            (partition by fsoi.source_order_reference)                          as number_of_items_in_order
        , row_number() over (partition by fsoi.sales_order_id
                        order by fsoi.sales_order_created desc)                 as rn
    from
        da_prod_db.datacore.fact_sales_order_items                              fsoi
        left join stitch.stitch_salesforce_prod."ORDER"                         sfo
            on fsoi.salesforce_sales_order_id = sfo.id
    where
        fsoi.salesforce_sales_order_id is not null
        and not is_internal
        and fsoi.sales_order_created >= '2020-01-01'
        qualify
            rn = 1
            and first_order_type = 'CR'
    )
, account_spine as (
    select
        sfa.id                                                                  as account_id
        , dim_dates.date                                                        as dte
        , sfa.name                                                              as account_name
        , 'https://synthego.lightning.force.com/lightning/r/Account/' ||
          sfa.id ||
          '/view'                                                               as salesforce_url
        , sfa.createddate                                                       as account_created_at
        , sfa.billingcity                                                       as account_billing_city
        , sfa.billingstate                                                      as account_billing_state
        , sfa.billingcountry                                                    as account_billing_country
        , sfa.industry
        , sfa.institution_type__c                                               as account_institution_type
        , sfa.numberofemployees
        , iff(sfa.parentid is not null, true, false)                            as has_parent_account
        , iff(sfa.parentid is not null
        , sfa.parentid
        , null)                                                                 as parent_account_id
        , iff(sfa.parentid is not null
        , sfa.parent_account_name__c
        , null)                                                                 as parent_account_name
    from
        stitch.stitch_salesforce_prod.account                                   sfa
        cross join da_prod_db.analyst_reporting.dim_dates
    where
        not sfa.isdeleted
        and sfa.id in (
        select distinct
            top_paying_accounts.account_id
        from
            top_paying_accounts
        )
    )
, accounts_with_orders as (
    /*
     Join the orders to the account that placed.
     */
    select
        account_spine.dte
        , account_spine.account_id
        , account_spine.account_name
        , account_spine.salesforce_url
        , account_spine.account_created_at
        , account_spine.account_billing_city
        , account_spine.account_billing_state
        , account_spine.account_billing_country
        , account_spine.industry
        , account_spine.account_institution_type
        , account_spine.numberofemployees
        , account_spine.has_parent_account
        , account_spine.parent_account_id
        , account_spine.parent_account_name
        , categorized_orders.sales_order_id
        , categorized_orders.salesforce_sales_order_id
        , categorized_orders.order_created_at_pst
        , categorized_orders.order_type
        , categorized_orders.order_shipped_at_pst
        , categorized_orders.sales_order_status
        , categorized_orders.first_cr_ordered_at_pst
        , categorized_orders.first_ec_ordered_at_pst
        , categorized_orders.number_of_items_in_order
    from
        account_spine
        left join categorized_orders
            on account_spine.account_id = categorized_orders.account_id
            and to_date(account_spine.dte) = to_date(categorized_orders.order_created_at_pst)
    )
, accounts_daily as (
    /*
     Create a data model with one row per day per account.
     */
    select
        accounts_with_orders.dte
        , accounts_with_orders.account_id
        , accounts_with_orders.account_name
        , accounts_with_orders.salesforce_url
        , accounts_with_orders.account_created_at
        , accounts_with_orders.account_billing_city
        , accounts_with_orders.account_billing_state
        , accounts_with_orders.account_billing_country
        , accounts_with_orders.industry
        , accounts_with_orders.account_institution_type
        , accounts_with_orders.numberofemployees
        , accounts_with_orders.has_parent_account
        , accounts_with_orders.parent_account_id
        , accounts_with_orders.parent_account_name
        , accounts_with_orders.sales_order_id
        , accounts_with_orders.salesforce_sales_order_id
        , accounts_with_orders.order_created_at_pst
        , accounts_with_orders.order_type
        , accounts_with_orders.order_shipped_at_pst
        , accounts_with_orders.sales_order_status
        , accounts_with_orders.number_of_items_in_order
        , first_value(accounts_with_orders.first_cr_ordered_at_pst ignore nulls)
            over (partition by accounts_with_orders.account_id
                  order by accounts_with_orders.dte)                            as first_cr_ordered_at
        , first_value(accounts_with_orders.first_ec_ordered_at_pst ignore nulls)
            over (partition by accounts_with_orders.account_id
                  order by accounts_with_orders.dte)                            as first_ec_ordered_at
    from
        accounts_with_orders
    )
, daily_rollup as (
    /*
    Aggregates order information to remove duplicate dates.
     */
    select
        accounts_daily.dte
        , accounts_daily.account_id
        , accounts_daily.account_name
        , accounts_daily.salesforce_url
        , accounts_daily.account_created_at
        , to_varchar(date_part(year
        , accounts_daily.first_cr_ordered_at))
        ||
          ' Q'
        ||
          to_varchar(date_part(quarter
              , accounts_daily.first_cr_ordered_at))                            as customer_cohort
        , accounts_daily.account_billing_city
        , accounts_daily.account_billing_state
        , accounts_daily.account_billing_country
        , accounts_daily.industry
        , accounts_daily.account_institution_type
        , accounts_daily.numberofemployees
        , accounts_daily.has_parent_account
        , accounts_daily.parent_account_id
        , accounts_daily.parent_account_name
        , accounts_daily.first_cr_ordered_at
        , accounts_daily.first_ec_ordered_at
        , count(accounts_daily.order_created_at_pst)                            as num_orders_placed
        , count(case when accounts_daily.order_type = 'CR'
            then accounts_daily.order_created_at_pst end)                       as num_cr_orders_placed
        , coalesce(sum(accounts_daily.number_of_items_in_order), 0)             as num_items_ordered

    from
        accounts_daily
    group by
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
    )
, upsell_tracking as (
    /*
    Measures upsell metric per account.
     */
select
    daily_rollup.dte
    , daily_rollup.account_id
    , daily_rollup.account_name
    , daily_rollup.salesforce_url
    , daily_rollup.account_created_at
    , daily_rollup.customer_cohort
    , daily_rollup.account_billing_city
    , daily_rollup.account_billing_state
    , daily_rollup.account_billing_country
    , daily_rollup.industry
    , daily_rollup.account_institution_type
    , daily_rollup.numberofemployees
    , top_paying_accounts.is_top_paying_account
    , daily_rollup.has_parent_account
    , daily_rollup.parent_account_id
    , daily_rollup.parent_account_name
    , daily_rollup.num_orders_placed
    , daily_rollup.num_cr_orders_placed
    , daily_rollup.num_items_ordered
    , daily_rollup.first_cr_ordered_at
    , iff(daily_rollup.dte >= daily_rollup.first_ec_ordered_at
            , true
            , false)                                                            as is_upsold
    , daily_rollup.first_ec_ordered_at                                          as upsold_at
    , sum(case when daily_rollup.dte < daily_rollup.first_ec_ordered_at
                or daily_rollup.first_ec_ordered_at is null
                    then daily_rollup.num_cr_orders_placed end) over
            (partition by daily_rollup.account_id)                              as num_cr_orders_before_ec_order
    , conditional_true_event((daily_rollup.dte <
                                 to_date(daily_rollup.first_ec_ordered_at)
                              or daily_rollup.first_ec_ordered_at is null)
            and daily_rollup.dte >=
                to_date(daily_rollup.first_cr_ordered_at)) over
            (partition by daily_rollup.account_id
                order by daily_rollup.dte)                                      as num_days_before_upsell
from
    daily_rollup
    join top_paying_accounts
        on daily_rollup.account_id = top_paying_accounts.account_id
)
select
    *
from
    upsell_tracking
where
    dte >= to_date(first_cr_ordered_at)
    and dte <= current_date()