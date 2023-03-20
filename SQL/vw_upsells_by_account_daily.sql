/*
Tracks every account where the first order is a CR-based order from the day of the first order to current date.
Cohorts are based on the quarter of the first order made.

Need to add netsuite bookings data for top paying customers.
 */

-- use database da_prod_db;
-- use schema analyst_reporting;
--
-- create or replace view vw_upsells_by_account_daily as

with categorized_orders as (
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
        , row_number() over (partition by fsoi.sales_order_id
        order by fsoi.sales_order_created desc)                                 as rn
    from
        da_prod_db.datacore.fact_sales_order_items                              fsoi
        left join stitch.stitch_salesforce_prod."ORDER"                         sfo
            on fsoi.salesforce_sales_order_id = sfo.id
    where
        fsoi.salesforce_sales_order_id is not null
        and not is_internal
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
--         , to_varchar(date_part(year, sfa.createddate))
--         ||
--           ' Q'
--         ||
--           to_varchar(date_part(quarter, sfa.createddate))                       as customer_cohort
        , sfa.accountsource
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
        stitch.stitch_salesforce_prod.account sfa
        cross join da_prod_db.analyst_reporting.dim_dates
    where
        not sfa.isdeleted
        and sfa.id in (
        select distinct
            categorized_orders.account_id
        from
            categorized_orders
        )
--         and to_date(dim_dates.date) >= to_date(sfa.createddate)
    )
   , accounts_with_orders as (
    select
        account_spine.dte
        , account_spine.account_id
        , account_spine.account_name
        , account_spine.salesforce_url
        , account_spine.account_created_at
--         , account_spine.customer_cohort
        , account_spine.accountsource
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
    from
        account_spine
        left join categorized_orders
            on account_spine.account_id = categorized_orders.account_id
            and to_date(account_spine.dte) = to_date(categorized_orders.order_created_at_pst)
    )
, accounts_daily as (
    select
        accounts_with_orders.dte
        , accounts_with_orders.account_id
        , accounts_with_orders.account_name
        , accounts_with_orders.salesforce_url
        , accounts_with_orders.account_created_at
--         , accounts_with_orders.customer_cohort
        , accounts_with_orders.accountsource
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
--         , iff(accounts_with_orders.first_ec_ordered_at_pst is not null
--             , true
--             , false)                                                            as is_upsold
        , first_value(accounts_with_orders.first_cr_ordered_at_pst ignore nulls)
            over (partition by accounts_with_orders.account_id
                  order by accounts_with_orders.dte)                            as first_cr_ordered_at
        , first_value(accounts_with_orders.first_ec_ordered_at_pst ignore nulls)
            over (partition by accounts_with_orders.account_id
                  order by accounts_with_orders.dte)                            as first_ec_ordered_at
--         , accounts_with_orders.first_ec_ordered_at_pst                                 as upsold_at
--         , conditional_true_event((account_daily.dte <
--                                  to_date(account_daily.first_ec_ordered_at_pst)
--             and account_daily.dte >=
--                 to_date(account_daily.first_cr_ordered_at_pst)) over
--             (partition by account_daily.account_id
--                 order by account_daily.dte)                                     as num_days_before_upsell
    from
        accounts_with_orders
    )
, upsell_tracking as (
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
--     , accounts_daily.customer_cohort
    , accounts_daily.accountsource
    , accounts_daily.account_billing_city
    , accounts_daily.account_billing_state
    , accounts_daily.account_billing_country
    , accounts_daily.industry
    , accounts_daily.account_institution_type
    , accounts_daily.numberofemployees
    , accounts_daily.has_parent_account
    , accounts_daily.parent_account_id
    , accounts_daily.parent_account_name
    , accounts_daily.sales_order_id
    , accounts_daily.salesforce_sales_order_id
    , accounts_daily.order_created_at_pst
    , accounts_daily.order_type
    , accounts_daily.order_shipped_at_pst
    , accounts_daily.sales_order_status
    , accounts_daily.first_cr_ordered_at
    , iff(accounts_daily.dte >= accounts_daily.first_ec_ordered_at
            , true
            , false)                                                            as is_upsold
    , accounts_daily.first_ec_ordered_at                                        as upsold_at
    , count(case
                when accounts_daily.order_type = 'CR'
                and accounts_daily.dte < accounts_daily.first_ec_ordered_at
                    then accounts_daily.order_created_at_pst end) over
            (partition by accounts_daily.account_id)                            as num_cr_orders_before_ec_order
    , conditional_true_event((accounts_daily.dte <
                                 to_date(accounts_daily.first_ec_ordered_at)
                              or accounts_daily.first_ec_ordered_at is null)
            and accounts_daily.dte >=
                to_date(accounts_daily.first_cr_ordered_at)) over
            (partition by accounts_daily.account_id
                order by accounts_daily.dte)                                    as num_days_before_upsell
from accounts_daily
)
select
    *
from
    upsell_tracking
where
    dte >= to_date(first_cr_ordered_at)
    and dte <= current_date()