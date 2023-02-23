/*
 joins in salesforce: account -> order (on account_id = id)
                      order -> datacore fact sales order items (on salesforce_sales_order_id = id)
                      user -> account (on createdbyid = id)
 createddate of account might be lead converted to account date

 Let's start initially with a createddate of an account >= '2020-01-01'

 confirmed that no order has both CR and EC products
 */


with accounts_with_orders as (
    select
        sfa.id                                                                  as account_id
        , sfa.name                                                              as account_name
        , sfa.createddate                                                       as account_created_at
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
            , null)                                                             as parent_account_id
        , iff(sfa.parentid is not null
            , sfa.parent_account_name__c
            , null)                                                             as parent_account_name
    from
        stitch.stitch_salesforce_prod.account                                   sfa
    where
        not sfa.isdeleted
        and sfa.createddate >= '2020-01-01'
        and sfa.id in (
            select distinct
                accountid
            from
                stitch.stitch_salesforce_prod."ORDER"
        )
    )
, categorized_orders as (
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
        , row_number() over (partition by fsoi.sales_order_id
            order by fsoi.sales_order_created desc)                             as rn
    from
        da_prod_db.datacore.fact_sales_order_items                                         fsoi
        left join stitch.stitch_salesforce_prod."ORDER"                         sfo
            on fsoi.salesforce_sales_order_id = sfo.id
    where
        fsoi.salesforce_sales_order_id is not null
        and not is_internal
    qualify
        rn = 1
    )
, min_orders_per_account as (
    select
        accounts_with_orders.account_id
        , accounts_with_orders.account_name
        , accounts_with_orders.account_created_at
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
        , min(case
                  when categorized_orders.order_type = 'CR'
                      then categorized_orders.order_created_at_pst
              end)      as first_cr_ordered_at_pst
        , min(case
                  when categorized_orders.order_type = 'EC'
                      then categorized_orders.order_created_at_pst
              end)      as first_ec_ordered_at_pst
    from
        accounts_with_orders
        left join categorized_orders
            on accounts_with_orders.account_id = categorized_orders.account_id
    group by
        1,2,3,4,5,6,7,8,9,10,11,12,13
    )
, accounts_with_upsells as (
    select
        mopa.account_id
        , mopa.account_name
        , mopa.account_created_at
        , mopa.accountsource
        , mopa.account_billing_city
        , mopa.account_billing_state
        , mopa.account_billing_country
        , mopa.industry
        , mopa.account_institution_type
        , mopa.numberofemployees
        , mopa.has_parent_account
        , mopa.parent_account_id
        , mopa.parent_account_name
        , mopa.first_cr_ordered_at_pst
        , mopa.first_ec_ordered_at_pst
        , iff(mopa.first_cr_ordered_at_pst < mopa.first_ec_ordered_at_pst
            , true
            , false)                                                            as is_upsell
        , datediff('quarter'
            , mopa.first_cr_ordered_at_pst
            , mopa.first_ec_ordered_at_pst)                                     as qtr_between_orders
    from
        min_orders_per_account                                                  mopa
    )
select * from accounts_with_upsells