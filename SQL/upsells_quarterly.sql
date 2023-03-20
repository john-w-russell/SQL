with base as (
    select
        date_trunc(quarter, uad.dte)                                        as qtr
        , uad.customer_cohort
        , uad.account_id
        , uad.account_name
        , uad.salesforce_url
        , uad.account_created_at
        , uad.account_billing_city
        , uad.account_billing_state
        , uad.account_billing_country
        , uad.industry
        , uad.account_institution_type
        , uad.numberofemployees
        , uad.is_top_paying_account
        , uad.has_parent_account
        , uad.parent_account_id
        , uad.parent_account_name
        , uad.first_cr_ordered_at
        , uad.upsold_at
        , datediff(quarter, uad.first_cr_ordered_at, qtr)                       as num_qtr_since_start
        , max(to_number(uad.is_upsold))                                         as num_upsells
        , max(iff(upsold_at is null, uad.num_days_before_upsell, null))         as days_since_cr_order
        , max(iff(upsold_at is not null, uad.num_days_before_upsell, null))     as days_before_upsell
        , avg(days_before_upsell) over
            (partition by uad.customer_cohort)                                  as avg_num_days_before_upsell
    from
        analyst_reporting.vw_upsells_account_daily_top_companies                uad
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
        , 17
        , 18
    )
select
    *
from
    base