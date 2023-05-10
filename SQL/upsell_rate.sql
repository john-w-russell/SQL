with sales_order_item_rollup as (
    select
        fnoi.buck_order_id
        , lower(concat_ws(',', coalesce(ua.company_name, '')
        , coalesce(ua.city, '')
        , coalesce(ua.country, '')
        ))                                                                      as buck_ship_to_account
        , lower(coalesce(concat_ws(',', coalesce(fnoi.end_company_name, '')
                             , coalesce(fnoi.ship_city, '')
                             , coalesce(fnoi.ship_country, '')
                             ), fnoi.company_name))                             as fnoi_ship_to_account
        , nvl(buck_ship_to_account, fnoi_ship_to_account)                       as full_site
        , row_number() over (partition by fnoi.buck_order_id
        order by fnoi.order_date)                                               as rn
    from
        stitch.stitch_buckaneer_prod.order_order                                o
        left join stitch.stitch_buckaneer_prod.userprofile_address              ua
            on ua.id = o.shipping_address_id
        left join da_prod_db.datacore.fact_netsuite_order_items                            fnoi
            on o.id = fnoi.buck_order_id
    where
        fnoi_ship_to_account != ',,'
        qualify
            rn = 1
    )
   , gko_items as (
    select
        fact_booking.buck_sales_order_id                                        as buck_order_id
        , to_date(fact_booking.date)                                            as order_date
        , first_value(fact_booking.item_product_family) over
        (partition by fact_booking.buck_sales_order_id
        order by item_product_family desc)                                      as order_family
        , fact_booking.commerce_id
        , fact_booking.item_product_line
        , fact_booking.item_name
        , initcap(fact_booking.end_customer)                                    as company_name
        , fact_booking.net_amount_in_usd
    from
        da_prod_db.datacore.fact_booking
    where
        fact_booking.item_product_line = 'GKO'
        and fact_booking.date_shipped is not null
        and fact_booking.buck_sales_order_id is not null
    )
   , gko_orders_rollup as (
    select
        gko_items.buck_order_id
        , gko_items.company_name
        , gko_items.order_date
        , gko_items.order_family
        , 'GKO'                                                                 as product_type
        , sum(gko_items.net_amount_in_usd)                                      as total_order_amount
    from
        gko_items
    group by
        1
        , 2
        , 3
        , 4
        , 5
    )
   , gko_orders_by_fullsite_date as (
    select
        soi.full_site
        , gor.company_name
        , gor.buck_order_id
        , gor.order_date
        , gor.order_family
        , gor.product_type
        , gor.total_order_amount
    from
        gko_orders_rollup                                                       gor
        join sales_order_item_rollup                                            soi
            on gor.buck_order_id = soi.buck_order_id
    )
   , ec_items as (
    select
        fact_booking.buck_sales_order_id                                        as buck_order_id
        , to_date(fact_booking.date)                                            as order_date
        , first_value(fact_booking.item_product_family) over
        (partition by fact_booking.buck_sales_order_id
        order by item_product_family desc)                                      as order_family
        , fact_booking.commerce_id
        , fact_booking.item_product_line
        , fact_booking.item_name
        , initcap(fact_booking.end_customer)                                    as company_name
        , fact_booking.net_amount_in_usd
    from
        da_prod_db.datacore.fact_booking
    where
        fact_booking.item_product_family = 'Engineered Cells'
    )
   , ec_orders_rollup as (
    select
        ec_items.buck_order_id
        , ec_items.company_name
        , ec_items.order_date
        , ec_items.order_family
        , 'EC'                                                                  as product_type
        , sum(ec_items.net_amount_in_usd)                                       as total_order_amount
    from
        ec_items
    group by
        1
        , 2
        , 3
        , 4
        , 5
    )
   , ec_orders_by_fullsite_date as (
    select
        soi.full_site
        , eor.company_name
        , eor.buck_order_id
        , eor.order_date
        , eor.order_family
        , eor.product_type
        , eor.total_order_amount
    from
        ec_orders_rollup                                                        eor
        join sales_order_item_rollup                                            soi
            on eor.buck_order_id = soi.buck_order_id
    where
            soi.full_site in (
            select distinct
                gko_orders_by_fullsite_date.full_site
            from
                gko_orders_by_fullsite_date
            )
    )
   , combined_orders as (
    select
        gof.full_site
        , gof.company_name
        , gof.buck_order_id
        , gof.order_date
        , gof.order_family
        , gof.product_type
        , gof.total_order_amount
    from
        gko_orders_by_fullsite_date                                             gof
    union
    select
        eof.full_site
        , eof.company_name
        , eof.buck_order_id
        , eof.order_date
        , eof.order_family
        , eof.product_type
        , eof.total_order_amount
    from
        ec_orders_by_fullsite_date eof
    )
    , orders_by_date as (
        select
            combined_orders.full_site
            , combined_orders.company_name
            , combined_orders.buck_order_id
            , combined_orders.order_date
            , combined_orders.order_family
            , combined_orders.product_type
            , combined_orders.total_order_amount
            , row_number() over (partition by combined_orders.full_site
                order by combined_orders.order_date)                            as rn
        from
            combined_orders
    )
, order_grouping as (
    select
        obd.full_site
        , obd.company_name
--         , sfo.id                                                                as salesforce_order_id
        , mode(sfa.id) over (partition by obd.company_name)                     as salesforce_account_id
        , mode(sfa.name) over (partition by obd.company_name)                   as salesforce_account_name
--         , mode(sfa.segmentf__c) over (partition by obd.company_name)            as segment
        , mode(sfa.industry) over (partition by obd.company_name)               as industry
        , mode(sfa.institution_type__c) over (partition by obd.company_name)    as institution_type
        , mode(sfa.numberofemployees) over (partition by obd.company_name)      as number_of_employees
        , mode(sfa.type__c) over (partition by obd.company_name)                as company_type
--         , obd.buck_order_id
--         , obd.order_date
        , year(obd.order_date)                                                  as order_year
        , quarter(obd.order_date)                                               as order_quarter
        , order_year || '-' || order_quarter                                    as order_qrt_yr
--         , month(obd.order_date)                                                 as order_month
--         , weekiso(obd.order_date)                                               as order_week_num
        , obd.order_family
        , obd.product_type
        , min(obd.order_date) over (partition by obd.full_site
            , obd.product_type
            , order_qrt_yr)                                                     as first_order_date
        , listagg(obd.buck_order_id || ' - ' || obd.order_date, ', ') over
            (partition by obd.full_site
            , obd.product_type
            , order_qrt_yr)                                                     as list_buck_order_id
        , listagg(sfo.id, ', ') over
            (partition by obd.full_site
            , obd.product_type
            , order_qrt_yr)                                                     as list_sf_order_id
        , sum(obd.total_order_amount) over (partition by obd.full_site
            , obd.product_type, order_qrt_yr)                                   as total_amount_qtr
        , row_number() over (partition by obd.full_site
            , obd.product_type, order_qrt_yr
            order by order_qrt_yr)                                              as row_num
    from
        orders_by_date obd
        left join stitch.stitch_salesforce_prod."ORDER"                         sfo
            on obd.buck_order_id = sfo.buck_id__c
        left join stitch.stitch_salesforce_prod.account                         sfa
            on sfo.accountid = sfa.id
    qualify
        row_num = 1
    )
, upsells as (
    select
        og.full_site
        , og.company_name
        , og.salesforce_account_id
        , og.salesforce_account_name
        , og.industry
        , og.institution_type
        , og.number_of_employees
        , og.company_type
        , og.order_year
        , og.order_quarter
        , og.order_qrt_yr
        , og.order_family
        , og.product_type
        , og.list_buck_order_id
        , og.list_sf_order_id
        , og.first_order_date
        , og.total_amount_qtr
        , lead(og.product_type)
              over (partition by og.full_site order by og.first_order_date)     as next_product_type
        , lead(og.order_qrt_yr)
              over (partition by og.full_site order by og.first_order_date)     as next_order_qrt
        , case
              when og.order_qrt_yr = next_order_qrt
                  then true
              when og.order_qrt_yr != next_order_qrt
                  then false
          end                                                                   as led_to_upsell
        , iff(product_type = 'EC', true, false)                                 as is_ec_order
    from
        order_grouping                                                          og
    )
select * from upsells
-- select
--     full_site
--     , company_name
--     , iff(is_upsell, order_date, null)                                          as upsell_date
--     , iff(is_ec_order, order_date, null)                                        as ec_order_date
--     , total_order_amount
--     , sum(iff(product_type = 'GKO'))
--     , sum(iff(is_upsell, 1, 0)) over (partition by full_site)                   as total_upsells
--     , sum(iff(is_ec_order, 1, 0)) over (partition by full_site)                 as total_ec_orders
-- from
--     upsells
-- order by
--     full_site


/*
 Further work: if consecutive rn goes GKO -> EC, count towards numerator. Distinct count of full site = denominator
 Difference between GKO and EC <= 2 months to count
 */