with full_site as (
    select
        fnoi.buck_order_id
        , fnoi.commerce_id
        , lower(concat_ws(',', coalesce(ua.company_name, '')
                             , coalesce(ua.city, '')
                             , coalesce(ua.country, '')
                             ))                           as buck_ship_to_account
        , lower(coalesce(concat_ws(',', coalesce(fnoi.end_company_name, '')
                             , coalesce(fnoi.ship_city, '')
                             , coalesce(fnoi.ship_country, '')
                             ), fnoi.company_name))       as fnoi_ship_to_account
        , nvl(buck_ship_to_account, fnoi_ship_to_account) as full_site
        , row_number() over (partition by fnoi.buck_order_id
            , fnoi.commerce_id
            order by fnoi.order_date)                     as rn
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
   , bookings as (
    select distinct
        fb.buck_sales_order_id                                                  as buck_order_id
        , fb.commerce_id
        , full_site.full_site                                                   as ship_to_account
        , min(fb.date)                                                          as created_date
        , sum(fb.net_amount_in_usd)                                             as net_amt
    from
        da_prod_db.datacore.fact_booking                                                   fb
        left join full_site
            on fb.commerce_id = full_site.commerce_id
            and fb.buck_sales_order_id = full_site.buck_order_id
    where
        fb.net_amount_in_usd is not null
        and fb.item_product_family = 'Engineered Cells' -- 'CRISPRevolution for CR
        and fb.factory_status not in ('canceled', 'pending_cancellation')
    group by
        1
        , 2
        , 3
    )
   , prior_dates as (
    select distinct
        b.buck_order_id
        , min(bp.created_date)                                                  as prior_min_order_date
    from
        bookings b
        left join (
            select distinct
                b3.ship_to_account
                , b3.created_date
            from
                bookings b3
            )    bp
                    on bp.ship_to_account = b.ship_to_account
                    and to_date(bp.created_date) < to_date(b.created_date)
                    and datediff(day, bp.created_date, b.created_date) < 366
    group by
        1
    )
   , first_order_year as (
    select distinct
        b2.ship_to_account
        , year(min(b2.created_date))                                            as start_year_first_order
    from
        bookings b2
    group by
        1
    )
   , allspending_12m as (
    select
        months
        , years
        , sum(prior1yr_bookings)                                                as total_orders_past_12m
    from
        (
            select distinct
                month(b5.created_date)                                          as months
                , year(b5.created_date)                                         as years
                , booking_12m.buck_order_id
                , booking_12m.commerce_id
                , booking_12m.created_date
                , booking_12m.ship_to_account
                , booking_12m.prior1yr_bookings
            from
                bookings b5
                left join (
                    select distinct
                        buck_order_id
                        , commerce_id
                        , created_date
                        , ship_to_account
                        , net_amt                                               as prior1yr_bookings
                    from
                        bookings
                    where
                        bookings.ship_to_account is not null
                        and bookings.buck_order_id is not null
                    )    booking_12m
                            on datediff(day, booking_12m.created_date
                                , b5.created_date) > 0
                            and datediff(month, booking_12m.created_date
                                , b5.created_date) < 12
            where
                b5.ship_to_account is not null
                and b5.buck_order_id is not null
            )
    group by
        1
        , 2
    )
   , past12m_rollingup as (
    select
        past_12m2.months
        , past_12m2.years
        , past_12m2.ship_to_account
        , past_12m2.total_orders_past_12m_account
        , allspending_12m.total_orders_past_12m
        , past_12m2.total_orders_past_12m_account
              / allspending_12m.total_orders_past_12m                           as prcnt_total
        , sum(prcnt_total) over (partition by past_12m2.years
            , past_12m2.months
            order by past_12m2.total_orders_past_12m_account desc)              as running_prcnt_total
    from
        (
            select
                past_12m.months
                , past_12m.years
                , past_12m.ship_to_account
                , sum(prior1yr_bookings)                                        as total_orders_past_12m_account
            from
                (
                    select distinct
                        month(b5.created_date)                                  as months
                        , year(b5.created_date)                                 as years
                        , booking_12m.buck_order_id
                        , booking_12m.commerce_id
                        , booking_12m.created_date
                        , booking_12m.ship_to_account
                        , booking_12m.prior1yr_bookings
                    from
                        bookings b5
                        left join (
                            select distinct
                                buck_order_id
                                , commerce_id
                                , created_date
                                , ship_to_account
                                , net_amt                                       as prior1yr_bookings
                            from
                                bookings
                            where
                                bookings.ship_to_account is not null
                                and bookings.buck_order_id is not null
                            )    booking_12m
                                    on datediff(day, booking_12m.created_date
                                        , b5.created_date) > 0
                                    and datediff(month, booking_12m.created_date
                                        , b5.created_date) < 12
                    where
                        b5.ship_to_account is not null
                        and b5.buck_order_id is not null
                    ) past_12m
            group by
                1
                , 2
                , 3
            ) past_12m2
        left join allspending_12m
            on allspending_12m.years = past_12m2.years
            and allspending_12m.months = past_12m2.months
    )

select distinct
    b.ship_to_account
    , b.buck_order_id
    , b.commerce_id
    , b.created_date
    , year(fb3.date) || '-' || month(fb3.date)                                  as month
    , year(fb3.date) || '-' || quarter(fb3.date)                                as quarter
    , year(b.created_date)                                                      as year
    , b.net_amt
    , pd.prior_min_order_date                                                   as earliest_order_date_last_one_year
    , iff(earliest_order_date_last_one_year is not null
        , 'Yes'
        , 'No')                                                                 as reorder
    , past12m_rollingup.prcnt_total
    , past12m_rollingup.running_prcnt_total
    , iff(past12m_rollingup.running_prcnt_total <= 0.5
        , 'Yes'
        , 'No')                                                                 as top_paying
    , fb3.item_product_line                                                     as product_line
    , fb3.item_product_family                                                   as product_family
    , fnoi.item_code                                                            as item_code
    , iff(fnoi.item_code is not null
          and fnoi.item_code = 'KOM-CP-IMM-SSTUBE'
        , true
        , false)                                                                as is_maverick
    , iff(upper(fb3.end_customer) in
          ('ABBVIE BIORESEARCH CENTER','AMGEN', 'ASTRAZENECA'
          ,'EMD SERONO', 'GLAXOSMITHKLINE', 'JANSSEN BIOTHERAPEUTICS'
          , 'MERCK KGAA', 'NOVARTIS', 'PFIZER', 'TAKEDA'
          , 'ARRAY BIO', 'JUNO THERAPEUTICS', 'MEDIMMUNE'
          , 'PHARMACYCLICS', 'GLAXOSMITHKLINE (GSK)'
          , 'ROCHE DIAGNOSTICS GMBH', 'CHUGAI PHARMABODY RESEARCH'
          , 'ALEXION PHARMACEUTICALS, INC.', 'BRISTOL-MYERS SQUIBB'
          , 'GENENTECH')
        , true
        , false)                                                                as is_gka
    , fb3.factory_status
    , fb3.customer
    , fb3.end_customer
    , fb3.customer_category_name
    , fb3.primary_sales_representative
    , fb3.sales_territory
    , fb3.date_shipped
    , acc.has_clinical_applications__c                                          as has_clinical_applications
    , acc.industry
    , fnoi.customer_tier                                                        as netsuite_customer_tier
    , acc.segment__c                                                            as segment
    , foy.start_year_first_order
    , acc.discovery_rep_estimated_acct_potential__c
    , acc.clinical_rep_estimated_acct_potential__c
    , acc.clinical_acct_potential_confidence__c
    , acc.discovery_acct_potential_confidence__c
    , row_number() over (partition by b.buck_order_id
        , b.commerce_id
        order by b.Ship_to_account desc)                                        as rn
from
    bookings                                                                    b
    left join first_order_year                                                  foy
        on b.ship_to_account = foy.ship_to_account
    left join prior_dates                                                       pd
        on pd.buck_order_id = b.buck_order_id
    join da_prod_db.datacore.fact_booking                                       fb3
        on b.commerce_id = fb3.commerce_id
        and b.buck_order_id = fb3.buck_sales_order_id
    left join da_prod_db.datacore.fact_netsuite_order_items                     fnoi
        on fb3.commerce_id = fnoi.commerce_id
        and  fb3.buck_sales_order_id = fnoi.external_id
    left join stitch.stitch_salesforce_prod."ORDER"                             o
        on b.buck_order_id::varchar = o.buck_id__c::varchar
    left join stitch.stitch_salesforce_prod.account                             acc
        on acc.id::varchar = o.accountid::varchar
    left join past12m_rollingup
        on past12m_rollingup.ship_to_account = b.ship_to_account
        and past12m_rollingup.years = year(b.created_date)
        and past12m_rollingup.months = month(b.created_date)
    qualify
        rn = 1