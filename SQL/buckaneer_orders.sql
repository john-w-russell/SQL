use database da_prod_db;
use schema analyst_reporting;

create or replace view vw_buckaneer_orders as

with chosen_cell_lines as (
    select
        name
    from
        stitch.stitch_buckaneer_prod.product_kocellline
    )
, chosen_gene_names as (
    select distinct
        symbol
    from
        stitch.stitch_buckaneer_prod.product_kodesigndata
    )
, web_eligible_products as (
    select distinct
        buckaneer_product_id
        , product_line
        , product_category
        , sku_list_product_name                                                 as product_name
        , product_code
    from
        da_prod_db.analyst_reporting.web_store_product_sku
    )
    , buckaneer_orders as (
    select distinct
        odg.order_id
        , oi.id                                                                 as order_item_id
        , upper(pp.family)                                                      as family
        , oi.source_system
        , oo.status
        , oo.created                                                            as order_created
        , oo.in_process_start_date
        , oo.estimated_ship_date
        , oo.actual_ship_date
        , oi.product_id
        , net_oi.product_category
        , net_oi.product_line
        , pp.name                                                               as product_name
        , coalesce(pp.netsuite_part_number, net_oi.item_code)                   as product_sku
        , oi.product_name                                                       as product_details
        , oi.units_sold
        , oi.units_sold * oi.unit_price_net                                     as units_x_net
        , oi.unit_price_net
        , oi.unit_price_gross
        , oi.unit_price_list
        , oi.product_config
        , upa.company_name                                                      as shipping_company_name
        , upa.first_name
        , upa.last_name
        , upa.notification_email
        , upa.country
    from
        stitch.stitch_buckaneer_prod.order_ordereditem                          oi
        left join stitch.stitch_buckaneer_prod.order_deliverygroup              odg
            on oi.delivery_group_id = odg.id
        left join stitch.stitch_buckaneer_prod.product_product                  pp
            on oi.product_id = pp.id
            and lower(pp.name) not like '%shipping%' -- exclude shipping addon charges
        left join stitch.stitch_buckaneer_prod.order_order                      oo
            on odg.order_id = oo.id
            and oo.status not in ('cart', 'canceled', 'declined', 'new')
        left join datacore.fact_netsuite_order_items                            net_oi
            on oo.netsuite_internal_id = net_oi.order_internal_id
            and oi.commerce_id = net_oi.commerce_id
        left join stitch.stitch_buckaneer_prod.userprofile_address              upa
            on oo.shipping_address_id = upa.id
        where
            oi.product_name not ilike '%ship%'
    )
   , extract_product_info as (
    select
        bo.order_id
        , bo.order_item_id
        , bo.family
        , bo.source_system
        , bo.status
        , bo.order_created
        , bo.in_process_start_date
        , bo.estimated_ship_date
        , bo.actual_ship_date
        , bo.product_id
        , bo.product_name
        , bo.product_sku
        , bo.product_details
        , bo.units_sold
        , bo.units_x_net
        , bo.unit_price_net
        , bo.unit_price_gross
        , bo.unit_price_list
        , bo.shipping_company_name
        , bo.first_name
        , bo.last_name
        , bo.notification_email
        , bo.country
        , bo.product_config:product_type                                        as product_type_json
        , bo.product_config:gene_name                                           as gene_name
        , bo.product_config:cell_line                                           as cell_line
        , bo.product_config:cell_source                                         as cell_source
        , bo.product_config:cell_type                                           as cell_type
        , bo.product_config:edit_type                                           as edit_type
        , bo.product_config:is_optimized                                        as is_optimized
        , bo.product_config:item_type_name                                      as item_type_name
        , bo.product_config:modification                                        as cell_modification
        , bo.product_config:population                                          as cell_population
        , bo.product_config:species                                             as species
        , bo.product_category
        , bo.product_line
        , last_value(bo.product_category) ignore nulls over
            (partition by bo.product_id
             order by bo.in_process_start_date)                                 as product_category_cln
        , last_value(bo.product_line) ignore nulls over
            (partition by product_id, bo.product_sku
             order by bo.in_process_start_date)                                 as product_line_cln
    from
        buckaneer_orders                                                        bo
    )
, web_eligibility_ec as (
    select
        epi.order_id
        , epi.order_item_id
        , epi.family                                                            as product_family
        , epi.source_system
        , epi.status
        , epi.order_created
        , epi.in_process_start_date
        , epi.estimated_ship_date
        , epi.actual_ship_date
        , epi.product_id
        , epi.product_category_cln                                              as product_category
        , epi.product_line_cln                                                  as product_line
        , epi.product_name
--         , case
--               when epi.product_id in (40, 44, 1008, 1009, 1010, 1011, 1183, 1187, 1216, 1331)
--                   then 'KO Cell Pool'
--               when epi.product_id in (45, 1139, 1176, 1214, 1219)
--                   then 'KO Cell Clone'
--               when epi.product_type_json = 'engineered_cell_libraries'
--                   or epi.product_name like '%LIBRARY'
--                   then 'ECL'
--               when epi.product_id in ()
--                   then 'Standard sgRNA'
--               when epi.product_name ilike 'crispr custom rna%'
--                   then 'Custom RNA'
--               when epi.product_name ilike '%custom library%'
--                   then 'Custom Libraries'
--               when epi.sku ilike '%accessory%'
--                   then 'CR Accessories'
--               else 'Unknown'
--           end                                                                   as product_category
        , epi.product_sku
        , epi.product_details
        , epi.units_sold
        , epi.units_x_net
        , epi.unit_price_net
        , epi.unit_price_gross
        , epi.unit_price_list
        , epi.shipping_company_name
        , epi.first_name
        , epi.last_name
        , epi.notification_email
        , epi.country
        , epi.gene_name
        , epi.cell_line
        , epi.cell_source
        , epi.cell_type
        , epi.edit_type
        , epi.is_optimized
        , epi.item_type_name
        , epi.cell_modification
        , epi.cell_population
        , epi.species
        , iff(epi.product_id in (
        select
            buckaneer_product_id
        from
            web_eligible_products
        )
                  and epi.species = 'human'
                  and epi.cell_modification = 'knock_out'
                  and epi.edit_type in ('single_guide', 'single_gene')
                  and epi.cell_type = 'immortalized'
                  and epi.cell_source = 'synthego_supplied'
                  and epi.cell_line in (
            select
                name
            from
                chosen_cell_lines
            )
                  and epi.gene_name in (
            select
                symbol
            from
                chosen_gene_names
            )
        , true
        , false)                                                                as is_webstore_eligible
        , iff(epi.source_system = 'webstore', true, false)                      as is_webstore_order
    from
        extract_product_info                                                    epi
    where
        epi.family = 'EC'
        and epi.estimated_ship_date is not null
        and epi.in_process_start_date >= '2021-01-01'
    )
, web_eligibility_cr as (
    select
        epi.order_id
        , epi.order_item_id
        , epi.family                                                            as product_family
        , epi.source_system
        , epi.status
        , epi.order_created
        , epi.in_process_start_date
        , epi.estimated_ship_date
        , epi.actual_ship_date
        , epi.product_id
        , epi.product_category_cln                                              as product_category
        , epi.product_line_cln                                                  as product_line
        , epi.product_name
--         , case
--               when epi.product_id in (40, 44, 1008, 1009, 1010, 1011, 1183, 1187, 1216, 1331)
--                   then 'KO Cell Pool'
--               when epi.product_id in (45, 1139, 1176, 1214, 1219)
--                   then 'KO Cell Clone'
--               when epi.product_type_json = 'engineered_cell_libraries'
--                   or epi.product_name like '%LIBRARY'
--                   then 'ECL'
--               when epi.product_id in ()
--                   then 'Standard sgRNA'
--               when epi.product_name ilike 'crispr custom rna%'
--                   then 'Custom RNA'
--               when epi.product_name ilike '%custom library%'
--                   then 'Custom Libraries'
--               when epi.sku ilike '%accessory%'
--                   then 'CR Accessories'
--               else 'Unknown'
--           end                                                                   as product_category
        , epi.product_sku
        , epi.product_details
        , epi.units_sold
        , epi.units_x_net
        , epi.unit_price_net
        , epi.unit_price_gross
        , epi.unit_price_list
        , epi.shipping_company_name
        , epi.first_name
        , epi.last_name
        , epi.notification_email
        , epi.country
        , epi.gene_name
        , epi.cell_line
        , epi.cell_source
        , epi.cell_type
        , epi.edit_type
        , epi.is_optimized
        , epi.item_type_name
        , epi.cell_modification
        , epi.cell_population
        , epi.species
        , iff(epi.product_id in (
            select
                buckaneer_product_id
            from
                web_eligible_products
        )
            , true
            , false)                                                            as is_webstore_eligible
        , iff(epi.source_system = 'webstore', true, false)                      as is_webstore_order
    from
        extract_product_info                                                    epi
    where
        epi.family not in ('EC', 'EDIT_CREDIT')
        and epi.estimated_ship_date is not null
        and epi.in_process_start_date >= '2021-01-01'
    )
, final_union as (
    select
        we_ec.order_id
        , we_ec.order_item_id
        , we_ec.product_family
        , we_ec.source_system
        , we_ec.status
        , we_ec.order_created
        , we_ec.in_process_start_date
        , we_ec.estimated_ship_date
        , we_ec.actual_ship_date
        , we_ec.product_id
        , we_ec.product_category
        , iff(we_ec.product_line ilike '%libraries'
            , 'ECL'
            , we_ec.product_line)                                               as product_line
        , we_ec.product_name
        , we_ec.product_sku
        , we_ec.product_details
        , we_ec.units_sold
        , we_ec.units_x_net
        , we_ec.unit_price_net
        , we_ec.unit_price_gross
        , we_ec.unit_price_list
        , we_ec.shipping_company_name
        , we_ec.first_name
        , we_ec.last_name
        , we_ec.notification_email
        , we_ec.country
        , we_ec.gene_name
        , we_ec.cell_line
        , we_ec.cell_source
        , we_ec.cell_type
        , we_ec.edit_type
        , we_ec.is_optimized
        , we_ec.item_type_name
        , we_ec.cell_modification
        , we_ec.cell_population
        , we_ec.species
        , we_ec.is_webstore_eligible
        , we_ec.is_webstore_order
    from
        web_eligibility_ec                                                      we_ec

    union all

    select
        we_cr.order_id
        , we_cr.order_item_id
        , we_cr.product_family
        , we_cr.source_system
        , we_cr.status
        , we_cr.order_created
        , we_cr.in_process_start_date
        , we_cr.estimated_ship_date
        , we_cr.actual_ship_date
        , we_cr.product_id
        , we_cr.product_category
        , we_cr.product_line
        , we_cr.product_name
        , we_cr.product_sku
        , we_cr.product_details
        , we_cr.units_sold
        , we_cr.units_x_net
        , we_cr.unit_price_net
        , we_cr.unit_price_gross
        , we_cr.unit_price_list
        , we_cr.shipping_company_name
        , we_cr.first_name
        , we_cr.last_name
        , we_cr.notification_email
        , we_cr.country
        , we_cr.gene_name
        , we_cr.cell_line
        , we_cr.cell_source
        , we_cr.cell_type
        , we_cr.edit_type
        , we_cr.is_optimized
        , we_cr.item_type_name
        , we_cr.cell_modification
        , we_cr.cell_population
        , we_cr.species
        , we_cr.is_webstore_eligible
        , we_cr.is_webstore_order
    from
        web_eligibility_cr                                                      we_cr
    )
select * from final_union