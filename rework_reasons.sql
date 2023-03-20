-- use database da_prod_db;
-- use schema analyst_reporting;
--
-- create or replace view vw_rework_reasons as

with work_orders as (
    select
        wwo.id                                                                  as work_order_id
        , convert_timezone('America/Los_Angeles'
            , wwo.created)                                                      as work_order_created_at
        , convert_timezone('America/Los_Angeles'
            , wwo.target_completion)                                            as work_order_target_completion_at
        , wwo.created_by_id
        , wwo.work_order_type_id
        , wot.work_order_type_name
        , wot.work_order_type_desc
        , wwo.name                                                              as work_order_name
        , wwo.status                                                            as work_order_status
        , configs.VALUE:"rework_reason"                                         as work_order_reason
    from
        stitch.stitch_barb_prod.wip_work_order                                  wwo
        left join da_prod_db.datacore.dim_work_order_type                       wot
            on wwo.work_order_type_id = wot.work_order_type_id
        , lateral flatten (input => parse_json(wwo.config_data))                configs
    where
        wwo.config_data ilike '%reason%'
        and configs.key = 'custom_fields'
    )
, work_order_reworks as (
    select
        work_orders.work_order_id
        , work_orders.work_order_created_at
        , work_orders.work_order_target_completion_at
        , work_orders.created_by_id
        , work_orders.work_order_type_id
        , work_orders.work_order_type_name
        , work_orders.work_order_type_desc
        , work_orders.work_order_name
        , work_orders.work_order_status
        , work_orders.work_order_reason
        , fsoi.sales_order_item_id
        , fsoi.sales_order_item_commerce_id
        , fsoi.ext_edit_type
        , coalesce(fsoi.netsuite_product_line_v1
            , fsoi.netsuite_product_line_v2)                                    as product_line
        , fsoi.module
        , fsoi.sales_order_id
        , fsoi.source_order_reference
        , convert_timezone('America/Los_Angeles'
            , fsoi.sales_order_created)                                         as sales_order_created_at
        , convert_timezone('America/Los_Angeles'
            , fsoi.chosen_order_actual_shipped_at)                              as sales_order_shipped_at
        , case
              when work_orders.work_order_reason in
                   ('Multi-Gene Transfection', 'Cell Viability'
                   , 'First attempt', 'Low KI for Gene', 'Contamination')
                  then work_orders.work_order_reason
              when work_orders.work_order_reason in
                   ('Low EE for Gene (KI only)', 'Low EE for Gene (KO only)')
                  then 'Low EE for Gene'
              when work_orders.work_order_reason in
                   ('No clones of requested genotype'
                   , 'Not enough clones of requested genotype')
                  then 'No / Not Enough clones of requested genotype'
              else 'Other'
          end                                                                   as rework_reasons_grouping
        , iff(work_orders.work_order_reason in
              ('First attempt', 'Multi-Gene Transfection')
            , false
            , true)                                                             as is_rework
    from
        work_orders
        left join da_prod_db.datacore.fact_sales_order_items                    fsoi
            on work_orders.work_order_id = fsoi.work_order_id
    where
        work_orders.work_order_reason != 'null'
    )
select
--     count(*) --941
    *
from
    work_order_reworks