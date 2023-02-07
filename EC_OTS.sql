with distinct_shipped_soi as (
    select distinct
        fact_sales_order_items.sales_order_id
        , fact_sales_order_items.sales_order_item_id
        , fact_sales_order_items.source_order_reference
        , convert_timezone('America/Los_Angeles'
        , fact_sales_order_items.sales_order_created)                           as sales_order_created_at_pst
        , fact_sales_order_items.sales_order_status
--         , fact_sales_order_items.ext_edit_type     --29 sales_order_items have 2 values, removing for now
        , fact_sales_order_items.gene_name
        , fact_sales_order_items.cell_modification
        , fact_sales_order_items.netsuite_product_line_v1                       as product_line
        , fact_sales_order_items.product_type
        , fact_sales_order_items.growth_mode
        , fact_sales_order_items.cell_edit_type
        , fact_sales_order_items.cell_line
        , fact_sales_order_items.cell_type
        , convert_timezone('America/Los_Angeles'
        , fact_sales_order_items.ready_to_ship_at)                              as order_ready_to_ship_at_pst
        , convert_timezone('America/Los_Angeles'
        , fact_sales_order_items.chosen_order_actual_shipped_at)                as order_actual_shipped_at_pst
        , convert_timezone('America/Los_Angeles'
        , fact_sales_order_items.chosen_original_promised_at)                   as order_original_promised_at_pst
        , convert_timezone('America/Los_Angeles'
        , fact_sales_order_items.chosen_current_promised_at)                    as order_current_promised_at_pst
        , fact_sales_order_items.blocked_day_duration
    from
        da_prod_db.datacore.fact_sales_order_items
    where
        fact_sales_order_items.sales_order_status = 'shipped'
        and fact_sales_order_items.factory_label = 'ec'
        and (fact_sales_order_items.company_name != 'Cells Synthego'
             and fact_sales_order_items.source_system != 'LIMS') --removes internal orders
    )
   , on_time_shipment as (
    select
        distinct_shipped_soi.sales_order_item_id
        , distinct_shipped_soi.sales_order_id
        , distinct_shipped_soi.source_order_reference
        , distinct_shipped_soi.sales_order_created_at_pst
        , distinct_shipped_soi.order_ready_to_ship_at_pst
        , distinct_shipped_soi.order_actual_shipped_at_pst
        , distinct_shipped_soi.order_original_promised_at_pst
        , distinct_shipped_soi.order_current_promised_at_pst
        , distinct_shipped_soi.blocked_day_duration
        , distinct_shipped_soi.sales_order_status
        , distinct_shipped_soi.gene_name
        , distinct_shipped_soi.cell_modification
        , distinct_shipped_soi.product_line
        , distinct_shipped_soi.product_type
        , distinct_shipped_soi.growth_mode
        , distinct_shipped_soi.cell_edit_type
 --       , distinct_shipped_soi.ext_edit_type
        , distinct_shipped_soi.cell_line
        , distinct_shipped_soi.cell_type
        , datediff(day, distinct_shipped_soi.sales_order_created_at_pst
        , distinct_shipped_soi.order_original_promised_at_pst)                  as days_from_created_to_original_promised
        , datediff(day
            , distinct_shipped_soi.order_original_promised_at_pst
            , distinct_shipped_soi.order_actual_shipped_at_pst)                 as diff_from_ots_original
        , iff(diff_from_ots_original < 1, true, false)                          as is_ots_original --lat alias
        , coalesce(diff_from_ots_original
                       + distinct_shipped_soi.blocked_day_duration
            , diff_from_ots_original)                                           as diff_from_ots_block_original
        , iff(diff_from_ots_block_original < 1, true, false)                    as is_ots_block_original --lat alias
        , datediff(day, distinct_shipped_soi.sales_order_created_at_pst
        , distinct_shipped_soi.order_current_promised_at_pst)                   as days_from_created_to_current_promised
        , datediff(day
            , distinct_shipped_soi.order_current_promised_at_pst
            , distinct_shipped_soi.order_actual_shipped_at_pst)                 as diff_from_ots_current
        , iff(diff_from_ots_current < 1, true, false)                           as is_ots_current --lat alias
        , coalesce(diff_from_ots_current
                       + distinct_shipped_soi.blocked_day_duration
            , diff_from_ots_current)                                            as diff_from_ots_block_current
        , iff(diff_from_ots_block_current < 1, true, false)                     as is_ots_block_current --lat alias
    from
        distinct_shipped_soi
    )
select
    *
from
    on_time_shipment
where
    abs(on_time_shipment.diff_from_ots_original) < 1000 --removes outliers