/*
    With Eclipse, sales orders = sales order items
    Good clones analysis: Rank shipped clones low TAT + low # reworks
    by CELL LINE, GENE NAME, PRODUCT LINE, EDIT TYPE, CELL SOURCE
*/

with reworks as (
    select distinct
        convert_timezone('America/Los_Angeles'
            , fact_sales_order_items.sales_order_created)                                           as sales_order_created_pst
        , convert_timezone('America/Los_Angeles'
            , fact_sales_order_items.chosen_order_actual_shipped_at)                                as sales_order_shipped_pst
        , fact_sales_order_items.sales_order_id
        , fact_sales_order_items.source_order_reference
        , fact_sales_order_items.company_name
        , fact_sales_order_items.work_order_id
        , fact_sales_order_items.work_order_type
        , convert_timezone('America/Los_Angeles'
            , dim_work_order_status.created_at)                                                     as work_order_created_pst
        , coalesce(fact_sales_order_items.netsuite_product_line_v1
                 , fact_sales_order_items.netsuite_product_line_v2)                                 as product_line
        , fact_sales_order_items.cell_source
        , fact_sales_order_items.cell_type
        , fact_sales_order_items.cell_line
        , fact_sales_order_items.gene_name
        , fact_sales_order_items.cell_modification
        , fact_sales_order_items.cell_edit_type
--         , fact_sales_order_items.ext_edit_type
        , fact_sales_order_items.source_system
        , coalesce(fact_sales_order_items.blocked_day_duration, 0)                                  as blocked_duration
        , fact_sales_order_items.milestone
        , fact_sales_order_items.module
        , false                                                                                     as is_rework
    from
        da_prod_db.datacore.fact_sales_order_items
    join da_prod_db.datacore.dim_work_order_status
        on fact_sales_order_items.work_order_id = dim_work_order_status.work_order_id
    where
        fact_sales_order_items.factory_label = 'ec'
        and module not in ('error', 'UNMAPPED', '3: Transfection', '4: Single Cell Dispense')
union
    select distinct
        convert_timezone('America/Los_Angeles'
            , fact_sales_order_items.sales_order_created)                                           as sales_order_created_pst
        , convert_timezone('America/Los_Angeles'
            , fact_sales_order_items.chosen_order_actual_shipped_at)                                as sales_order_shipped_pst
        , fact_sales_order_items.sales_order_id
        , fact_sales_order_items.source_order_reference
        , fact_sales_order_items.company_name
        , fact_sales_order_items.work_order_id
        , fact_sales_order_items.work_order_type
        , convert_timezone('America/Los_Angeles'
            , dim_work_order_status.created_at)                                                     as work_order_created_pst
        , coalesce(fact_sales_order_items.netsuite_product_line_v1
                 , fact_sales_order_items.netsuite_product_line_v2)                                 as product_line
        , fact_sales_order_items.cell_source
        , fact_sales_order_items.cell_type
        , fact_sales_order_items.cell_line
        , fact_sales_order_items.gene_name
        , fact_sales_order_items.cell_modification
        , fact_sales_order_items.cell_edit_type
--         , fact_sales_order_items.ext_edit_type
        , fact_sales_order_items.source_system
        , coalesce(fact_sales_order_items.blocked_day_duration, 0)                                  as blocked_duration
        , fact_sales_order_items.milestone
        , fact_sales_order_items.module
        , row_number() over
                  (partition by fact_sales_order_items.sales_order_id
                      , fact_sales_order_items.work_order_type
                  order by dim_work_order_status.created_at) != 1                                   as is_rework
    from
        da_prod_db.datacore.fact_sales_order_items
    join da_prod_db.datacore.dim_work_order_status
        on fact_sales_order_items.work_order_id = dim_work_order_status.work_order_id
    where
        fact_sales_order_items.ext_edit_type not in ('double_gene', 'triple_gene')
        and fact_sales_order_items.factory_label = 'ec'
        and module in ('3: Transfection', '4: Single Cell Dispense')
union
    select distinct
        convert_timezone('America/Los_Angeles'
            , fact_sales_order_items.sales_order_created)                                           as sales_order_created_pst
        , convert_timezone('America/Los_Angeles'
            , fact_sales_order_items.chosen_order_actual_shipped_at)                                as sales_order_shipped_pst
        , fact_sales_order_items.sales_order_id
        , fact_sales_order_items.source_order_reference
        , fact_sales_order_items.company_name
        , fact_sales_order_items.work_order_id
        , fact_sales_order_items.work_order_type
        , convert_timezone('America/Los_Angeles'
            , dim_work_order_status.created_at)                                                     as work_order_created_pst
        , coalesce(fact_sales_order_items.netsuite_product_line_v1
                 , fact_sales_order_items.netsuite_product_line_v2)                                 as product_line
        , fact_sales_order_items.cell_source
        , fact_sales_order_items.cell_type
        , fact_sales_order_items.cell_line
        , fact_sales_order_items.gene_name
        , fact_sales_order_items.cell_modification
        , fact_sales_order_items.cell_edit_type
--         , fact_sales_order_items.ext_edit_type
        , fact_sales_order_items.source_system
        , coalesce(fact_sales_order_items.blocked_day_duration, 0)                                  as blocked_duration
        , fact_sales_order_items.milestone
        , fact_sales_order_items.module
        , row_number() over
                  (partition by fact_sales_order_items.sales_order_id
                      , fact_sales_order_items.work_order_type
                  order by dim_work_order_status.created_at) > 2                                    as is_rework
    from
        da_prod_db.datacore.fact_sales_order_items
    join da_prod_db.datacore.dim_work_order_status
        on fact_sales_order_items.work_order_id = dim_work_order_status.work_order_id
    where
        fact_sales_order_items.ext_edit_type = 'double_gene'
        and fact_sales_order_items.factory_label = 'ec'
        and module in ('3: Transfection', '4: Single Cell Dispense')
    union
    select distinct
        convert_timezone('America/Los_Angeles'
            , fact_sales_order_items.sales_order_created)                                           as sales_order_created_pst
        , convert_timezone('America/Los_Angeles'
            , fact_sales_order_items.chosen_order_actual_shipped_at)                                as sales_order_shipped_pst
        , fact_sales_order_items.sales_order_id
        , fact_sales_order_items.source_order_reference
        , fact_sales_order_items.company_name
        , fact_sales_order_items.work_order_id
        , fact_sales_order_items.work_order_type
        , convert_timezone('America/Los_Angeles'
            , dim_work_order_status.created_at)                                                     as work_order_created_pst
        , coalesce(fact_sales_order_items.netsuite_product_line_v1
                 , fact_sales_order_items.netsuite_product_line_v2)                                 as product_line
        , fact_sales_order_items.cell_source
        , fact_sales_order_items.cell_type
        , fact_sales_order_items.cell_line
        , fact_sales_order_items.gene_name
        , fact_sales_order_items.cell_modification
        , fact_sales_order_items.cell_edit_type
--         , fact_sales_order_items.ext_edit_type
        , fact_sales_order_items.source_system
        , coalesce(fact_sales_order_items.blocked_day_duration, 0)                                  as blocked_duration
        , fact_sales_order_items.milestone
        , fact_sales_order_items.module
        , row_number() over
                  (partition by fact_sales_order_items.sales_order_id
                      , fact_sales_order_items.work_order_type
                  order by dim_work_order_status.created_at) > 3                                    as is_rework
    from
        da_prod_db.datacore.fact_sales_order_items
    join da_prod_db.datacore.dim_work_order_status
        on fact_sales_order_items.work_order_id = dim_work_order_status.work_order_id
    where
        fact_sales_order_items.ext_edit_type = 'triple_gene'
        and fact_sales_order_items.factory_label = 'ec'
        and module in ('3: Transfection', '4: Single Cell Dispense')
    )
, base as (
    select
        reworks.sales_order_created_pst
        , reworks.sales_order_shipped_pst
        , reworks.sales_order_id
        , reworks.source_order_reference
--         , reworks.work_order_id
--         , reworks.work_order_type
--         , reworks.work_order_created_pst
        , reworks.product_line
        , reworks.cell_source
        , reworks.cell_type
        , reworks.cell_line
        , reworks.gene_name
        , reworks.cell_modification
        , reworks.cell_edit_type
--         , reworks.ext_edit_type
        , reworks.company_name
        , reworks.source_system
        , iff(reworks.company_name like 'Cells Synthego' and reworks.source_system = 'LIMS'
            , true
            , false)                                                                                as is_internal
--         , reworks.blocked_duration
--         , reworks.milestone
--         , reworks.module
--         , reworks.is_rework
        , count_if(reworks.is_rework = true)                                                        as total_reworks
        , count(reworks.is_rework)                                                                  as total_orders
        , datediff('day', reworks.sales_order_created_pst
                      , reworks.sales_order_shipped_pst)                                            as tat
        , min(blocked_duration)                                                                     as total_blocked
        , tat + total_blocked                                                                       as tat_blocked
    from
        reworks
    where
        reworks.sales_order_shipped_pst is not null
    group by
        reworks.sales_order_created_pst
        , reworks.sales_order_shipped_pst
        , reworks.sales_order_id
        , reworks.source_order_reference
--         , reworks.work_order_id
--         , reworks.work_order_type
--         , reworks.work_order_created_pst
        , reworks.product_line
        , reworks.cell_source
        , reworks.cell_type
        , reworks.cell_line
        , reworks.gene_name
        , reworks.cell_modification
        , reworks.cell_edit_type
--         , reworks.ext_edit_type
        , reworks.source_system
        , reworks.company_name
--         , reworks.blocked_duration
--         , reworks.milestone
--         , reworks.module
--         , reworks.is_rework
    )
select * from base