with remove_unwanted_parts as (
    select
        fsoi.sales_order_id
        , fsoi.source_order_reference
        , fsoi.sales_order_item_id
        , fsoi.sales_order_item_commerce_id
        , coalesce(netsuite_product_line_v1, netsuite_product_line_v2)          as product_line
        , case
              when product_type in ('gko_kit_homo_sapiens', 'multiplex_sgrna'
              , 'tok_multiguide')
                  then 'GKO'
              when product_type in ('crisprcas9_sgrna_ez_kit', 'ez_RNA_kit'
              , 'ez_sgRNA_oligonucleotide', 'ez_sgRNA_oligonucleotide_modified'
              , 'sgrna_cell_validated', 'sgrna_kit', 'sgrna_nobuffer'
              , 'sgrna_screening_plate')
                  then 'sgRNA'
              else 'Other'
          end                                                                   as product_category
        , nmol_units
--         , iff(product_line like any ('GKO', '%sgRNA%')
--             and nmol_units  <= 5
--             and nmol_units >= 1.5
--             , true, false)                                                      as chosen_orders --lat alias
         , iff(product_category != 'Other'
            and nmol_units  <= 5
            and nmol_units >= 1.5
            , true, false)                                                      as chosen_orders --lat alias
        , conditional_change_event(chosen_orders)
            over (partition by fsoi.sales_order_id
                order by chosen_orders desc)                                    as product_changes --lat alias
    from
        datacore.fact_sales_order_items                                         fsoi
    qualify
        product_changes = 1
    )
, base as (
    select distinct
        fsoi.sales_order_id
        , fsoi.source_order_reference
        , fsoi.product_type
        , nvl(regexp_substr(fsoi.source_order_reference, '-([^-]*)-', 1, 1, 'e'),
              fsoi.source_order_reference)                                      as sor_prefix
        , fsoi.sales_order_item_commerce_id
        , sor_prefix || '-' || fsoi.sales_order_item_commerce_id                as sor_prefix_commerceid
        , convert_timezone('America/Los_Angeles',
                           fsoi.chosen_order_actual_shipped_at)                 as order_shipdate
        , convert_timezone('America/Los_Angeles',
                           fsoi.chosen_order_created_at)                        as order_createddate
        , convert_timezone('America/Los_Angeles',
                           fsoi.chosen_current_promised_at)                     as order_current_promise_date
        , convert_timezone('America/Los_Angeles',
                           fsoi.chosen_original_promised_at)                    as order_original_promise_date
        , convert_timezone('America/Los_Angeles',
                           fsoi.ready_to_ship_at)                               as order_ready_to_shipdate
        , fsoi.company_name
        , fsoi.gene_name
        , fsoi.source_system
        , fsoi.sales_order_status
        , nvl(fsoi.netsuite_product_line_v1,
              fsoi.netsuite_product_line_v2)                                    as product_line
        , case
              when fsoi.product_type in ('gko_kit_homo_sapiens'
              , 'multiplex_sgrna', 'tok_multiguide')
                  then 'GKO'
              when fsoi.product_type in ('crisprcas9_sgrna_ez_kit', 'ez_RNA_kit'
              , 'ez_sgRNA_oligonucleotide', 'ez_sgRNA_oligonucleotide_modified'
              , 'sgrna_cell_validated', 'sgrna_kit', 'sgrna_nobuffer'
              , 'sgrna_screening_plate')
                  then 'sgRNA'
              else 'Other'
          end                                                                   as product_category
--         , iff(product_line like any ('GKO', '%sgRNA%')
--             and nmol_units  <= 5
--             and nmol_units >= 1.5
--             , true, false)                                                      as chosen_orders --lat alias
        , iff(product_category != 'Other'
            and nmol_units  <= 5
            and nmol_units >= 1.5
            , true, false)                                                      as chosen_orders --lat alias
        , iff(datediff(day, order_original_promise_date, order_shipdate) <= 0, 1,
              0)                                                                as shipped_ontime_original
        , iff(datediff(day, order_current_promise_date, order_shipdate) <= 0, 1,
              0)                                                                as shipped_ontime_current
        , timestampdiff(minute, order_createddate, order_shipdate) /
          1440                                                                  as overall_tat
        , timestampdiff(minute, order_createddate, order_ready_to_shipdate) /
          1440                                                                  as build_tat
        , datediff(day, order_original_promise_date, order_shipdate)            as ots_daydiff_original
        , datediff(day, order_current_promise_date, order_shipdate)             as ots_daydiff_current
        , fsoi.nmol_units
        , iff(fsoi.company_name like 'Cells Synthego'
                  and fsoi.source_system = 'LIMS'
            , true
            , false)                                                            as is_internal
        , fsoi.factory_label                                                    as label
        , fsoi.work_order_group_abbreviation                                    as work_order_group
        , sbi.sequence_length                                                   as oligo_length
        , sbi.id                                                                as build_id
    from
        da_prod_db.datacore.fact_sales_order_items                              fsoi
        left join stitch.stitch_barb_prod.sales_build_item                      sbi
            on fsoi.sales_order_item_id = sbi.sales_order_item_id

    where
        fsoi.sales_order_status = 'shipped'
        and fsoi.sales_order_id not in (
            select distinct
                sales_order_id
            from
                remove_unwanted_parts
        )
        and chosen_orders
    )
select * from base