/*
    TO BE RAN ON THE FIRST BUSINESS DAY OF THE MONTH FOR THE PREVIOUS MONTH
*/

alter session set timezone = 'America/Los_Angeles';
set (start_date, end_date) = (to_timestamp(date_trunc('month', current_date() - interval '1 month')), to_timestamp(date_trunc('month', current_date()) - interval '1 second'));

//Use below for monthly report
//SET start_date = TO_TIMESTAMP(add_months($end_date, -1));

//Use below for weekly report
-- set start_date = to_timestamp(dateadd('day', -31, $end_date));
-- set end_date = $end_date - interval '1 second';

-- select $start_date, $end_date from dual; -- use to validate dates

with base as (
    select
        /* sales order data */
        sales_order.source_order_reference                             as "source sales order id"
        , '=HYPERLINK("http://hook/salesorders/' || sales_order.id || '", "' || sales_order.name ||
          '")'                                                         as "barb sales order name"
        , sales_order.id                                               as "sales order pk"
        , sales_order.description                                      as "sales order description"
        , sales_order_item.id                                          as "sales order item pk"
        , sales_build_item.id                                          as "build item pk"
        , to_char(
            convert_timezone('America/Los_Angeles', sales_order.created),
            'MM/DD/YYYY'
        )                                                              as "sales order created date"
        , to_char(
            convert_timezone('America/Los_Angeles', sales_order.status_datetime),
            'MM/DD/YYYY'
        )                                                              as "sales order status date"
        , substring(
            sales_order.name, 13, 3
        )                                                              as "sales order type"
        , sales_order.status                                           as "sales order status"
        , sales_order.is_internal                                      as "is internal order"
        , sales_order.company_name                                     as "company name"
        , sales_order.contact_name                                     as "contact name"
        , sales_order_item.plate_name                                  as "shipping plate name"
        , sales_order_item.product_type                                as "buckaneer product type"
        ,
        /* oligo data */
        wip_work_order_item.id                                         as "work order item pk"
        , wip_work_order.id                                            as "work order pk"
        , '=HYPERLINK("http://hook/workorders/' || wip_work_order.id || '", "' ||
          wip_work_order.name ||
          '")'                                                         as "work order name"
        , regexp_replace(wip_work_order.description, '[\\n\\r]+', ' ') as "work order description"
        , wip_work_order_group.name                                    as "work order group"
        , wip_work_order_group.abbreviation                            as "work order group abbreviation"
        , case
              when wip_work_order.status = 'canceled'
                  then true
              else false
          end                                                          as "work order item died on vine"
        , case
              when parse_json(wip_work_order.config_data):mutable_task_parameters:resume_synthesis_cycle >
                   '0'
                  then true
              else false
          end                                                          as "is synthesized from scaffold"
        , to_char(
            convert_timezone('America/Los_Angeles', work_order_history.started_datetime),
            'MM/DD/YYYY'
        )                                                              as "synthesis work order start date"
        , to_char(
            convert_timezone('America/Los_Angeles', wip_work_order.status_datetime),
            'MM/DD/YYYY'
        )                                                              as "synthesis work order status date"
        , wip_work_order.status                                        as "synthesis status"
        , sales_order_item.customer_label                              as "customer sequence label"
        , sales_order_item.commerce_id                                 as "commerce id"
        , sales_build_item.sequence_length                             as "sequence length"
        , sales_build_item.sequence_contains_mod                       as "sequence is mod"
        , (
        case
            when sales_build_item.sequence_length between 0 and 29
                then '< 30mers'
            when sales_build_item.sequence_length between 30 and 59
                then '30-59mers'
            when sales_build_item.sequence_length between 60 and 80
                then '60-80mers'
            when sales_build_item.sequence_length between 81 and 105
                then '81-105mers'
            when sales_build_item.sequence_length between 106 and 200
                then '> 105mers'
            else 'unknown'
        end
        )                                                              as "oligomer type"
        , (
        case
            when sales_build_item.sequence_contains_mod = 'TRUE' and
                 sales_order_item.product_type like '%custom%'
                then 'custom_mod'
            when sales_build_item.sequence_contains_mod = 'FALSE' and
                 sales_order_item.product_type like '%custom%'
                then 'custom_vanilla'
            when sales_build_item.sequence_contains_mod = 'TRUE' and
                 sales_order_item.product_type not like '%custom%'
                then 'mod'
            when sales_build_item.sequence_contains_mod = 'FALSE' and
                 sales_order_item.product_type not like '%custom%'
                then 'vanilla'
            else 'unknown'
        end
        )                                                              as "sales order product type"
        , sales_build_item.four_letter_sequence                        as "four letter sequence"
        , sales_build_item.build_yield                                 as "guaranteed molar yield"
        , sample_unit.name                                             as "guaranteed molar yield unit"
        , sample_measurement."well passed qc"                          as "well passed qc"
        , wip_work_order_item.control                                  as "is control"
    from
        stitch.stitch_barb_prod.wip_work_order_item
        join stitch.stitch_barb_prod.wip_work_order
            on wip_work_order.id = wip_work_order_item.work_order_id
        join
            (
                select
                    work_order_id
                    , max(status_datetime) as started_datetime
                from
                    stitch.stitch_barb_prod.wip_work_order_history
                where
                    status = 'started'
                group by
                    work_order_id
                ) as work_order_history
                on wip_work_order.id = work_order_history.work_order_id
        join stitch.stitch_barb_prod.wip_work_order_type
            on wip_work_order_type.id = wip_work_order.work_order_type_id
        join stitch.stitch_barb_prod.wip_work_order_group
            on wip_work_order_group.id = wip_work_order.work_order_group_id
        left outer join
            (
                select
                    sample_aliquot.work_order_item_id as "work order item pk"
                    , sample_measurementbatch.qc_pass as "well passed qc"
                from
                    stitch.stitch_barb_prod.sample_aliquot
                    join
                        stitch.stitch_barb_prod.sample_measurementbatch
                            on
                            sample_aliquot.id = sample_measurementbatch.aliquot_id
                where
                    sample_measurementbatch.status = 'active'
                ) as sample_measurement
                on wip_work_order_item.id = sample_measurement."work order item pk"
        left outer join stitch.stitch_barb_prod.wip_work_order_item_source_build_items
            on wip_work_order_item.id = wip_work_order_item_source_build_items.workorderitem_id
        left outer join stitch.stitch_barb_prod.sales_build_item
            on wip_work_order_item_source_build_items.builditem_id = sales_build_item.id
        left outer join stitch.stitch_barb_prod.sample_unit
            on sample_unit.id = sales_build_item.build_yield_unit_id
        left outer join stitch.stitch_barb_prod.sales_order_item
            on sales_build_item.sales_order_item_id = sales_order_item.id
        left outer join stitch.stitch_barb_prod.sales_order
            on sales_order.id = sales_order_item.sales_order_id
    where
        wip_work_order_type.abbreviation = 'SYN'
        and (
            (
                        wip_work_order.status in ('completed', 'canceled', 'started') and
                        convert_timezone('America/Los_Angeles', wip_work_order.status_datetime) >=
                        $start_date and
                        convert_timezone('America/Los_Angeles', wip_work_order.status_datetime) <= $end_date
                )
            or
            (
                        sales_order.status = 'shipped' and
                        convert_timezone('America/Los_Angeles', sales_order.status_datetime) >=
                        $start_date and
                        convert_timezone('America/Los_Angeles', sales_order.status_datetime) <= $end_date
                )
        )
    )
select
    *
from
    base
order by
    "sales order status date"
    , "synthesis work order start date"
    , "work order pk"
    , "work order item pk"
limit 100
