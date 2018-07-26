# -*- coding: utf-8 -*-
"""TPCH Q14 Semi Join Benchmark

"""

import os
from datetime import datetime, timedelta

from s3filter import ROOT_DIR
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q14
from s3filter.util.test_util import gen_test_id


def main():
    run(0)
    run(1024)


def run(buffer_size):
    """

      :return: None
      """

    print('')
    print("TPCH Q14 Semi Join")
    print("------------------")

    query_plan = QueryPlan(is_async=False, buffer_size=buffer_size)

    # Query plan
    date = '1993-01-01'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    part_scan_1 = query_plan.add_operator(
        tpch_q14.sql_scan_part_partkey_where_brand12_operator_def('part_table_scan_1', query_plan))
    part_scan_1_project = query_plan.add_operator(
        tpch_q14.project_p_partkey_operator_def('part_scan_1_project', query_plan))
    part_bloom_create = query_plan.add_operator(
        tpch_q14.bloom_create_p_partkey_operator_def('part_bloom_create', query_plan))
    lineitem_scan_1 = query_plan.add_operator(
        tpch_q14.bloom_scan_lineitem_partkey_where_shipdate_operator_def(min_shipped_date, max_shipped_date,
                                                                         'lineitem_scan_1', query_plan))
    lineitem_scan_1_project = query_plan.add_operator(
        tpch_q14.project_l_partkey_operator_def('lineitem_scan_1_project', query_plan))
    part_lineitem_join_1 = query_plan.add_operator(
        tpch_q14.join_part_lineitem_operator_def('part_lineitem_join_1', query_plan))
    join_bloom_create = query_plan.add_operator(
        tpch_q14.bloom_create_l_partkey_operator_def('join_bloom_create', query_plan))
    part_scan_2 = query_plan.add_operator(
        tpch_q14.bloom_scan_part_partkey_type_brand12_operator_def('part_table_scan_2', query_plan))
    part_scan_2_project = query_plan.add_operator(
        tpch_q14.project_partkey_type_operator_def('part_scan_2_project', query_plan))
    lineitem_scan_2 = query_plan.add_operator(
        tpch_q14.bloom_scan_lineitem_where_shipdate_operator_def(min_shipped_date, max_shipped_date, 'lineitem_scan_2',
                                                                 query_plan))
    lineitem_scan_2_project = query_plan.add_operator(
        tpch_q14.project_partkey_extendedprice_discount_operator_def('lineitem_scan_2_project', query_plan))
    part_lineitem_join_2 = query_plan.add_operator(
        tpch_q14.join_part_lineitem_operator_def('part_lineitem_join_2', query_plan))
    aggregate = query_plan.add_operator(tpch_q14.aggregate_promo_revenue_operator_def('aggregate', query_plan))
    aggregate_project = query_plan.add_operator(
        tpch_q14.project_promo_revenue_operator_def('aggregate_project', query_plan))
    collate = query_plan.add_operator(tpch_q14.collate_operator_def('collate', query_plan))

    part_scan_1.connect(part_scan_1_project)
    part_scan_1_project.connect(part_bloom_create)
    part_bloom_create.connect(lineitem_scan_1)
    part_lineitem_join_1.connect_left_producer(part_scan_1_project)
    lineitem_scan_1.connect(lineitem_scan_1_project)
    part_lineitem_join_1.connect_right_producer(lineitem_scan_1_project)
    part_lineitem_join_1.connect(join_bloom_create)
    join_bloom_create.connect(part_scan_2)
    join_bloom_create.connect(lineitem_scan_2)
    part_scan_2.connect(part_scan_2_project)
    part_lineitem_join_2.connect_left_producer(part_scan_2_project)
    lineitem_scan_2.connect(lineitem_scan_2_project)
    part_lineitem_join_2.connect_right_producer(lineitem_scan_2_project)
    part_lineitem_join_2.connect(aggregate)
    aggregate.connect(aggregate_project)
    aggregate_project.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in collate.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    collate.print_tuples()

    # Write the metrics
    query_plan.print_metrics()

    field_names = ['promo_revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [15.090116526324298]


if __name__ == "__main__":
    main()
