# -*- coding: utf-8 -*-
"""Group by query tests

"""

from op.collate import Collate
from op.group import Group
from op.table_scan import TableScan
from op.tuple import LabelledTuple


def test_group_count():
    """Tests a group by query with a count aggregate

    :return: None
    """

    num_rows = 0

    # Query plan
    # select s_nationkey, count(s_suppkey) from supplier.csv group by s_nationkey
    ts = TableScan('supplier.csv', 'select * from S3Object;', 'ts', False)
    g = Group([3],
              aggregate_expr_strs=[
                  'count(_0)'  # count(s_suppkey)
              ])
    c = Collate()

    ts.connect(g)
    g.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for _ in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['_0', '_1', '_2', '_3', '_4', '_5', '_6']

    nation_24 = filter(lambda t: LabelledTuple(t, field_names)['_0'] == '24', c.tuples())[0]
    assert nation_24[1] == 393
    assert num_rows == 25 + 1


def test_group_sum():
    """Tests a group by query with a sum aggregate

    :return: None
    """

    num_rows = 0

    # Query plan
    # select s_nationkey, sum(float(s_acctbal)) from supplier.csv group by s_nationkey
    ts = TableScan('supplier.csv', 'select * from S3Object;', 'ts', False)
    g = Group([3],
              aggregate_expr_strs=[
                  'sum(_5)'  # sum(l_extendedprice)
              ])
    c = Collate()

    ts.connect(g)
    g.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for _ in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['_0', '_1', '_2', '_3', '_4', '_5', '_6']

    nation_24 = filter(lambda t: LabelledTuple(t, field_names)['_0'] == '24', c.tuples())[0]
    assert round(nation_24[1], 2) == 1833872.56
    assert num_rows == 25 + 1
