class SyntheticJoinSettings(object):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 table_A_key,
                 table_A_parts,
                 table_A_sharded,
                 table_A_field_names,
                 table_A_AB_join_key,
                 table_B_key,
                 table_B_parts,
                 table_B_sharded,
                 table_B_field_names,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_BC_join_key,
                 table_C_detail_field_name):
        self.parallel = parallel
        self.use_pandas = use_pandas
        self.secure = secure
        self.use_native = use_native
        self.buffer_size = buffer_size
        self.table_A_key = table_A_key
        self.table_A_parts = table_A_parts
        self.table_A_sharded = table_A_sharded
        self.table_A_field_names = table_A_field_names
        self.table_A_AB_join_key = table_A_AB_join_key
        self.table_B_key = table_B_key
        self.table_B_parts = table_B_parts
        self.table_B_sharded = table_B_sharded
        self.table_B_field_names = table_B_field_names
        self.table_B_AB_join_key = table_B_AB_join_key
        self.table_B_BC_join_key = table_B_BC_join_key
        self.table_C_key = table_C_key
        self.table_C_parts = table_C_parts
        self.table_C_sharded = table_C_sharded
        self.table_C_field_names = table_C_field_names
        self.table_C_BC_join_key = table_C_BC_join_key
        self.table_C_detail_field_name = table_C_detail_field_name


class SyntheticBaselineJoinSettings(SyntheticJoinSettings):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 table_A_key,
                 table_A_parts,
                 table_A_sharded,
                 table_A_field_names,
                 table_A_filter_fn,
                 table_A_AB_join_key,
                 table_B_key,
                 table_B_parts,
                 table_B_sharded,
                 table_B_field_names,
                 table_B_filter_fn,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_filter_fn,
                 table_C_BC_join_key,
                 table_C_detail_field_name):
        super(SyntheticBaselineJoinSettings, self).__init__(parallel,
                                                            use_pandas,
                                                            secure,
                                                            use_native,
                                                            buffer_size,
                                                            table_A_key,
                                                            table_A_parts,
                                                            table_A_sharded,
                                                            table_A_field_names,
                                                            table_A_AB_join_key,
                                                            table_B_key,
                                                            table_B_parts,
                                                            table_B_sharded,
                                                            table_B_field_names,
                                                            table_B_AB_join_key,
                                                            table_B_BC_join_key,
                                                            table_C_key,
                                                            table_C_parts,
                                                            table_C_sharded,
                                                            table_C_field_names,
                                                            table_C_BC_join_key,
                                                            table_C_detail_field_name)

        self.table_A_filter_fn = table_A_filter_fn
        self.table_B_filter_fn = table_B_filter_fn
        self.table_C_filter_fn = table_C_filter_fn


class SyntheticFilteredJoinSettings(SyntheticJoinSettings):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 table_A_key,
                 table_A_parts,
                 table_A_sharded,
                 table_A_field_names,
                 table_A_filter_sql,
                 table_A_AB_join_key,
                 table_B_key,
                 table_B_parts,
                 table_B_sharded,
                 table_B_field_names,
                 table_B_filter_sql,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_filter_sql,
                 table_C_BC_join_key,
                 table_C_detail_field_name):
        super(SyntheticFilteredJoinSettings, self).__init__(parallel,
                                                            use_pandas,
                                                            secure,
                                                            use_native,
                                                            buffer_size,
                                                            table_A_key,
                                                            table_A_parts,
                                                            table_A_sharded,
                                                            table_A_field_names,
                                                            table_A_AB_join_key,
                                                            table_B_key,
                                                            table_B_parts,
                                                            table_B_sharded,
                                                            table_B_field_names,
                                                            table_B_AB_join_key,
                                                            table_B_BC_join_key,
                                                            table_C_key,
                                                            table_C_parts,
                                                            table_C_sharded,
                                                            table_C_field_names,
                                                            table_C_BC_join_key,
                                                            table_C_detail_field_name)

        self.table_A_filter_sql = table_A_filter_sql
        self.table_B_filter_sql = table_B_filter_sql
        self.table_C_filter_sql = table_C_filter_sql


class SyntheticBloomJoinSettings(SyntheticJoinSettings):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 table_A_key,
                 table_A_parts,
                 table_A_sharded,
                 table_A_field_names,
                 table_A_filter_sql,
                 table_A_AB_join_key,
                 table_B_key,
                 table_B_parts,
                 table_B_sharded,
                 table_B_field_names,
                 table_B_filter_sql,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_filter_sql,
                 table_C_BC_join_key,
                 table_C_detail_field_name):
        super(SyntheticBloomJoinSettings, self).__init__(parallel,
                                                         use_pandas,
                                                         secure,
                                                         use_native,
                                                         buffer_size,
                                                         table_A_key,
                                                         table_A_parts,
                                                         table_A_sharded,
                                                         table_A_field_names,
                                                         table_A_AB_join_key,
                                                         table_B_key,
                                                         table_B_parts,
                                                         table_B_sharded,
                                                         table_B_field_names,
                                                         table_B_AB_join_key,
                                                         table_B_BC_join_key,
                                                         table_C_key,
                                                         table_C_parts,
                                                         table_C_sharded,
                                                         table_C_field_names,
                                                         table_C_BC_join_key,
                                                         table_C_detail_field_name)

        self.table_A_filter_sql = table_A_filter_sql
        self.table_B_filter_sql = table_B_filter_sql
        self.table_C_filter_sql = table_C_filter_sql


class SyntheticSemiJoinSettings(SyntheticJoinSettings):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 table_A_key,
                 table_A_parts,
                 table_A_sharded,
                 table_A_field_names,
                 table_A_filter_sql,
                 table_A_AB_join_key,
                 table_B_key,
                 table_B_parts,
                 table_B_sharded,
                 table_B_field_names,
                 table_B_filter_sql,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_filter_sql,
                 table_C_BC_join_key,
                 table_C_primary_key,
                 table_C_detail_field_name):
        super(SyntheticSemiJoinSettings, self).__init__(parallel,
                                                        use_pandas,
                                                        secure,
                                                        use_native,
                                                        buffer_size,
                                                        table_A_key,
                                                        table_A_parts,
                                                        table_A_sharded,
                                                        table_A_field_names,
                                                        table_A_AB_join_key,
                                                        table_B_key,
                                                        table_B_parts,
                                                        table_B_sharded,
                                                        table_B_field_names,
                                                        table_B_AB_join_key,
                                                        table_B_BC_join_key,
                                                        table_C_key,
                                                        table_C_parts,
                                                        table_C_sharded,
                                                        table_C_field_names,
                                                        table_C_BC_join_key,
                                                        table_C_detail_field_name)

        self.table_A_filter_sql = table_A_filter_sql
        self.table_B_filter_sql = table_B_filter_sql
        self.table_C_filter_sql = table_C_filter_sql
        self.table_C_primary_key = table_C_primary_key