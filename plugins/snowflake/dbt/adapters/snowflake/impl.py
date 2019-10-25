from __future__ import absolute_import

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.snowflake import SnowflakeConnectionManager
from dbt.adapters.snowflake import SnowflakeRelation
from dbt.utils import filter_null_values
from dbt.logger import GLOBAL_LOGGER as logger


GET_COLUMNS_IN_RELATION_MACRO_NAME = 'snowflake__get_columns_in_relation'

class SnowflakeAdapter(SQLAdapter):
    Relation = SnowflakeRelation
    ConnectionManager = SnowflakeConnectionManager

    AdapterSpecificConfigs = frozenset(
        {"transient", "cluster_by", "automatic_clustering"}
    )

    @classmethod
    def date_function(cls):
        return "CURRENT_TIMESTAMP()"

    @classmethod
    def _catalog_filter_table(cls, table, manifest):
        # On snowflake, users can set QUOTED_IDENTIFIERS_IGNORE_CASE, so force
        # the column names to their lowercased forms.
        lowered = table.rename(
            column_names=[c.lower() for c in table.column_names]
        )
        return super(SnowflakeAdapter, cls)._catalog_filter_table(
            lowered, manifest
        )
    
    def get_columns_in_relation(self, relation):
        return self.execute_macro(
            GET_COLUMNS_IN_RELATION_MACRO_NAME,
            kwargs={'relation': relation}
        )

    def _make_match_kwargs(self, database, schema, identifier):
        quoting = self.config.quoting
        if identifier is not None and quoting["identifier"] is False:
            identifier = identifier.upper()

        if schema is not None and quoting["schema"] is False:
            schema = schema.upper()

        if database is not None and quoting["database"] is False:
            database = database.upper()

        return filter_null_values(
            {"identifier": identifier, "schema": schema, "database": database}
        )
    
    def has_schema_changed(self, goal, current):
        """
        Look for schema changes between the target columns and reference columns
        Step through each column and return that a schema change *has* happened
        if any of the following are true:
        1. Number of columns are different (column has been added or removed)
        2. Columns have different names
        3. Columns have different data type
        4. Columns have different data type size
        """
        reference_columns = {
            c.name: c for c in
            self.get_columns_in_relation(goal)
        }

        # Theoretically this works, but postgres temporary tables are held within their
        # own schema, so it's not possible to track the differences.
        # This logic should work with Snowflake based on manual testing, but not confirmed
        # in this code path.
        target_columns = {
            c.name: c for c in
            self.get_columns_in_relation(current)
        }

        # 1. The schema has changed if columns have been added or removed
        # if len(reference_columns) != len(target_columns):
        #     logger.debug("Schema difference detected: Reason 1")
        #     return True

        for reference_column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(reference_column_name)
            # 2a. The schema has changed if a reference column is not found in the target columns
            if target_column is None:
                logger.debug("Schema difference detected: Reason 2a")
                logger.debug('target' = target_columns)
                logger.debug('ref'= reference_columns)
                return True

            # 3/4. If the columns do not have the same data type and size (see core/dbt/schema.py for more details)
            if reference_column.data_type != target_column.data_type:
                logger.debug("Schema difference detected: Reason 3/4")
                return True

        for i, target_column_name in enumerate(target_columns):
            reference_column = reference_columns.get(target_column_name)
            target_column = target_columns.get(target_column_name)
            # 2b. The schema has changed if a target column is not found in the reference columns
            if reference_column is None:
                logger.debug("Schema difference detected: Reason 2b")
                return True

        # Nothing has detected as changed
        logger.debug("No schema difference detected")
        return False
