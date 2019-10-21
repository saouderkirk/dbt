{% macro dbt__incremental_delete(target_relation, tmp_relation) -%}

  {%- set unique_key = config.require('unique_key') -%}

  delete
  from {{ target_relation }}
  where ({{ unique_key }}) in (
    select ({{ unique_key }})
    from {{ tmp_relation.include(schema=False, database=False) }}
  );

{%- endmacro %}

-- Ideally, you should be able to override this macro to define what you want to consider a schema change. The default is to
-- look at the following between the new relation and the old relation:
--    1. Addition of new column
--    2. Deletion of column
--    3. Rename of column
--    4. Changing of column data type
{% macro dbt__default_has_schema_changed(on_schema_change_fail, old_relation, target_relation) -%}

{% if on_schema_change_fail and adapter.target_contains_schema_change(old_relation=old_relation, to_relation=target_relation) -%}
  {{ exceptions.raise_fail_on_schema_change() }}
{%- endif %}

{%- endmacro %}

{% materialization incremental, default -%}
  {%- set unique_key = config.get('unique_key') -%}

  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_incremental_tmp' -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database,  type='table') -%}
  {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                             schema=schema,
                                             database=database, type='table') -%}

  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {%- set on_schema_change_fail = (config.get('on_schema_change') == 'fail') -%}

  {%- set on_schema_change_full_refresh = (config.get('on_schema_change') == 'full_refresh') -%}

  {% if on_schema_change_full_refresh and adapter.target_contains_schema_change(old_relation=old_relation, to_relation=target_relation) %}
    {%- set full_refresh_mode = True -%}
  {% endif %}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_not_as_table = (old_relation is not none and not old_relation.is_table) -%}

  {%- set should_drop = (full_refresh_mode or exists_not_as_table) -%}

  -- setup
  {% if old_relation is none -%}
    -- noop
  {%- elif should_drop -%}
    {{ adapter.drop_relation(old_relation) }}
    {%- set old_relation = none -%}
  {%- endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% if full_refresh_mode or old_relation is none -%}
    {%- call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall -%}
  {%- else -%}
     {%- call statement() -%}

       {{ dbt.create_table_as(True, tmp_relation, sql) }}

     {%- endcall -%}

     {{ dbt__default_has_schema_changed(on_schema_change_fail, old_relation, tmp_relation) }}

     {{ adapter.expand_target_column_types(temp_table=tmp_identifier,
                                           to_relation=target_relation) }}

     {%- call statement('main') -%}
       {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
       {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

       {% if unique_key is not none -%}

         {{ dbt__incremental_delete(target_relation, tmp_relation) }}

       {%- endif %}

       insert into {{ target_relation }} ({{ dest_cols_csv }})
       (
         select {{ dest_cols_csv }}
         from {{ tmp_relation.include(schema=False, database=False) }}
       );
     {% endcall %}
  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

{%- endmaterialization %}
