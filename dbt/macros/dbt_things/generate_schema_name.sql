{% macro generate_schema_name(custom_schema_name, node) %}
    {#
    Generates the schema name based on the provided custom schema name or defaults to the target schema.

    Args:
        custom_schema_name (str or None): The custom schema name, if provided.
        node (dict): The dbt node object (unused but included for compatibility).

    Returns:
        str: The resolved schema name.
    #}
    {{ custom_schema_name | trim if custom_schema_name is not none else target.schema }}
{% endmacro %}
