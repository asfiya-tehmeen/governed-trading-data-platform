{#- "Amara Okafor" -> "A**** O*****": keeps initials for support, hides the name. -#}
{% macro mask_name(column) -%}
    array_to_string(
        array(
            select concat(substr(part, 1, 1), repeat('*', greatest(length(part) - 1, 0)))
            from unnest(split({{ column }}, ' ')) as part with offset
            order by offset
        ),
        ' '
    )
{%- endmacro %}
