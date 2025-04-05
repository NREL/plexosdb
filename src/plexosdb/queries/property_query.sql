SELECT
    o.name AS name,
    p.name AS property,
    d.value AS property_value,
    REPLACE(GROUP_CONCAT(DISTINCT txt.value), ',', '; ') AS text,
    REPLACE(GROUP_CONCAT(DISTINCT pt.name), ',', '; ') AS tags,
    REPLACE(GROUP_CONCAT(DISTINCT band.band_id), ',', '; ') AS bands,
    scenario.scenario_name AS scenario,
    scenario.scenario_category AS scenario_category,
    u.value AS unit
FROM t_object AS o
LEFT JOIN t_class AS c
    ON o.class_id = c.class_id
LEFT JOIN t_category AS cat
    ON o.category_id = cat.category_id
LEFT JOIN t_membership AS m
    ON m.child_object_id = o.object_id
LEFT JOIN t_data AS d
    ON d.membership_id = m.membership_id
LEFT JOIN t_property AS p
    ON d.property_id = p.property_id
LEFT JOIN t_unit AS u
    ON p.unit_id = u.unit_id
LEFT JOIN t_text AS txt
    ON d.data_id = txt.data_id
LEFT JOIN t_tag AS tag
    ON d.data_id = tag.data_id
LEFT JOIN t_property_tag AS pt
    ON tag.action_id = pt.tag_id
LEFT JOIN t_band AS band
    ON d.data_id = band.data_id
LEFT JOIN (
    SELECT
        t.data_id,
        obj.name AS scenario_name,
        cat.name AS scenario_category
    FROM t_membership AS mem
    LEFT JOIN t_tag AS t
        ON t.object_id = mem.child_object_id
    LEFT JOIN t_object AS obj
        ON mem.child_object_id = obj.object_id
    LEFT JOIN t_category AS cat
        ON obj.category_id = cat.category_id
    LEFT JOIN t_class AS c_scen
        ON mem.child_class_id = c_scen.class_id
    WHERE c_scen.name = 'Scenario'
) AS scenario
    ON scenario.data_id = d.data_id
WHERE c.name = '{class_name}'
  {extra_filters}
GROUP BY
    d.data_id, o.name, p.name, d.value,
    scenario.scenario_name, scenario.scenario_category, u.value;
