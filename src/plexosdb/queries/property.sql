SELECT
    d.data_id,
    o.name AS name,
    p.name AS property,
    d.value AS property_value,
    u.value AS unit
FROM t_data d
JOIN t_property p ON d.property_id = p.property_id
JOIN t_membership m ON d.membership_id = m.membership_id
JOIN t_object o ON m.child_object_id = o.object_id
LEFT JOIN t_unit u  ON p.unit_id = u.unit_id
WHERE d.data_id IN ({placeholders});
