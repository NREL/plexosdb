WITH scenario_cte AS (
    SELECT
        obj.name,
        obj.object_id,
        tag.data_id,
        mem.collection_id
    FROM
        t_membership AS mem
    INNER JOIN t_tag AS tag ON
        tag.object_id = mem.child_object_id
    INNER JOIN t_object AS obj ON
        mem.child_object_id = obj.object_id
    WHERE
        mem.child_class_id = 78
        AND mem.collection_id IN (1, 698, 706, 700) -- Collections belong to scenarios
),
tag_cte AS (
    SELECT
        obj.object_id,
        obj.name AS nested_object,
        prop.name AS nested_property_name,
        text.value AS text,
        d.data_id as t_data_id,
        d.value,
        tag.data_id AS tag_data_id,
        date_from.date AS date_from,
        date_to.date AS date_to,
        memo.value AS memo,
        scenario.name AS scenario,
        text.class_id AS text_class_id,
        text.action_id AS text_action_id,
        class_text.name AS class_text_name,
        action.action_symbol AS action_symbol
    FROM
        t_membership AS mem
    LEFT JOIN t_data AS d ON
        mem.membership_id = d.membership_id
    LEFT JOIN t_property AS prop ON
        prop.property_id = d.property_id
    LEFT JOIN t_memo_data AS memo ON
        memo.data_id = d.data_id
    LEFT JOIN t_date_from AS date_from ON
        d.data_id = date_from.data_id
    LEFT JOIN t_date_to AS date_to ON
        d.data_id = date_to.data_id
    INNER JOIN t_text AS text ON
        text.data_id = d.data_id
    INNER JOIN t_object AS obj ON
        mem.child_object_id = obj.object_id
    INNER JOIN t_tag AS tag ON
        tag.object_id = obj.object_id
    LEFT JOIN t_class AS class_text ON
        text.class_id = class_text.class_id
    LEFT JOIN t_action AS action ON
        text.action_id = action.action_id
    LEFT JOIN scenario_cte AS scenario ON
        d.data_id = scenario.data_id
)
SELECT
    mem.membership_id,
    class_parent.class_id AS parent_class_id,
    mem.parent_object_id as parent_object_id,
    class_parent.name AS parent_class,
    class_child.class_id AS child_class_id,
    class_child.name AS child_class,
    cat.name AS category,
    child_obj.object_id as child_object_id,
    child_obj.name as child_object_name,
    prop.name AS property_name,
    unit.value AS property_unit,
    data.value as property_value,
    IFNULL(band.band_id, 1) as band,
    COALESCE(date_from.date, nested_object.date_from) as date_from,
    COALESCE(date_to.date, nested_object.date_to) as date_to,
    COALESCE(memo.value, nested_object.memo) as memo,
    COALESCE(scenario.name, nested_object.scenario) AS scenario,
    MAX(CASE WHEN tag_object_class.name = 'Scenario' THEN tag_object.name END) AS scenario_tag,
    MAX(CASE WHEN tag_object_class.name = 'Variable' THEN tag_object.name END) AS var_tag,
    MAX(CASE WHEN tag_object_class.name = 'Data File' THEN tag_object.name END) AS data_file_tag,
    MAX(CASE WHEN tag_object_class.name = 'Timeslice' THEN tag_object.name END) AS timeslice_tag,
    MAX(action.action_symbol) AS action_symbol,
    MAX(VAR_TEXT.value) AS var_text,
    MAX(CASE WHEN class_text_name = 'Data File' THEN text END) AS data_file,
    MAX(CASE WHEN class_text_name = 'Timeslice' THEN text END) AS timeslice,
    MAX(CASE WHEN class_text_name = 'Variable' THEN text END) AS variable
FROM
    t_membership AS mem
LEFT JOIN t_class AS class_parent ON
    mem.parent_class_id = class_parent.class_id
LEFT JOIN t_class AS class_child ON
    mem.child_class_id = class_child.class_id
LEFT JOIN t_object AS child_obj ON
    child_obj.object_id = mem.child_object_id
LEFT JOIN t_data AS data ON
    data.membership_id = mem.membership_id
LEFT JOIN t_memo_data AS memo ON
    memo.data_id = data.data_id
LEFT JOIN t_date_from AS date_from ON
    data.data_id = date_from.data_id
LEFT JOIN t_date_to AS date_to ON
    data.data_id = date_to.data_id
LEFT JOIN t_property AS prop ON
    data.property_id = prop.property_id
LEFT JOIN t_unit AS unit ON
    unit.unit_id = prop.unit_id
LEFT JOIN t_band AS band ON
    data.data_id = band.data_id
LEFT JOIN scenario_cte AS scenario ON
    scenario.data_id = data.data_id
LEFT JOIN t_category AS cat ON
    child_obj.category_id = cat.category_id
LEFT JOIN tag_cte AS nested_object ON
    data.data_id = nested_object.tag_data_id
LEFT JOIN t_tag AS tag ON
    data.data_id = tag.data_id
LEFT JOIN t_object AS tag_object ON
    tag_object.object_id = tag.object_id
LEFT JOIN t_class as tag_object_class ON
    tag_object.class_id = tag_object_class.class_id
LEFT JOIN t_action as action ON
    action.action_id = tag.action_id
-- backtrack to bring in the variable object 
LEFT JOIN t_membership AS VAR_tag_obj_mem ON
    VAR_tag_obj_mem.child_object_id = tag_object.object_id
LEFT JOIN t_data as VAR_tag_obj_data ON
    VAR_tag_obj_mem.membership_id = VAR_tag_obj_data.membership_id
LEFT JOIN t_tag as VAR_tag_obj_data_tag ON
    VAR_tag_obj_data.data_id = VAR_tag_obj_data_tag.data_id
LEFT JOIN t_membership AS VAR_MEM_BASE ON
    VAR_tag_obj_data_tag.object_id = VAR_MEM_BASE.child_object_id
LEFT JOIN t_data as VAR_BASE ON
    VAR_MEM_BASE.membership_id = VAR_BASE.membership_id
LEFT JOIN t_text as VAR_TEXT ON
    VAR_BASE.data_id = VAR_TEXT.data_id
GROUP BY
    class_parent.class_id,
    mem.parent_object_id,
    class_parent.name,
    class_child.class_id,
    class_child.name,
    cat.name,
    child_obj.object_id,
    child_obj.name,
    prop.name,
    unit.value,
    data.value,
    IFNULL(band.band_id, 1),
    COALESCE(date_from.date, nested_object.date_from),
    COALESCE(date_to.date, nested_object.date_to),
    COALESCE(memo.value, nested_object.memo),
    COALESCE(scenario.name, nested_object.scenario),
    text