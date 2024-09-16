WITH scenario_cte AS (
    SELECT
        obj.name,
        obj.object_id,
        tag.data_id,
        mem.collection_id,
        cat.name AS category_name
    FROM
        t_membership AS mem
    INNER JOIN t_tag AS tag ON
        tag.object_id = mem.child_object_id
    INNER JOIN t_object AS obj ON
        mem.child_object_id = obj.object_id
    LEFT JOIN t_category AS cat ON
    	obj.category_id = cat.category_id
    WHERE
        mem.child_class_id = 78
        AND mem.collection_id IN (1, 698, 706, 701) -- Collections belong to scenarios
),
text_cte AS (
    SELECT
        base_gen_data.data_id AS parent_object_data_id,
        obj.object_id,
        obj.name AS nested_object_name,
        prop.name AS nested_property_name,
        memo.value AS memo,
        text.value as text_value,
        text_class.name ,
        date_from.date AS date_from,
        date_to.date AS date_to,
        text.action_id,
        tagged_object.name AS tagged_object_name,
        scenario.name AS scenario,
        text_tag_obj_data.value AS base_value,
        CASE
            WHEN text_class.name IN ('Data File') THEN 'Data File'
            WHEN text_class.name IN ('Timeslice') THEN 'Timeslice'
        END AS text_class_type
    FROM
        t_membership AS mem
    INNER JOIN t_object AS obj ON
        mem.child_object_id = obj.object_id
    LEFT JOIN t_data AS base_gen_data ON
        mem.membership_id = base_gen_data.membership_id
    LEFT JOIN t_property AS prop ON
        prop.property_id = base_gen_data.property_id
    LEFT JOIN t_memo_data AS memo ON
        memo.data_id = base_gen_data.data_id
    LEFT JOIN t_date_from AS date_from ON
        base_gen_data.data_id = date_from.data_id
    LEFT JOIN t_date_to AS date_to ON
        base_gen_data.data_id = date_to.data_id
    ----- nested text data
    INNER JOIN t_tag AS tag ON
        tag.data_id = base_gen_data.data_id
    LEFT JOIN t_object AS tagged_object ON
        tagged_object.object_id = tag.object_id
    LEFT JOIN t_class AS tagged_object_class ON
        tagged_object.class_id = tagged_object_class.class_id
    LEFT JOIN t_action AS action ON
        action.action_id = tag.action_id
    -- bring in the text object
    INNER JOIN t_membership AS text_tag_obj_mem ON
        text_tag_obj_mem.child_object_id = tagged_object.object_id
    INNER JOIN t_data AS text_tag_obj_data ON
        text_tag_obj_mem.membership_id = text_tag_obj_data.membership_id
    INNER JOIN t_text AS text ON
        text.data_id = text_tag_obj_data.data_id
    LEFT JOIN t_class AS text_class on
        text.class_id = text_class.class_id
    LEFT JOIN scenario_cte AS scenario ON
        base_gen_data.data_id = scenario.data_id
    WHERE text_class.name IN ('Data File', 'Timeslice')
),
tag_cte AS (
SELECT
    base_gen_data.data_id AS parent_object_data_id,
    obj.object_id,
    obj.name AS nested_object_name,
    prop.name AS nested_property_name,
    tagged_object.name AS tagged_object_name,
    tagged_object_class.name AS tagged_obj_class_name,
    action.action_symbol AS action_symbol
FROM
    t_membership AS mem
INNER JOIN t_object AS obj ON
    mem.child_object_id = obj.object_id
LEFT JOIN t_data AS base_gen_data ON
    mem.membership_id = base_gen_data.membership_id
LEFT JOIN t_property AS prop ON
    prop.property_id = base_gen_data.property_id
INNER JOIN t_tag AS tag ON
    tag.data_id = base_gen_data.data_id
LEFT JOIN t_object AS tagged_object ON
    tagged_object.object_id = tag.object_id
LEFT JOIN t_class AS tagged_object_class ON
    tagged_object.class_id = tagged_object_class.class_id
LEFT JOIN t_action AS action ON
    action.action_id = tag.action_id
WHERE tagged_obj_class_name == "Variable"
)
SELECT
    mem.membership_id,
    mem.parent_object_id AS parent_object_id,
    parent_object.name AS parent_object_name,
    class_parent.name AS parent_class,
    class_child.name AS child_class,
    cat.name AS category,
    child_obj.object_id AS child_object_id,
    child_obj.name AS child_object_name,
    prop.name AS property_name,
    unit.value AS property_unit,
    data.value AS property_value,
    IFNULL(band.band_id, 1) AS band,
    COALESCE(date_from.date, nested_object_df.date_from) AS date_from,
    COALESCE(date_to.date, nested_object_df.date_to) AS date_to,
    COALESCE(memo.value, nested_object_df.memo) AS memo,
    scenario.category_name AS scenario_category,
    COALESCE(scenario.name, nested_object_df.scenario) AS scenario,
    nested_variable_object.action_symbol AS action_symbol,
	nested_object_df.tagged_object_name AS data_file_tag,
	nested_object_df.text_value AS data_file,
	nested_variable_object.tagged_object_name AS varible_tag,
	nested_object_ts.tagged_object_name AS timeslice_tag,
	nested_object_ts.text_value AS timeslice,
	nested_object_ts.base_value AS timeslice_base_value,
	data_text.value AS data_text
FROM
    t_membership AS mem
LEFT JOIN t_class AS class_parent ON
    mem.parent_class_id = class_parent.class_id
LEFT JOIN t_object AS parent_object ON
	mem.parent_object_id = parent_object.object_id
LEFT JOIN t_object AS child_obj ON
    child_obj.object_id = mem.child_object_id
LEFT JOIN t_class AS class_child ON
    mem.child_class_id = class_child.class_id
LEFT JOIN t_category AS cat ON
    child_obj.category_id = cat.category_id
-------- property data -----------------------
LEFT JOIN t_data AS data ON
    data.membership_id = mem.membership_id
LEFT JOIN t_text AS data_text ON
    data_text.data_id = data.data_id
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
------------ CTE Fields ------------
LEFT JOIN scenario_cte AS scenario ON
    scenario.data_id = data.data_id
LEFT JOIN text_cte AS nested_object_df ON
    data.data_id = nested_object_df.parent_object_data_id
	AND nested_object_df.text_class_type = 'Data File'
LEFT JOIN text_cte AS nested_object_ts ON
    data.data_id = nested_object_ts.parent_object_data_id
	AND nested_object_ts.text_class_type = 'Timeslice'
LEFT JOIN tag_cte AS nested_variable_object ON
	data.data_id = nested_variable_object.parent_object_data_id
