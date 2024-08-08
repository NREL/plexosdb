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
        obj.name AS nested_object_name,
        prop.name AS nested_property_name,
        tag.data_id AS tag_data_id,
        date_from.date AS date_from,
        date_to.date AS date_to,
        memo.value AS memo,
        scenario.name AS scenario
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
    LEFT JOIN scenario_cte AS scenario ON
        d.data_id = scenario.data_id
)
SELECT
    mem.membership_id,
    mem.parent_object_id AS parent_object_id,
    class_parent.name AS parent_class,
    class_child.name AS child_class,
    cat.name AS category,
    child_obj.object_id AS child_object_id,
    child_obj.name AS child_object_name,
    prop.name AS property_name,
    unit.value AS property_unit,
    data.value AS property_value,
    IFNULL(band.band_id, 1) AS band,
    COALESCE(date_from.date, nested_object.date_from) AS date_from,
    COALESCE(date_to.date, nested_object.date_to) AS date_to,
    COALESCE(memo.value, nested_object.memo) AS memo,
    COALESCE(scenario.name, nested_object.scenario) AS scenario,
    MAX(CASE WHEN tagged_object_class.name = 'Scenario' THEN tagged_object.name END) AS scenario_tag,
    action.action_symbol AS action_symbol,
    VAR_tag_obj.name AS var_tag_obj_name,
    MAX(CASE WHEN tagged_object_class.name = 'Variable' THEN tagged_object.name END) AS var_tag,
    VAR_TEXT.value AS variable,
    MAX(CASE WHEN tagged_object_class.name = 'Data File' THEN tagged_object.name END) AS data_file_tag,
    MAX(CASE WHEN tagged_object_text_class.name = 'Data File' THEN tagged_object_text.value END) AS data_file,
    MAX(CASE WHEN tagged_object_class.name = 'Timeslice' THEN tagged_object.name END) AS timeslice_tag,
    MAX(CASE WHEN tagged_object_text_class.name = 'Timeslice' THEN tagged_object_text.value END) AS timeslice,
    MAX(CASE WHEN tagged_object_text_class.name = 'Variable' THEN tagged_object_text.value END) AS variable_alt
FROM
    t_membership AS mem
LEFT JOIN t_class AS class_parent ON
    mem.parent_class_id = class_parent.class_id
LEFT JOIN t_object AS child_obj ON -- generator objects
    child_obj.object_id = mem.child_object_id
LEFT JOIN t_class AS class_child ON
    mem.child_class_id = class_child.class_id
LEFT JOIN t_category AS cat ON  -- gen category
    child_obj.category_id = cat.category_id
----------- generator property data
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
------------ ^ t_data information ^ ------------
LEFT JOIN scenario_cte AS scenario ON
    scenario.data_id = data.data_id
LEFT JOIN tag_cte AS nested_object ON
    data.data_id = nested_object.tag_data_id
-------------------------------------------------
LEFT JOIN t_tag AS tag ON
    data.data_id = tag.data_id
LEFT JOIN t_object AS tagged_object ON
    tagged_object.object_id = tag.object_id
LEFT JOIN t_class AS tagged_object_class ON
    tagged_object.class_id = tagged_object_class.class_id
LEFT JOIN t_action AS action ON
    action.action_id = tag.action_id
-- backtrack to bring in the variable object 
LEFT JOIN t_membership AS VAR_tag_obj_mem ON
    VAR_tag_obj_mem.child_object_id = tagged_object.object_id
LEFT JOIN t_data AS VAR_tag_obj_data ON
    VAR_tag_obj_mem.membership_id = VAR_tag_obj_data.membership_id
LEFT JOIN t_tag AS VAR_tag_obj_data_tag ON
    VAR_tag_obj_data.data_id = VAR_tag_obj_data_tag.data_id
LEFT JOIN t_object AS VAR_tag_obj ON
    VAR_tag_obj_data_tag.object_id = VAR_tag_obj.object_id
LEFT JOIN t_membership AS VAR_MEM_BASE ON
    VAR_tag_obj_data_tag.object_id = VAR_MEM_BASE.child_object_id
LEFT JOIN t_data AS VAR_BASE ON
    VAR_MEM_BASE.membership_id = VAR_BASE.membership_id
LEFT JOIN t_text AS VAR_TEXT ON -- Variable text
    VAR_BASE.data_id = VAR_TEXT.data_id
LEFT JOIN t_text AS tagged_object_text ON -- Tagged object data text
    tagged_object_text.data_id = VAR_tag_obj_data.data_id
LEFT JOIN t_class AS tagged_object_text_class ON -- Tagged object data text class
    tagged_object_text.class_id = tagged_object_text_class.class_id
GROUP BY
    mem.parent_object_id,
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
    VAR_tag_obj.name,
    action.action_symbol,
    VAR_TEXT.value,
    VAR_tag_obj_data.data_id