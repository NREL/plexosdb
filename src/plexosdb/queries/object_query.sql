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
	AND mem.collection_id IN (1, 698, 706, 700) -- Collections belongs to scenarios
),
text_cte AS (
SELECT
	obj.object_id,
	obj.name AS nested_object,
	prop.name AS nested_property_name,
	text.value AS text,
	d.data_id,
	d.value,
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
	class_parent.class_id AS parent_class_id,
	mem.parent_object_id as parent_object_id,
	class_parent.name AS parent_class,
	class_child.class_id AS child_class_id,
	class_child.name AS child_class,
	cat.name AS category,
	child_obj.object_id as object_id,
	child_obj.name as object_name,
	prop.name AS property_name,
	unit.value AS property_unit,
	data.value as property_value,
	IFNULL(band.band_id, 1) as band,
        COALESCE(date_from.date, nested_object.date_from) as date_from,
	COALESCE(date_to.date, nested_object.date_to) as date_to,
	COALESCE(memo.value, nested_object.memo) as memo,
	COALESCE(text.value, nested_object.text) AS text,
	COALESCE(scenario.name, nested_object.scenario) AS scenario
FROM
	t_membership AS mem
LEFT JOIN t_class AS class_parent ON
	mem.parent_class_id = class_parent.class_id
LEFT JOIN t_class AS class_child ON
	mem.child_class_id = class_child.class_id
LEFT JOIN t_collection AS collection ON
	collection.collection_id = mem.collection_id
LEFT JOIN t_object AS child_obj ON
	child_obj.object_id = mem.child_object_id
LEFT JOIN t_object AS parent_obj ON
	parent_obj.object_id = mem.parent_object_id
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
LEFT JOIN t_text AS text ON
	text.data_id = data.data_id
LEFT JOIN text_cte AS nested_object ON
	data.data_id = nested_object.tag_data_id
LEFT JOIN scenario_cte AS scenario ON
	scenario.data_id = data.data_id
LEFT JOIN t_category AS cat ON
	child_obj.category_id = cat.category_id;
