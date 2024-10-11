-- Query to get all the objects and nested objects
-- We do not include scenario tags on nested objects.
WITH scenario_cte AS (
SELECT
	object.name,
	object.object_id,
	tag.data_id,
	mem.collection_id,
	  cat.name AS category_name
FROM
	t_membership AS mem
LEFT JOIN t_tag AS tag ON
	tag.object_id = mem.child_object_id
LEFT JOIN t_object AS object ON
	mem.child_object_id = object.object_id
LEFT JOIN t_category AS cat ON
	object.category_id = cat.category_id
WHERE
	mem.collection_id in
        (
	SELECT
		collection_id
	FROM
		t_collection
	LEFT JOIN t_class ON
		t_class.class_id = t_collection.parent_class_id
	where
		t_class.name <> 'System'
        )
 )
SELECT
	parent_class.name AS parent_class_name,
	child_class.name AS child_class_name,
	parent_object.object_id AS parent_obj_id,
	object.object_id AS object_id,
	object.name AS name,
	category.name AS category,
	property.name AS property_name,
	unit.value AS property_unit,
	data.value AS property_value,
	IFNULL(band.band_id, 1) AS band,
	date_from.date AS date_from,
	date_to.date AS date_to,
	text.value AS text,
	text_class.name AS text_class_name,
	tag.data_id AS tag_data_id,
	tag_object.name AS tag_object,
	tag.object_id AS tag_object_id,
	tag_class.name AS tag_class_name,
	action.action_symbol as action,
	scenario.name AS scenario
	-- scenario.object_id AS scenario_obj_id,
	-- scenario.data_id AS scenario_data_id,
	-- scenario.collection_id AS scenario_collection_id
FROM
	t_membership membership
LEFT JOIN t_object AS object ON
	membership.child_object_id = object.object_id
LEFT JOIN t_category AS category ON
	object.category_id = category.category_id
LEFT JOIN t_object AS parent_object ON
	membership.parent_object_id = parent_object.object_id
LEFT JOIN t_class AS child_class ON
	membership.child_class_id = child_class.class_id
LEFT JOIN t_class AS parent_class ON
	membership.parent_class_id = parent_class.class_id
LEFT JOIN t_data AS data ON
	membership.membership_id = data.membership_id
LEFT JOIN t_band AS band on
	data.data_id = band.band_id
LEFT JOIN t_property AS property ON
	data.property_id = property.property_id
LEFT JOIN t_unit AS unit ON
	property.unit_id = unit.unit_id
LEFT JOIN t_date_from AS date_from ON
	data.data_id = date_from.data_id
LEFT JOIN t_date_to AS date_to ON
	data.data_id = date_to.data_id
LEFT JOIN t_tag AS tag ON
	data.data_id = tag.data_id
LEFT JOIN t_action AS action ON
	tag.action_id = action.action_id
LEFT JOIN scenario_cte AS scenario ON
	scenario.data_id = data.data_id
	and tag.data_id = scenario.data_id
LEFT JOIN t_text AS text on
	text.data_id = data.data_id
LEFT JOIN t_class AS text_class on
	text.class_id = text_class.class_id
LEFT JOIN t_object AS tag_object ON
	tag_object.object_id = tag.object_id
LEFT JOIN t_class AS tag_class ON
	tag_class.class_id = tag_object.class_id
WHERE
	(tag_class.name ISNULL
		or tag_class.name <> 'Scenario')
