-- Query to get all the objects and nested objects
-- We do not include scenario tags on nested objects.
WITH scenario_cte as (
SELECT
	obj.name,
	obj.object_id,
	tag.data_id,
	mem.collection_id,
	  cat.name AS category_name
FROM
	t_membership AS mem
LEFT JOIN t_tag AS tag ON
	tag.object_id = mem.child_object_id
LEFT JOIN t_object AS obj ON
	mem.child_object_id = obj.object_id
LEFT JOIN t_category AS cat ON
	obj.category_id = cat.category_id
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
	obj.name,
	obj.object_id,
	m.collection_id,
	m.membership_id,
	prop.name,
	data.value AS value,
	date_from.date as date_from,
	date_to.date as date_to,
	text.value as text_name,
	text.class_id as text_class_id,
	tag.data_id as tag_data_id,
	tag_object.name as tag_object,
	tag.object_id as tag_object_id,
	tag_class.name as tag_class_name,
	scenario.name AS scenario,
	scenario.object_id AS scenario_obj_id,
	scenario.data_id AS scenario_data_id,
	scenario.collection_id AS scenario_collection_id
FROM
	t_membership m
LEFT JOIN t_object AS obj ON
	m.child_object_id = obj.object_id
LEFT JOIN t_data data ON
	m.membership_id = data.membership_id
LEFT JOIN t_property as prop ON
	data.property_id = prop.property_id
LEFT JOIN t_date_from AS date_from ON
	data.data_id = date_from.data_id
LEFT JOIN t_date_to AS date_to ON
	data.data_id = date_to.data_id
LEFT JOIN t_tag as tag ON
	data.data_id = tag.data_id
LEFT JOIN scenario_cte as scenario ON
	scenario.data_id = data.data_id
	and tag.data_id = scenario.data_id
LEFT JOIN t_text as text on
	text.data_id = data.data_id
LEFT JOIN t_object as tag_object ON
	tag_object.object_id = tag.object_id
LEFT JOIN t_class as tag_class ON
	tag_class.class_id = tag_object.class_id
WHERE
	(tag_class.name ISNULL
		or tag_class.name <> 'Scenario')
