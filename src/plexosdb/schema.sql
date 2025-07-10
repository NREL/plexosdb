CREATE TABLE `t_assembly`
(
    `assembly_id` INT AUTO_INCREMENT NOT NULL,
    `filename` VARCHAR(255) NULL,
    `namespace` VARCHAR(255) NULL,
    `is_enabled` BIT NOT NULL,
    CONSTRAINT PK_t_assembly
               PRIMARY KEY (`assembly_id`)
);

CREATE TABLE `t_class_group`
(
    `class_group_id` INT NOT NULL,
    `name` VARCHAR(255) NULL,
    `lang_id` INT NULL,
    CONSTRAINT PK_t_class_group
               PRIMARY KEY (`class_group_id`)
);

CREATE TABLE `t_config`
(
    `element` VARCHAR(255) NOT NULL,
    `value` VARCHAR(255) NULL,
    CONSTRAINT PK_t_config
               PRIMARY KEY (`element`)
);

CREATE TABLE `t_property_group`
(
    `property_group_id` INT NOT NULL,
    `name` VARCHAR(255) NULL,
    `lang_id` INT NULL,
    CONSTRAINT PK_t_property_group
               PRIMARY KEY (`property_group_id`)
);

CREATE TABLE `t_unit`
(
    `unit_id` INT NOT NULL,
    `value` VARCHAR(50) NULL,
    `default` VARCHAR(50) NULL,
    `imperial_energy` VARCHAR(50) NULL,
    `metric_level` VARCHAR(255) NULL,
    `imperial_level` VARCHAR(255) NULL,
    `metric_volume` VARCHAR(255) NULL,
    `imperial_volume` VARCHAR(255) NULL,
    `description` VARCHAR(255) NULL,
    `lang_id` INT NULL,
    CONSTRAINT PK_t_unit
               PRIMARY KEY (`unit_id`)
);

CREATE TABLE `t_action`
(
    `action_id` INT NOT NULL,
    `action_symbol` VARCHAR(50) NULL,
    CONSTRAINT PK_t_action
               PRIMARY KEY (`action_id`)
);

CREATE TABLE `t_message`
(
    `number` INT NOT NULL,
    `severity` INT NULL,
    `default_action` INT NULL,
    `action` INT NULL,
    `description` VARCHAR(512) NULL,
    CONSTRAINT PK_t_message
               PRIMARY KEY (`number`),
    CONSTRAINT FK_t_message_default_action
               FOREIGN KEY (`default_action`) REFERENCES `t_action`(`action_id`),
    CONSTRAINT FK_t_message_action
               FOREIGN KEY (`action`) REFERENCES `t_action`(`action_id`)
);

CREATE TABLE `t_property_tag`
(
    `tag_id` BIGINT NOT NULL,
    `name` VARCHAR(255) NULL,
    CONSTRAINT PK_t_property_tag
               PRIMARY KEY (`tag_id`)
);

CREATE TABLE `t_custom_rule`
(
    `number` INT AUTO_INCREMENT NOT NULL,
    `condition` VARCHAR(512) NULL,
    `action_id` INT NULL,
    `message` VARCHAR(512) NULL,
    CONSTRAINT PK_t_custom_rule
               PRIMARY KEY (`number`),
    CONSTRAINT FK_t_custom_rule_action
               FOREIGN KEY (`action_id`) REFERENCES `t_action`(`action_id`)
);

CREATE TABLE `t_class`
(
    `class_id` INT NOT NULL,
    `name` VARCHAR(255) NULL COLLATE NOSPACE,
    `class_group_id` INT NULL,
    `is_enabled` BIT NULL,
    `lang_id` INT NULL,
    `description` VARCHAR(255) NULL,
    `state` INT NULL,
    `inherits_from` INT NULL,
    CONSTRAINT PK_t_class
               PRIMARY KEY (`class_id`),
    CONSTRAINT FK_t_class_group
               FOREIGN KEY (`class_group_id`) REFERENCES `t_class_group`(`class_group_id`),
    CONSTRAINT FK_t_class_inherits
               FOREIGN KEY (`inherits_from`) REFERENCES `t_class`(`class_id`)
);

CREATE TABLE `t_collection`
(
    `collection_id` INT NOT NULL,
    `parent_class_id` INT NULL,
    `child_class_id` INT NULL,
    `name` VARCHAR(255) NULL COLLATE NOSPACE,
    `min_count` INT NULL,
    `max_count` INT NULL,
    `complement_name` VARCHAR(255) NULL,
    `complement_min_count` INT NULL,
    `complement_max_count` INT NULL,
    `is_enabled` BIT NULL,
    `is_one_to_many` BIT NULL,
    `lang_id` INT NULL,
    `description` VARCHAR(255) NULL,
    `complement_description` VARCHAR(255) NULL,
    `rank` INT NULL,
    CONSTRAINT PK_t_collection
               PRIMARY KEY (`collection_id`),
    CONSTRAINT FK_t_collection_parent_class
               FOREIGN KEY (`parent_class_id`) REFERENCES `t_class`(`class_id`),
    CONSTRAINT FK_t_collection_child_class
               FOREIGN KEY (`child_class_id`) REFERENCES `t_class`(`class_id`)
);

CREATE TABLE `t_collection_report`
(
    `collection_id` INT NOT NULL,
    `left_collection_id` INT NOT NULL,
    `right_collection_id` INT NOT NULL,
    `rule_left_collection_id` INT NULL,
    `rule_right_collection_id` INT NULL,
    `rule_id` INT NULL,
    CONSTRAINT PK_t_collection_report
               PRIMARY KEY (`collection_id`, `left_collection_id`, `right_collection_id`),
    CONSTRAINT FK_t_collection_report_collection
               FOREIGN KEY (`collection_id`) REFERENCES `t_collection`(`collection_id`),
    CONSTRAINT FK_t_collection_report_left
               FOREIGN KEY (`left_collection_id`) REFERENCES `t_collection`(`collection_id`),
    CONSTRAINT FK_t_collection_report_right
               FOREIGN KEY (`right_collection_id`) REFERENCES `t_collection`(`collection_id`),
    CONSTRAINT FK_t_collection_report_rule_left
               FOREIGN KEY (`rule_left_collection_id`) REFERENCES `t_collection`(`collection_id`),
    CONSTRAINT FK_t_collection_report_rule_right
               FOREIGN KEY (`rule_right_collection_id`) REFERENCES `t_collection`(`collection_id`)
);

CREATE TABLE `t_property`
(
    `property_id` INT NOT NULL,
    `collection_id` INT NULL,
    `property_group_id` INT NULL,
    `enum_id` INT NULL,
    `name` VARCHAR(255) NULL,
    `unit_id` INT NULL,
    `default_value` FLOAT NULL,
    `validation_rule` VARCHAR(50) NULL,
    `input_mask` VARCHAR(512) NULL,
    `upscaling_method` INT NULL,
    `downscaling_method` INT NULL,
    `property_type` INT NULL,
    `period_type_id` INT NULL,
    `is_key` BIT NULL,
    `is_enabled` BIT NULL,
    `is_dynamic` BIT NULL,
    `is_multi_band` BIT NULL,
    `max_band_id` INT NULL,
    `lang_id` INT NULL,
    `description` VARCHAR(255) NULL,
    `tag` VARCHAR(512) NULL,
    `is_visible` BIT NULL,
    CONSTRAINT PK_t_property
               PRIMARY KEY (`property_id`),
    CONSTRAINT FK_t_property_collection
               FOREIGN KEY (`collection_id`) REFERENCES `t_collection`(`collection_id`),
    CONSTRAINT FK_t_property_group
               FOREIGN KEY (`property_group_id`) REFERENCES `t_property_group`(`property_group_id`),
    CONSTRAINT FK_t_property_unit
               FOREIGN KEY (`unit_id`) REFERENCES `t_unit`(`unit_id`)
);

CREATE TABLE `t_property_report`
(
    `property_id` INT NOT NULL,
    `collection_id` INT NULL,
    `property_group_id` INT NULL,
    `enum_id` INT NULL,
    `name` VARCHAR(255) NULL,
    `summary_name` VARCHAR(255) NULL,
    `unit_id` INT NULL,
    `summary_unit_id` INT NULL,
    `is_period` BIT NULL,
    `is_summary` BIT NULL,
    `is_multi_band` BIT NULL,
    `is_quantity` BIT NULL,
    `is_LT` BIT NULL,
    `is_PA` BIT NULL,
    `is_MT` BIT NULL,
    `is_ST` BIT NULL,
    `lang_id` INT NULL,
    `summary_lang_id` INT NULL,
    `description` VARCHAR(255) NULL,
    `is_visible` BIT NULL,
    CONSTRAINT PK_t_property_report
               PRIMARY KEY (`property_id`),
    CONSTRAINT FK_t_property_report_collection
               FOREIGN KEY (`collection_id`) REFERENCES `t_collection`(`collection_id`),
    CONSTRAINT FK_t_property_report_group
               FOREIGN KEY (`property_group_id`) REFERENCES `t_property_group`(`property_group_id`),
    CONSTRAINT FK_t_property_report_unit
               FOREIGN KEY (`unit_id`) REFERENCES `t_unit`(`unit_id`),
    CONSTRAINT FK_t_property_report_summary_unit
               FOREIGN KEY (`summary_unit_id`) REFERENCES `t_unit`(`unit_id`)
);

CREATE TABLE `t_custom_column`
(
    `column_id` INT AUTO_INCREMENT NOT NULL,
    `class_id` INT NULL,
    `name` VARCHAR(255) NULL,
    `position` INT NULL,
    `GUID` CHAR(36) NULL,
    CONSTRAINT PK_t_custom_column
               PRIMARY KEY (`column_id`),
    CONSTRAINT FK_t_custom_column_class
               FOREIGN KEY (`class_id`) REFERENCES `t_class`(`class_id`)
);

CREATE TABLE `t_attribute`
(
    `attribute_id` INT NOT NULL,
    `class_id` INT NULL,
    `enum_id` INT NULL,
    `name` VARCHAR(255) NULL,
    `unit_id` INT NULL,
    `default_value` FLOAT NULL,
    `validation_rule` VARCHAR(50) NULL,
    `input_mask` VARCHAR(512) NULL,
    `is_enabled` BIT NULL,
    `is_integer` BIT NULL,
    `lang_id` INT NULL,
    `description` VARCHAR(255) NULL,
    `tag` VARCHAR(512) NULL,
    `is_visible` BIT NULL,
    CONSTRAINT PK_t_attribute
               PRIMARY KEY (`attribute_id`),
    CONSTRAINT FK_t_attribute_class
               FOREIGN KEY (`class_id`) REFERENCES `t_class`(`class_id`),
    CONSTRAINT FK_t_attribute_unit
               FOREIGN KEY (`unit_id`) REFERENCES `t_unit`(`unit_id`)
);

CREATE TABLE `t_category`
(
    `category_id` INTEGER,
    `class_id` INT NOT NULL,
    `rank` INT NOT NULL,
    `name` VARCHAR(512) NULL,
    `state` INT NULL,
    UNIQUE (`class_id`, `name`),
    CONSTRAINT PK_t_category
               PRIMARY KEY (`category_id`),
    CONSTRAINT FK_t_category_class
               FOREIGN KEY (`class_id`) REFERENCES `t_class`(`class_id`)
);

CREATE TABLE `t_object`
(
    `object_id` INTEGER UNIQUE,
    `class_id` INT NULL,
    `name` VARCHAR(512) NULL COLLATE NOCASE,
    `category_id` INT NULL,
    `description` VARCHAR(255) NULL,
    `GUID` CHAR(36) NOT NULL,
    `state` INT NULL,
    `X` INT NULL,
    `Y` INT NULL,
    `Z` INT NULL,
    UNIQUE (`class_id`, `name`),
    CONSTRAINT PK_t_object
               PRIMARY KEY (`object_id`),
    CONSTRAINT FK_t_object_class
               FOREIGN KEY (`class_id`) REFERENCES `t_class`(`class_id`),
    CONSTRAINT FK_t_object_category
               FOREIGN KEY (`category_id`) REFERENCES `t_category`(`category_id`)
);

CREATE TABLE `t_memo_object`
(
    `object_id` INT NOT NULL,
    `column_id` INT NOT NULL,
    `value` VARCHAR(512) NULL,
    `state` INT NULL,
    CONSTRAINT PK_t_memo_object
               PRIMARY KEY (`object_id`, `column_id`),
    CONSTRAINT FK_t_memo_object_object
               FOREIGN KEY (`object_id`) REFERENCES `t_object`(`object_id`) ON DELETE CASCADE,
    CONSTRAINT FK_t_memo_object_column
               FOREIGN KEY (`column_id`) REFERENCES `t_custom_column`(`column_id`)
);

CREATE TABLE `t_report`
(
    `object_id` INT NOT NULL,
    `property_id` INT NOT NULL,
    `phase_id` INT NOT NULL,
    `report_period` BIT NULL,
    `report_summary` BIT NULL,
    `report_statistics` BIT NULL,
    `report_samples` BIT NULL,
    `write_flat_files` BIT NULL,
    `state` INT NULL,
    CONSTRAINT PK_t_report
               PRIMARY KEY (`object_id`, `property_id`, `phase_id`),
    CONSTRAINT FK_t_report_object
               FOREIGN KEY (`object_id`) REFERENCES `t_object`(`object_id`) ON DELETE CASCADE,
    CONSTRAINT FK_t_report_property
               FOREIGN KEY (`property_id`) REFERENCES `t_property_report`(`property_id`)
);

CREATE TABLE `t_object_meta`
(
    `object_id` INT NOT NULL,
    `class` VARCHAR(512) NOT NULL,
    `property` VARCHAR(512) NOT NULL,
    `value` VARCHAR(512) NULL,
    `state` INT NULL,
    CONSTRAINT PK_t_object_meta
               PRIMARY KEY (`object_id`, `class`, `property`),
    CONSTRAINT FK_t_object_meta_object
               FOREIGN KEY (`object_id`) REFERENCES `t_object`(`object_id`) ON DELETE CASCADE
);

CREATE TABLE `t_attribute_data`
(
    `object_id` INT NOT NULL,
    `attribute_id` INT NOT NULL,
    `value` FLOAT NULL,
    `state` INT NULL,
    CONSTRAINT PK_t_attribute_data
               PRIMARY KEY (`object_id`, `attribute_id`),
    CONSTRAINT FK_t_attribute_data_object
               FOREIGN KEY (`object_id`) REFERENCES `t_object`(`object_id`) ON DELETE CASCADE,
    CONSTRAINT FK_t_attribute_data_attribute
               FOREIGN KEY (`attribute_id`) REFERENCES `t_attribute`(`attribute_id`)
);

CREATE TABLE `t_membership`
(
    `membership_id` INTEGER,
    `parent_class_id` INT NULL,
    `parent_object_id` INT NULL,
    `collection_id` INT NULL,
    `child_class_id` INT NULL,
    `child_object_id` INT NULL,
    `state` INT NULL,
    UNIQUE(`parent_object_id`, `collection_id`, `child_object_id`),
    CONSTRAINT PK_t_membership
               PRIMARY KEY (`membership_id`),
    CONSTRAINT FK_t_membership_parent_class
               FOREIGN KEY (`parent_class_id`) REFERENCES `t_class`(`class_id`),
    CONSTRAINT FK_t_membership_parent_object
               FOREIGN KEY (`parent_object_id`) REFERENCES `t_object`(`object_id`) ON DELETE CASCADE,
    CONSTRAINT FK_t_membership_collection
               FOREIGN KEY (`collection_id`) REFERENCES `t_collection`(`collection_id`),
    CONSTRAINT FK_t_membership_child_class
               FOREIGN KEY (`child_class_id`) REFERENCES `t_class`(`class_id`),
    CONSTRAINT FK_t_membership_child_object
               FOREIGN KEY (`child_object_id`) REFERENCES `t_object`(`object_id`) ON DELETE CASCADE
);

CREATE TABLE `t_memo_membership`
(
    `membership_id` INT NOT NULL,
    `value` VARCHAR(512) NULL,
    `state` INT NULL,
    CONSTRAINT PK_t_memo_membership
               PRIMARY KEY (`membership_id`),
    CONSTRAINT FK_t_memo_membership_membership
               FOREIGN KEY (`membership_id`) REFERENCES `t_membership`(`membership_id`) ON DELETE CASCADE
);

CREATE TABLE `t_membership_meta`
(
    `membership_id` INT NOT NULL,
    `class` VARCHAR(512) NOT NULL,
    `property` VARCHAR(512) NOT NULL,
    `value` VARCHAR(512) NULL,
    `state` INT NULL,
    CONSTRAINT PK_t_membership_meta
               PRIMARY KEY (`membership_id`, `class`, `property`),
    CONSTRAINT FK_t_membership_meta_membership
               FOREIGN KEY (`membership_id`) REFERENCES `t_membership`(`membership_id`) ON DELETE CASCADE
);

CREATE TABLE `t_data`
(
    `data_id` INTEGER,
    `membership_id` INT NULL,
    `property_id` INT NULL,
    `value` FLOAT NULL,
    `state` INT NULL,
    `uid` BIGINT NULL,
    CONSTRAINT PK_t_data
               PRIMARY KEY (`data_id`),
    CONSTRAINT FK_t_data_membership
               FOREIGN KEY (`membership_id`) REFERENCES `t_membership`(`membership_id`) ON DELETE CASCADE,
    CONSTRAINT FK_t_data_property
               FOREIGN KEY (`property_id`) REFERENCES `t_property`(`property_id`)
);

CREATE TABLE `t_date_from`
(
    `data_id` INT NOT NULL,
    `date` DATETIME NULL,
    `state` INT NULL,
    CONSTRAINT PK_t_date_from
               PRIMARY KEY (`data_id`),
    CONSTRAINT FK_t_date_from_data
               FOREIGN KEY (`data_id`) REFERENCES `t_data`(`data_id`) ON DELETE CASCADE
);

CREATE TABLE `t_date_to`
(
    `data_id` INT NOT NULL,
    `date` DATETIME NULL,
    `state` INT NULL,
    CONSTRAINT PK_t_date_to
               PRIMARY KEY (`data_id`),
    CONSTRAINT FK_t_date_to_data
               FOREIGN KEY (`data_id`) REFERENCES `t_data`(`data_id`) ON DELETE CASCADE
);

CREATE TABLE `t_tag`
(
    `data_id` INT NOT NULL,
    `object_id` INT NOT NULL,
    `state` INT NULL,
    `action_id` INT NULL,
    CONSTRAINT PK_t_tag
               PRIMARY KEY (`data_id`, `object_id`),
    CONSTRAINT FK_t_tag_data
               FOREIGN KEY (`data_id`) REFERENCES `t_data`(`data_id`) ON DELETE CASCADE,
    CONSTRAINT FK_t_tag_object
               FOREIGN KEY (`object_id`) REFERENCES `t_object`(`object_id`) ON DELETE CASCADE,
    CONSTRAINT FK_t_tag_action
               FOREIGN KEY (`action_id`) REFERENCES `t_action`(`action_id`)
);

CREATE TABLE `t_text`
(
    `data_id` INT NOT NULL,
    `class_id` INT NOT NULL,
    `value` VARCHAR(512) NULL,
    `state` INT NULL,
    `action_id` INT NULL,
    CONSTRAINT PK_t_text
               PRIMARY KEY (`data_id`, `class_id`),
    CONSTRAINT FK_t_text_data
               FOREIGN KEY (`data_id`) REFERENCES `t_data`(`data_id`) ON DELETE CASCADE,
    CONSTRAINT FK_t_text_class
               FOREIGN KEY (`class_id`) REFERENCES `t_class`(`class_id`),
    CONSTRAINT FK_t_text_action
               FOREIGN KEY (`action_id`) REFERENCES `t_action`(`action_id`)
);

CREATE TABLE `t_memo_data`
(
    `data_id` INT NOT NULL,
    `value` VARCHAR(512) NULL,
    `state` INT NULL,
    CONSTRAINT PK_t_memo_data
               PRIMARY KEY (`data_id`),
    CONSTRAINT FK_t_memo_data_data
               FOREIGN KEY (`data_id`) REFERENCES `t_data`(`data_id`) ON DELETE CASCADE
);

CREATE TABLE `t_data_meta`
(
    `data_id` INT NOT NULL,
    `class` VARCHAR(512) NOT NULL,
    `property` VARCHAR(512) NOT NULL,
    `value` VARCHAR(512) NULL,
    `state` INT NULL,
    CONSTRAINT PK_t_data_meta
               PRIMARY KEY (`data_id`, `class`, `property`),
    CONSTRAINT FK_t_data_meta_data
               FOREIGN KEY (`data_id`) REFERENCES `t_data`(`data_id`) ON DELETE CASCADE
);

CREATE TABLE `t_band`
(
    `data_id` INT NOT NULL,
    `band_id` INT NULL,
    `state` INT NULL,
    CONSTRAINT PK_t_band
               PRIMARY KEY (`data_id`),
    CONSTRAINT FK_t_band_data
               FOREIGN KEY (`data_id`) REFERENCES `t_data`(`data_id`) ON DELETE CASCADE
);
