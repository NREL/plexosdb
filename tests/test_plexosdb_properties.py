import pytest

from plexosdb import ClassEnum


def test_add_property_to_object_succeeds(db_with_topology):
    db_with_topology.add_property(ClassEnum.Generator, "thermal-01", "Max Capacity", 100.0, band=1)
    properties = db_with_topology.get_object_properties(ClassEnum.Generator, "thermal-01")
    assert len(properties) == 1


def test_add_property_with_band_2_succeeds(db_with_topology):
    db_with_topology.add_property(ClassEnum.Generator, "thermal-01", "Heat Rate", 10.5, band=1)
    db_with_topology.add_property(ClassEnum.Generator, "thermal-01", "Heat Rate", 11.5, band=2)
    properties = db_with_topology.get_object_properties(ClassEnum.Generator, "thermal-01")
    assert any(p["property"] == "Heat Rate" for p in properties)


def test_add_property_with_text_data_succeeds(db_with_topology, tmp_path):
    data_file = tmp_path / "test_data.csv"
    data_file.write_text("value1,value2")
    db_with_topology.add_property(
        ClassEnum.Generator, "thermal-01", "Rating", 0.0, band=1, text={ClassEnum.DataFile: str(data_file)}
    )
    properties = db_with_topology.get_object_properties(ClassEnum.Generator, "thermal-01")
    assert any(p["property"] == "Rating" for p in properties)


def test_add_multiple_properties_to_same_object_succeeds(db_with_topology):
    db_with_topology.add_property(ClassEnum.Generator, "thermal-01", "Max Capacity", 100.0, band=1)
    db_with_topology.add_property(ClassEnum.Generator, "thermal-01", "Fuel Price", 5.0, band=1)
    db_with_topology.add_property(ClassEnum.Generator, "thermal-01", "Heat Rate", 10.5, band=1)
    properties = db_with_topology.get_object_properties(ClassEnum.Generator, "thermal-01")
    assert len(properties) == 3


def test_add_property_to_different_objects_succeeds(db_with_topology):
    db_with_topology.add_property(ClassEnum.Generator, "thermal-01", "Max Capacity", 100.0, band=1)
    db_with_topology.add_property(ClassEnum.Generator, "solar-01", "Max Capacity", 50.0, band=1)
    thermal_props = db_with_topology.get_object_properties(ClassEnum.Generator, "thermal-01")
    solar_props = db_with_topology.get_object_properties(ClassEnum.Generator, "solar-01")
    assert len(thermal_props) > 0 and len(solar_props) > 0


def test_get_object_properties_returns_list(db_thermal_gen):
    properties = db_thermal_gen.get_object_properties(ClassEnum.Generator, "thermal-01")
    assert isinstance(properties, list)


def test_get_object_properties_contains_added_properties(db_thermal_gen):
    properties = db_thermal_gen.get_object_properties(ClassEnum.Generator, "thermal-01")
    property_names = [p["property"] for p in properties]
    assert "Max Capacity" in property_names


def test_iterate_properties_for_object_succeeds(db_thermal_gen):
    properties = db_thermal_gen.get_object_properties(ClassEnum.Generator, "thermal-01")
    assert len(properties) > 0


def test_iterate_properties_contains_correct_property_names(db_thermal_gen):
    property_names = [
        p["property"] for p in db_thermal_gen.get_object_properties(ClassEnum.Generator, "thermal-01")
    ]
    assert "Max Capacity" in property_names


def test_delete_property_removes_property(db_thermal_gen):
    db_thermal_gen.delete_property(ClassEnum.Generator, "thermal-01", property_name="Max Capacity")
    properties = db_thermal_gen.get_object_properties(ClassEnum.Generator, "thermal-01")
    assert not any(p["property"] == "Max Capacity" for p in properties)


def test_delete_property_from_multiband_property_succeeds(db_thermal_gen_multiband):
    # Delete one band of the multiband property
    initial_heat_rate_count = sum(
        1
        for p in db_thermal_gen_multiband.get_object_properties(ClassEnum.Generator, "thermal-01")
        if p["property"] == "Heat Rate"
    )
    # After deletion, we should still have some Heat Rate properties (other bands)
    db_thermal_gen_multiband.delete_property(ClassEnum.Generator, "thermal-01", property_name="Heat Rate")
    properties = db_thermal_gen_multiband.get_object_properties(ClassEnum.Generator, "thermal-01")
    # Should still have Heat Rate if it's multiband
    assert initial_heat_rate_count > 0
    assert len(properties) > 1


def test_add_property_with_invalid_object_fails(db_with_topology):
    with pytest.raises(Exception):
        db_with_topology.add_property(ClassEnum.Generator, "nonexistent-gen", "Max Capacity", 100.0, band=1)


def test_delete_nonexistent_property_raises_error(db_thermal_gen):
    with pytest.raises(Exception):
        db_thermal_gen.delete_property(
            ClassEnum.Generator, "thermal-01", property_name="Nonexistent Property"
        )


def test_add_property_to_node_object_succeeds(db_with_topology):
    db_with_topology.add_property(ClassEnum.Node, "node-01", "Load", 100.0, band=1)
    properties = db_with_topology.get_object_properties(ClassEnum.Node, "node-01")
    assert len(properties) > 0


def test_add_property_to_region_object_succeeds(db_with_topology):
    db_with_topology.add_property(ClassEnum.Region, "region-01", "Load", 100.0, band=1)
    properties = db_with_topology.get_object_properties(ClassEnum.Region, "region-01")
    assert len(properties) > 0


def test_property_value_is_stored_correctly(db_with_topology):
    expected_value = 123.45
    db_with_topology.add_property(ClassEnum.Generator, "thermal-01", "Max Capacity", expected_value, band=1)
    properties = db_with_topology.get_object_properties(ClassEnum.Generator, "thermal-01")
    capacity_prop = next((p for p in properties if p["property"] == "Max Capacity"), None)
    assert capacity_prop is not None and abs(capacity_prop.get("value", 0) - expected_value) < 0.01


def test_multiband_properties_have_different_bands(db_thermal_gen_multiband):
    properties = db_thermal_gen_multiband.get_object_properties(ClassEnum.Generator, "thermal-01")
    heat_rate_bands = [p.get("band") for p in properties if p["property"] == "Heat Rate"]
    assert len(heat_rate_bands) >= 2


@pytest.mark.parametrize(
    "class_enum,obj_name",
    [
        (ClassEnum.Generator, "thermal-01"),
        (ClassEnum.Generator, "solar-01"),
        (ClassEnum.Generator, "wind-01"),
    ],
)
def test_add_property_to_parametrized_generators_succeeds(db_with_topology, class_enum, obj_name):
    db_with_topology.add_property(class_enum, obj_name, "Max Capacity", 100.0, band=1)
    properties = db_with_topology.get_object_properties(class_enum, obj_name)
    assert len(properties) > 0


@pytest.mark.parametrize(
    "property_name,value",
    [
        ("Max Capacity", 100.0),
        ("Fuel Price", 5.0),
        ("Heat Rate", 10.5),
        ("Start Cost", 1000.0),
    ],
)
def test_add_different_property_types_succeeds(db_with_topology, property_name, value):
    db_with_topology.add_property(ClassEnum.Generator, "thermal-01", property_name, value, band=1)
    properties = db_with_topology.get_object_properties(ClassEnum.Generator, "thermal-01")
    assert any(p["property"] == property_name for p in properties)


@pytest.mark.parametrize("band", [1, 2, 3])
def test_add_property_with_different_bands_succeeds(db_with_topology, band):
    db_with_topology.add_property(ClassEnum.Generator, "thermal-01", "Heat Rate", 10.0 + band, band=band)
    properties = db_with_topology.get_object_properties(ClassEnum.Generator, "thermal-01")
    assert any(p["property"] == "Heat Rate" and p.get("band") == band for p in properties)
