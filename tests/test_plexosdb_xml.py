import pytest
import os
from pathlib import Path
import xml.etree.ElementTree as ET  # noqa: N817
from plexosdb.exceptions import MultlipleElementsError
from plexosdb.enums import Schema
from plexosdb.xml_handler import XMLHandler
from plexosdb.xml_handler import xml_query

XML_FPATH = Path("tests").joinpath("data/plexosdb.xml")
NAMESPACE = "http://tempuri.org/MasterDataSet.xsd"


@pytest.fixture
def xml_tree():
    return ET.parse(XML_FPATH)


def test_get_root(xml_tree):
    root = xml_tree.getroot()
    assert root is not None


@pytest.fixture(scope="module")
def xml_handler():
    return XMLHandler(fpath=XML_FPATH, namespace=NAMESPACE)


def test_xmlhandler_instance(xml_handler, xml_tree):
    assert isinstance(xml_handler, XMLHandler)
    assert type(xml_handler.tree) is type(xml_tree)
    assert xml_handler.fpath == XML_FPATH
    assert xml_handler.namespace == NAMESPACE


@pytest.mark.parametrize("in_memory", [True, False])
def test_in_memory_initialization(in_memory):
    handler = XMLHandler.parse(fpath=XML_FPATH, in_memory=in_memory)
    assert isinstance(handler, XMLHandler)
    assert handler.in_memory == in_memory
    assert handler.fpath == XML_FPATH
    assert handler.namespace == NAMESPACE
    elements = list(handler.iter(Schema.Objects))
    assert len(elements) == 3


def test_cache_construction():
    handler = XMLHandler.parse(fpath=XML_FPATH, in_memory=True)
    assert handler._cache is not None
    assert len(handler._cache) == 9  # Total number of elements parsed


@pytest.mark.parametrize(
    "class_name,category,name,expected_id",
    [(Schema.Class, None, "System", "1"), (Schema.Objects, 2, "SolarPV01", "2")],
)
def test_xml_get_id(xml_handler, class_name, category, name, expected_id):
    element_id = xml_handler.get_id(class_name, name=name, category_id=category)
    assert element_id is not None
    assert element_id == expected_id


def test_iter(xml_handler):
    elements = list(xml_handler.iter(Schema.Objects))
    assert len(elements) == 3


def test_get_records(xml_handler):
    elements = list(xml_handler.get_records(Schema.Objects))
    assert len(elements) == 3
    assert elements[0]["class_id"] == 1
    assert elements[0]["name"] == "System"
    assert elements[1]["class_id"] == 2
    assert elements[1]["name"] == "SolarPV01"


def test_save_xml(tmp_path):
    handler = XMLHandler.parse(fpath=XML_FPATH, in_memory=True)
    handler.to_xml(tmp_path / ".xml")

    assert os.path.exists(tmp_path / ".xml")
    assert getattr(handler, "_cache", False) is False
    assert getattr(handler, "_counts", False) is False


# TODO(pesap): Add test to round-trip serialization of plexos model.
# https://github.nrel.gov/PCM/R2X/issues/361


def test_get_element_id_returns(xml_handler):
    # Assert that we raise and error if element combination is not found
    with pytest.raises(KeyError):
        xml_handler.get_id(Schema.Class, name="test")

    # Assert that raises error if multiple matches found
    with pytest.raises(MultlipleElementsError, match="Multiple elements returned"):
        xml_handler.get_id(Schema.Class)


def test_get_max_id(xml_handler):
    max_id = xml_handler.get_max_id(Schema.Objects)
    assert max_id == 3


@pytest.mark.parametrize(
    "element_name, tags, tag_elements, expected_query",
    [
        ("t_object", (), {"class_id": "2"}, './/t_object[class_id="2"]'),
        ("t_object", (), {"class_id": "2", "enum_id": "1"}, './/t_object[class_id="2"][enum_id="1"]'),
        (
            "t_object",
            ("name",),
            {"class_id": "2", "enum_id": "1"},
            './/t_object[name][class_id="2"][enum_id="1"]',
        ),
    ],
)
def test_xml_query(element_name, tags, tag_elements, expected_query):
    assert xml_query(element_name, *tags, **tag_elements) == expected_query
