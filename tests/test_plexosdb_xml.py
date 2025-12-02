import os
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from plexosdb.enums import Schema
from plexosdb.xml_handler import XMLHandler, xml_query

XML_FPATH = Path("tests").joinpath("data/plexosdb.xml")
NAMESPACE = "http://tempuri.org/MasterDataSet.xsd"


@pytest.fixture
def xml_tree(_master_xml_param):
    return ET.parse(_master_xml_param)


def test_get_root(xml_tree):
    root = xml_tree.getroot()
    assert root is not None


@pytest.fixture(scope="module")
def xml_handler(_master_xml_param):
    return XMLHandler(fpath=_master_xml_param, namespace=NAMESPACE)


def test_xmlhandler_instance(xml_handler, xml_tree, _master_xml_param):
    assert isinstance(xml_handler, XMLHandler)
    assert type(xml_handler.tree) is type(xml_tree)
    assert xml_handler.fpath == _master_xml_param
    assert xml_handler.namespace == NAMESPACE


@pytest.mark.parametrize("in_memory", [True, False])
def test_in_memory_initialization(in_memory, _master_xml_param):
    handler = XMLHandler.parse(fpath=_master_xml_param, in_memory=in_memory)
    assert isinstance(handler, XMLHandler)
    assert handler.in_memory == in_memory
    assert handler.fpath == _master_xml_param
    assert handler.namespace == NAMESPACE
    elements = list(handler.iter(Schema.Objects))
    assert len(elements) > 0


def test_cache_construction():
    handler = XMLHandler.parse(fpath=XML_FPATH, in_memory=True)
    assert handler._cache is not None
    assert len(handler._cache) > 0


def test_iter(xml_handler):
    elements = list(xml_handler.iter(Schema.Objects))
    assert len(elements) > 4


def test_get_records(xml_handler):
    elements = list(xml_handler.get_records(Schema.Objects))
    assert len(elements) > 0
    assert elements[0]["class_id"] == 1


def test_save_xml(tmp_path):
    handler = XMLHandler.parse(fpath=XML_FPATH, in_memory=True)
    handler.to_xml(tmp_path / ".xml")

    assert os.path.exists(tmp_path / ".xml")
    assert getattr(handler, "_cache", False) is False
    assert getattr(handler, "_counts", False) is False


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
