"""Plexos Input XML API."""

from collections import defaultdict
import uuid
import xml.etree.ElementTree as ET  # noqa: N817
from collections.abc import Iterable, Iterator
from enum import Enum
from functools import lru_cache
from os import PathLike

from loguru import logger

from .exceptions import ModelError, MultlipleElementsError
from .enums import ClassEnum, CollectionEnum, Schema, str2enum
from .utils import validate_string


class XMLHandler:
    """PLEXOS XML handler."""

    def __init__(
        self, fpath: str | PathLike, namespace: str, in_memory: bool = True, model: str | None = None
    ) -> None:
        self.fpath = fpath
        self.namespace = namespace
        self.model = model
        self.in_memory = in_memory
        self._cache = {}
        self._counts = {}

        # Parse the XML using bare ElementTree
        self.tree = ET.parse(fpath)
        self.root = self.tree.getroot()

        # Clean the root for simplier queries
        self._remove_namespace(namespace)

        # Create in-memory cache to speed up searching on the document
        if self.in_memory:
            _cache = defaultdict(list)
            for element in self.root:
                _cache[element.tag].append(element)
            self._cache = _cache
            self._counts = {key: len(_cache[key]) for key in _cache}

    @classmethod
    def parse(
        cls, fpath: str | PathLike, namespace: str = "http://tempuri.org/MasterDataSet.xsd", **kwargs
    ) -> "XMLHandler":
        """Return XML instance from file requested."""
        return XMLHandler(fpath=fpath, namespace=namespace, **kwargs)

    def _preprocess_xml(self, model_name: str | None = None) -> None:
        if model_name is None:
            logger.info("No model provided. Skipping preprocess step.")
            return

        logger.debug("Parsing model {}", model_name)
        model_data = self.get_records(Schema.Objects, class_id=ClassEnum.Model, name=model_name)

        if not model_data:
            msg = f"Model {model_name} not found on the database. Validate that the model exists."
            raise KeyError(msg)

        # Safety mechanism for multiple models
        if len(model_data) > 1:
            msg = (
                f"Multiple models returned for {model_name}. "
                "Check spelling or database for duplicate entries."
            )
            raise ModelError(msg)
        else:
            model_data = model_data[0]

        scenarios_memberships_for_model = self.get_records(
            Schema.Memberships,
            parent_object_id=model_data.get("object_id", None),
            child_class_id=ClassEnum.Scenario,
        )

        scenarios_in_model = set(map(lambda x: x.get("child_object_id"), scenarios_memberships_for_model))
        tag_data = self.get_records(Schema.Tags)
        model_data_ids = [e.get("data_id") for e in tag_data if e.get("object_id") in scenarios_in_model]
        self.model_scenarios = scenarios_in_model
        self.model_data_ids = model_data_ids
        self.valid_properties = self.get_valid_properties_dict()
        return

    def get_records(
        self,
        element_enum: Schema,
        *elements: Iterable[str | int],
        rename_dict: dict | None = None,
        **tag_elements,
    ) -> list[dict]:
        """Return a given element(s) as list of dictionaries."""
        if rename_dict is None:
            rename_dict = {}
        element_list = self.iter(element_enum, *elements, **tag_elements)

        # Return a dict version of the elements
        return list(
            map(
                lambda element: {
                    rename_dict.get(e.tag, e.tag): validate_string(e.text)
                    for e in element.iter()
                    if e.tag != element_enum.name
                },
                element_list,
            )
        )

    def get_id(self, element_enum: Schema, *, label: str | None = None, **tag_elements) -> str:
        """Return element ID matching name, tags and values.

        This function should return the element_id for a a single element. If
        the query returns more than one element, it will raise an error.

        Returns
        -------
        str
            Element type id

        Raises
        ------
        KeyError
            If combination of element_name and tags do not exists
        MultipleElementsError
            If more than one element found
        """
        element = list(self.iter(element_enum, **tag_elements))

        if not element:
            msg = f"{element_enum=} with {tag_elements=} not found"
            raise KeyError(msg)

        if len(element) > 1:
            msg = (
                f"Multiple elements returned for {element_enum=}.{tag_elements}. "
                "Use `iter` too see all the returned elements or provide additional filters."
            )
            raise MultlipleElementsError(msg)

        if label is None:
            return element[0].findtext(element_enum.label)  # type: ignore

        return element[0].findtext(label)  # type: ignore

    def add_attribute(  # noqa: D102
        self,
        /,
        *,
        object_name: str,
        object_class: ClassEnum,
        attribute_class: ClassEnum,
        attribute_name: str,
        attribute_value: str | float | int,
    ) -> int:
        logger.trace("Addding properties for object: {}", object_name)
        object_id = self.get_id(Schema.Objects, name=object_name, clas_id=object_class)
        attribute_id = self.get_id(Schema.Attributes, class_id=attribute_class, name=attribute_name)
        new_xml = self._add_xml_child(
            Schema.AttributeData.name,
            attribute_id=attribute_id,
            value=attribute_value,
            object_id=object_id,
        )
        attribute_id = new_xml.findtext("attribute_id")
        assert attribute_id

        return int(attribute_id)

    def add_single_property(
        self,
        object_name,
        property_name,
        property_value,
        *,
        parent_object_name: str | None = None,
        property_enum: Schema = Schema.Property,
        collection_enum: CollectionEnum = CollectionEnum.SystemGenerators,
        data_enum: Schema = Schema.Data,
        scenario: str | None = "default",
        time_series_dict: dict | None = None,
        **kwargs,
    ) -> None:
        """Add property for a given object in the database.

        Parameters
        ----------
        object_name
            Name to be added to the object
        property_name
            Valid plexos property to be added for the given collection
        property_value
            Value to assign to the property
        object_class
            ClassEnum from the object to be added. E.g., for generators class_id=ClassEnum.Generators
        parent_object_name
            Name of the parent object. User for creating the membership.
        collection
            Collection for membership
        scenario
            Scenario tag to add to the property
        text
            Additional text to add to the property. E.g., memo data or Data File.

        Returns
        -------
        int
            data_id of the added property.
        """
        logger.trace("Addding properties for object: {}", object_name)
        object_id = self.get_id(Schema.Objects, name=object_name, **kwargs)
        property_id = self.get_id(property_enum, collection_id=collection_enum.value, name=property_name)
        if parent_object_name is not None:
            parent_object_id = self.get_id(Schema.Objects, name=parent_object_name, **kwargs)
        else:
            parent_object_id = None

        try:
            membership_id = self.get_id(Schema.Memberships, child_object_id=object_id)
        except MultlipleElementsError:
            membership_id = self.get_id(
                Schema.Memberships,
                child_object_id=object_id,
                collection_id=collection_enum.value,
                parent_object_id=parent_object_id,
            )

        data_id = self.get_max_id(data_enum) + 1  # Increment so that property does not start in 0

        # Change property to dynamic so that it is showed at the bottom of the Plexos GUI
        # We could probably do this once.
        property = self._get_xml_element(property_enum, property_id=property_id)
        setattr(property.find("is_dynamic"), "text", "true")
        setattr(property.find("is_enabled"), "text", "true")

        self._add_xml_child(
            data_enum.name,
            data_id=data_id,
            membership_id=membership_id,
            property_id=property_id,
            value=property_value,
        )

        # Create tags for scenario
        if scenario:
            try:
                scenario_object_id = self.get_id(Schema.Objects, name=scenario, class_id=ClassEnum.Scenario)
            except KeyError:
                scenario_object_id = self.add_object(
                    scenario,
                    ClassEnum.Scenario,
                    CollectionEnum.SystemScenarios,
                )
            self._add_xml_child(Schema.Tags.name, data_id=data_id, object_id=scenario_object_id)

        if time_series_dict:
            for key, value in time_series_dict.items():
                class_id = self.get_id(Schema.Class, name=key)
                self._add_xml_child(Schema.Text.name, data_id=data_id, class_id=class_id, value=value)

    def add_membership(  # noqa: D102
        self,
        parent_object_name,
        child_object_name,
        /,
        *,
        parent_class,
        child_class,
        collection,
        **kwargs,
    ):
        parent_object_class_id = kwargs.pop("parent_object_class_id", None)
        child_object_class_id = kwargs.pop("child_object_class_id", None)
        if not parent_object_class_id:
            parent_object_class_id = self.get_id(Schema.Objects, name=parent_object_name, label="class_id")
        if not child_object_class_id:
            child_object_class_id = self.get_id(Schema.Objects, name=child_object_name, label="class_id")
        parent_object_id = self.get_id(
            Schema.Objects, name=parent_object_name, class_id=parent_object_class_id, **kwargs
        )
        child_object_id = self.get_id(
            Schema.Objects, name=child_object_name, class_id=child_object_class_id, **kwargs
        )

        self._add_membership(
            child_object_class_id,
            child_object_id,
            parent_object_class_id,
            parent_object_id,
            collection,
            Schema.Memberships,
        )

        return

    def add_object(
        self,
        class_enum: ClassEnum,
        object_name: str,
        category_name: str | None = None,
        object_enum: Schema = Schema.Objects,
        collection_enum: CollectionEnum = CollectionEnum.SystemGenerators,
    ) -> str:
        """Add a plexos object to the XML."""
        logger.trace("Adding object {}", object_name)
        category_id = self.add_category(class_enum, category_name)
        if _ := list(self.iter(object_enum, class_id=class_enum.value, name=object_name)):
            raise KeyError(
                f"{object_name=} already exist in Objects for Class={class_enum}. Pick another name."
            )

        object_id = self.get_max_id(object_enum) + 1

        # Assert the class is enabled
        class_tag = self._get_xml_element(Schema.Class, class_id=class_enum.value)
        setattr(class_tag.find("is_enabled"), "text", "true")

        self._add_xml_child(
            object_enum.name,
            object_id=object_id,
            class_id=class_enum.value,
            name=object_name,
            category_id=category_id if category_id else "",
            GUID=str(uuid.uuid4()),
        )

        # Default membership is the System
        system_class_id = self.get_id(Schema.Class, class_id=ClassEnum.System.value)
        system_object_id = self.get_id(Schema.Objects, class_id=system_class_id)
        self._add_membership(
            class_enum.value,
            object_id,
            system_class_id,
            system_object_id,
            collection_enum=collection_enum,
        )

        return str(object_id)

    def add_category(  # noqa: D102
        self,
        class_enum: ClassEnum,
        category_name: str | None = None,
        category_enum: Schema = Schema.Categories,
    ) -> int:
        if not category_name:
            logger.trace("Skipping empty category")
            return int(self.get_id(category_enum, class_id=class_enum.value))
        categories = self.iter(category_enum, class_id=class_enum.value)
        categories_names = list(map(lambda x: x.findtext("name"), categories))
        category_id = self.get_max_id(Schema.Categories) + 1  # Increase category number

        # Increase rank based on existing categories
        if not categories_names:
            rank = 0
        else:
            rank = len(categories_names)  # List start at 0

        if category_name in categories_names:
            logger.trace(f"{category_name=} already exists. Skipping")
            return int(self.get_id(Schema.Categories, class_id=class_enum.value, name=category_name))
        logger.trace("Adding category {} to Class {}", category_name, class_enum)
        self._add_xml_child(
            Schema.Categories.name,
            category_id=category_id,
            class_id=class_enum.value,
            rank=rank,
            name=category_name,
        )

        return category_id

    def _add_membership(
        self,
        child_class_id: int | str,
        child_object_id: int | str,
        parent_class_id: int | str,
        parent_object_id: int | str,
        collection_enum: CollectionEnum = CollectionEnum.SystemGenerators,
        membership_enum: Schema = Schema.Memberships,
    ):
        membership_id = self.get_max_id(membership_enum) + 1
        logger.trace(
            "Adding membership {} for {}.{}",
            membership_id,
            parent_object_id,
            child_object_id,
        )
        self._add_xml_child(
            membership_enum.name,
            membership_id=membership_id,
            parent_class_id=parent_class_id,
            parent_object_id=parent_object_id,
            collection_id=collection_enum.value,
            child_class_id=child_class_id,
            child_object_id=child_object_id,
        )
        return membership_id

    def _add_xml_child(self, child_name: str, **tag_elements):
        logger.trace("Creating new xml child with {}", child_name)
        xml_child = ET.SubElement(self.root, child_name)
        for key, value in tag_elements.items():
            new_tag = ET.SubElement(xml_child, key)
            if isinstance(value, float):
                text = round(value, 4)
            else:
                text = value
            new_tag.text = f"{text}"
        child_enum = str2enum(child_name)
        self._cache[child_enum.name].append(xml_child)
        # If cache is empty assign it to 1. This happen when we start from an empty xml.
        try:
            self._counts[child_enum.name] += 1
        except KeyError:
            self._counts[child_enum.name] = 1
        return xml_child

    def get_max_id(self, element_type: Schema):
        """Return max id for a given child class.

        Paramters
        ---------
        element_type
            XML parent tag to iterate over.
        """
        # element = list(self.iter(element_type.name))
        return max(0, self._counts.get(element_type.name, 0))

    def _get_xml_element(self, element_type: Schema, label: str | None = None, **kwargs) -> ET.Element:
        element_id = self.get_id(element_enum=element_type, label=label, **kwargs)
        element = list(self.iter(element_type, element_id, label=label))[0]  # noqa: RUF015
        return element

    def iter(
        self, element_type: Schema, *elements: Iterable[str | int], label: str | None = None, **tags
    ) -> Iterable[ET.Element]:
        """Return elements from the XML based on the type.

        This functions serves as a low-level query to the XML file.

        Parameters
        ----------
        element_type
            Enum of the Schema wanted, e.g., `Schema.Class`, `Schema.Objects`.
        *elements
            Sequence of ids, strings, or ints to get.
        label
            XML child label to extract. Defaults to `Schema[elementy_type].label`.
        **tags
            Additional key: value pairs to match the XML, e.g., `kwargs = {"class_id": 1}`.

        Return
        ------
            XML query match.
        """
        if not self.in_memory:
            yield from self._iter_elements(element_type.name, *elements, **tags)
            return

        if not elements:
            yield from self._cache_iter(element_type, **tags)

        # We assume that label comes from element_type
        if not label:
            label = element_type.label

        for element in elements:
            yield from self._cache_iter(element_type, **{f"{label}": element})

    def to_xml(self, fpath: str | PathLike) -> None:
        """Save memory xml to file."""
        ET.indent(self.tree)

        # Sorting elements by their text
        sorted_elements = sorted(self.root.findall("*"), key=lambda e: e.tag)
        self.root[:] = sorted_elements

        # Rebuilding the XML tree with sorted elements
        logger.debug("Saving xml file")
        self.root.set("xmlns", self.namespace)
        with open(fpath, "wb") as f:
            self.tree.write(
                f,
            )
        logger.info("Saved xml file to {}", fpath)

        logger.info("Deleting Cache")
        if self.in_memory:
            del self._cache
            del self._counts

        return None

    def _cache_iter(self, element_type: Schema, **tag_elements) -> Iterator | list:
        if not tag_elements:
            return iter(self._cache[element_type.name])
        if element_type.label not in tag_elements:
            return filter(construct_condition_lambda(**tag_elements), self._cache[element_type.name])
        index = int(tag_elements[element_type.label]) - 1
        return iter([self._cache[element_type.name][index]])

    def _iter_elements(self, element_type: str, *elements, **tag_elements) -> Iterator:
        """Iterate over the xml file.

        This method also includes a simple cache mechanism to re-use results.

        Paremeters
        ----------
        element_type
            XML parent tag to iterate over.
        *elements
            Strings to filter for a given children only
        **tag_elements
            Key-value to match a given tag
        """
        xpath_query = xml_query(element_type, *elements, **tag_elements)
        logger.trace("{}", xpath_query)
        elements = self.root.findall(xpath_query)
        yield from elements

    @lru_cache
    def get_valid_properties_list(self, collection_enum: CollectionEnum | None = None):
        """Return list of valid properties for the given Collection."""
        return list(
            map(
                lambda x: x.findtext("name"),
                self.iter(Schema.Property, collection_id=collection_enum),
            )
        )

    @lru_cache
    def get_valid_properties_dict(self, collection_enum: CollectionEnum | None = None):
        """Return list of valid properties for the given Collection."""
        return {
            x.findtext("property_id"): x.findtext("name")
            for x in self.iter(Schema.Property, collection_id=collection_enum)
        }

    def _remove_namespace(self, namespace: str) -> None:
        """Remove namespace in the passed document in place.

        Stolen from
        -----------
        [^1]:
        https://stackoverflow.com/questions/18159221/remove-namespace-and-prefix-from-xml-in-python-using-lxml
        """
        ns = "{%s}" % namespace  # noqa: UP031
        nsl = len(ns)
        for elem in self.root.iter():
            if elem.tag.startswith(ns):
                elem.tag = elem.tag[nsl:]

    def _validate_get_element_id(self, element_name, *elements, **tag_elements):
        try:
            element = self.get_id(element_name, *elements, **tag_elements)
        except KeyError:
            return None
        return element


def construct_condition_lambda(**kwargs):  # noqa: D103
    # Precompute the values of findtext calls
    findtext_values = {key: str(value) for key, value in kwargs.items() if value}

    # Construct the lambda function
    def condition(x):
        for key, value in findtext_values.items():
            if isinstance(value, Enum):
                value = value.value
            if x.findtext(key) != value:
                return False
        return True

    return condition


def xml_query(element_name: str, *tags, **tag_elements) -> str:
    """Construct XPath query for extracting data from a XML with no namespace.

    Parameters
    ----------
    element_name
        String that matches the desired element
    *tags
        Tag names to filter
    **kwargs
        Tag name and value child of the element. (E.g., class_id=2)

    Returns
    -------
    XPath query string constructed based on the provided conditions.

    Examples
    --------
    A simple example for one condition:

    >>> query_string = xml_query("t_object", class_id="2")
    >>> print(query_string)
    ".//t_object[class_id='2']"

    For multiple condition:

    >>> query_string = xml_query("t_object", class_id="2", enum_id="1")
    >>> print(query_string)
    ".//t_object[class_id='2'][enum_id='1']"
    """
    xpath_query = f".//{element_name}"
    for tag in tags:
        xpath_query += f"[{tag}]"
    for attr, value in tag_elements.items():
        if value:
            xpath_query += f'[{attr}="{value}"]'
    return xpath_query
