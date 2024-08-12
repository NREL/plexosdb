"""Plexos Input XML API."""

from collections import defaultdict
import xml.etree.ElementTree as ET  # noqa: N817
from collections.abc import Iterable, Iterator
from enum import Enum
from functools import lru_cache
from os import PathLike

from loguru import logger

from .exceptions import MultlipleElementsError
from .enums import CollectionEnum, Schema
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
        self._cache: dict = {}
        self._counts: dict = {}

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
                    rename_dict.get(e.tag, e.tag): validate_string(e.text)  # type: ignore
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
        elements = self.root.findall(xpath_query)  # type: ignore
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
