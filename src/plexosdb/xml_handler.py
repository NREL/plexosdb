"""Plexos Input XML API."""

import xml.etree.ElementTree as ET  # noqa: N817
from collections import defaultdict
from collections.abc import Iterable, Iterator
from os import PathLike
from typing import TYPE_CHECKING

from loguru import logger

from .enums import Schema
from .utils import validate_string

PLEXOS_NAMESPACE = "http://tempuri.org/MasterDataSet.xsd"


class XMLHandler:
    """PLEXOS XML handler."""

    # Tell mypy that these attributes are never None after initialization
    if TYPE_CHECKING:
        root: ET.Element
        tree: ET.ElementTree

    def __init__(
        self,
        fpath: str | PathLike | None = None,
        namespace: str = PLEXOS_NAMESPACE,
        in_memory: bool = True,
        initialize: bool = False,
    ) -> None:
        self.fpath = fpath
        self.namespace = namespace
        self.in_memory = in_memory
        self._cache: dict = {}
        self._counts: dict = {}

        if initialize:
            self.root = ET.Element("MasterDataSet")
            self.tree = ET.ElementTree(self.root)

        # If we are parsing an XML file
        if fpath and not initialize:
            self.tree = ET.parse(fpath)
            self.root = self.tree.getroot()
            self._remove_namespace(namespace)

        # At this point both should be either define from a file or from initialize.
        assert self.root is not None
        assert self.tree is not None

        # Create in-memory cache to speed up searching on the document
        if self.in_memory:
            _cache = defaultdict(list)
            for element in self.root:
                _cache[element.tag].append(element)
            self._cache = _cache
            self._counts = {key: len(_cache[key]) for key in _cache}

    @classmethod
    def parse(cls, fpath: str | PathLike, namespace: str = PLEXOS_NAMESPACE, **kwargs) -> "XMLHandler":
        """Return XML instance from file requested."""
        return XMLHandler(fpath=fpath, namespace=namespace, **kwargs)

    def create_table_element(self, rows: list[tuple], column_types: dict[str, str], table_name: str) -> bool:
        """Create XML elements for a given table."""
        for row in rows:
            table_element = ET.SubElement(self.root, table_name)
            for (column_name, column_type), column_value in zip(column_types.items(), row):
                if column_value is None:
                    continue
                column_element = ET.SubElement(table_element, column_name)
                match column_type:
                    case "BIT":
                        match column_value:
                            case 1:
                                column_element.text = "true"
                            case 0:
                                column_element.text = "false"
                    case _:
                        column_element.text = str(column_value)
        return True

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

    def to_xml(self, fpath: str | PathLike) -> bool:
        """Save memory xml to file."""
        assert self.root is not None
        assert self.tree is not None
        ET.indent(self.tree)

        # Sorting elements by their text
        sorted_elements = sorted(self.root.findall("*"), key=lambda e: e.tag)
        self.root[:] = sorted_elements

        # Rebuilding the XML tree with sorted elements
        logger.debug("Saving xml file to {}", fpath)
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

        return True

    def _cache_iter(self, element_type: Schema, **tag_elements) -> Iterator | list:
        if not tag_elements:
            return iter(self._cache[element_type.name])
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

    def _remove_namespace(self, namespace: str) -> None:
        """Remove namespace in the passed document in place.

        Stolen from
        -----------
        [^1]:
        https://stackoverflow.com/questions/18159221/remove-namespace-and-prefix-from-xml-in-python-using-lxml
        """
        assert self.root is not None
        ns = "{%s}" % namespace  # noqa: UP031
        nsl = len(ns)
        for elem in self.root.iter():
            if elem.tag.startswith(ns):
                elem.tag = elem.tag[nsl:]


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
