"""Checks used on plexosdb."""

MEMBERSHIP_FROM_RECORD_FIELDS = {
    "parent_object_id",
    "child_object_id",
    "collection_id",
    "child_class_id",
    "parent_class_id",
}


def check_memberships_from_records(memberships: list[dict[str, int]]) -> bool:
    """Check that all the records have the required fields for a membership."""
    return all(record.keys() == MEMBERSHIP_FROM_RECORD_FIELDS for record in memberships)
