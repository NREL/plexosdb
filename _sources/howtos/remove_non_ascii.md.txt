# Removing non-ASCII text from names

This in an example function that removes all non-ASCII characters.

```python
import unicodedata

def clean_international_text(text: str) -> str:
    normalized = unicodedata.normalize('NFKD', text)
    ascii_text = normalized.encode('ASCII', 'ignore').decode('ASCII')
    return ascii_text

from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum, CollectionEnum
db = PlexosDB()
db.create_schema()

original_name="Pälli")
db.add_object(ClassEnum.Generator, original_name)
new_name = clean_international_text(original_name)
assert db.update_object(ClassEnum.Generator, "Pälli", new_name=new_name)
```
