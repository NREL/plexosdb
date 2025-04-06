# Adding Attributes to the objects

Objects in PlexosDB can have attributes that are saved on the `t_attribute_data`
table.


To see the list of available attributes per `ClassEnum` use:
```python
from plexosdb import PlexosDB, ClasEnum
db = PlexosDB.from_xml("/path/to/your/xml")

attributes = db.list_attributes(ClasEnum.Generator)
print(attributes)
```
