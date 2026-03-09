# Database-Independent-Object-Mapper

To make use of this project, the files in the core and front_end folder need to be downloaded, along with the specific database connection wanted from the datastores folder. 
In the event that you want to use a database which is not currently supported, a class that does support it can be created in a new file. To add one such class, two steps need to be completed.
The first is the addition of a connection to that database, added to the connection file in the front end. The new class must import this connection to be used, to make switching database connection easier. 
The second is the template inheritance. The newly created class has to inherit the template class in the core, and make sure all abstract functions assigned there are created in the new class for the new database, making sure input and output signatures are respected. This allows the newly created class to make use of the created generic relational algebra functionality.

Once the correct files are installed, the application can be used. To make use of the implementation the first step is to assign a database connection, in the front end. The second step is assigning a data schema to be used by the database. An example schema for all three implemented databases is currently present in the schema file, make note of the small, necessary, addition to the SQLAlchemy schema. These schema classes are how the data is stored, and all functions present in the template take them as input or output. Furthermore the schema defined class names can be used as input for the RA functionality present in the core.
While most input and output signatures are fairly self-explanatory, one that is of note is within the query of the find_by_field function. This query takes as input a dictionary assigning what query functionality to use. The value of this dictionary can be a simple string, to signify a direct match, or another query layer, that assigns a different operation. As such, this query input should look like {field: "value"} or {field: {operation: "value}}. The possible operations here are as follows.
- $sub: Finds all values with the provided value as a substring.
- $regex: Allows the user to input a regex pattern.
- Numeric operators: Allows the user to search on numeric values with the operations "$gt", "$gte", "$lt", "$lte", "$eq", "$ne".

The same input structure is used by the RA Select function, with identical query requirements.
