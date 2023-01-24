# Schema Generator
<br/><br/>
Script that takes a formatted CSV or XLSX file of database information and generates all the DDL scripts needed to implement in the database (Oracle)
<br/><br/>
Built and tested using Python 3.8, 3.10. Requires Python 3.7+
<br/><br/>
Currently generates the following scripts:
- Table (including columns, datatypes, options (not null, default, etc.))
    - LOB handling with options
	- Virtual columns
	- Invisible columns
- Primary key (including index and constraint; compound keys)
- Sequences (including custom start value, sequence recycling, and trigger population)
- Additional Unique (with constraint) or Non-Unique Indexes
- Compound Indexes
- Foreign key constraints
- Simple Check Constraints (value matching on a single field)
- Standardized audit columns and trigger population (requires custom schema logging packages / synonyms be configured first)
- History tables with trigger population 
    - Option to using procedures in triggers
    - Option to use logging in procedures (requires custom schema logging packages / synonyms be configured first)
- Comments on tables and columns
- Grants (via second CSV file)
- Generated "build.sql" script on the root of the output for running all scripts
- Generated "clean.sql" script to reverse (most) changes in a build.sql run
- JSON config file for flexibility / options
	- Settings for:
		- Output directory and file names
		- History tables: Whether to use procedures or logging for triggers
		- Logging: Set logging level for application
		- Clean script: Toggle whether to generate clean script in output directory
		- Formatting: Spacing used in scripts
		- LOB Defaults: Default settings to use for LOB-type fields if not defined in CSV
<br/><br/>
# To use application from EXE (in 'dist' folder):
- Run once to generate empty CSV or XLSX document(s) and config file
<br/><br/>
- Fill in input file:
    - Grants file uses 'X' in the grant type columns to denote inclusion
        - Example: 'X' in the 'INSERT' column denotes 'GRANT SELECT, INSERT ON <x> TO <y>'
        - SELECT is always granted to a user in the file for a given table. A user in the file with no 'X' in other type fields will only be granted SELECT
    - Binary fields are generally denoted as 'Y' or 'N' with blank typically equalling 'N'
        - 'Index' also accepts 'U' to denote a unique index
		- 'Index' can also have a number after the letter to create groupings for compound indexes. For example, having multiple columns with 'U1' will combine those into a compound index and constraint
        - 'Index' can also have both a compound, regular, or even multiple compound indexes defined. Each of these should be separated by commas (e.g. 'Y,U1,U3' would be a non-unique single index and participation in 2 unique compound indexes). NOTE: If both unique and non-unique singular indexes is defined, the output will be a single unique index.
		- 'Primary key' also allows multiple columns to have 'Y' specified (no number) and all PK columns will be merged into a compound key
    - 'Units' only used on CHAR and VARCHAR2 fields and only if 'Size' is defined
        - 'Size' can be used without 'Units' for other fields such as NUMBERS
        - If 'Units' excluded on CHAR or VARCHAR2 'Type', Oracle default will be used ('BYTE')
	- 'Virtual Expression' is only used if 'Virtual' = 'Y'
		- This is the expression used to calculate a virtual column. Should be a valid SQL expression.
		- WARNING: SQL Expression is not parsed for validity. Table script could fail to run if expression is not valid
	- 'Simple Check Constraint' can be used to enforce a single or set of values for a single column
		- Should be formatting as a condition. For example: " = 'Y'", " in (0,1)", or " <> 'Bananas'"
		- WARNING: Condition is not validated. Invalid conditions could prevent table creation.
    - 'Sequence start' accepts:
        - An integer to denote the first value used in a new sequence 
        - A string that is the name of another sequence in the same schema (either existing or to be created)
            - New sequences are named '[tablename]_[fieldname]_SEQ', if you wish to predict the name of a sequence to be created elsewhere in the schema to reuse
            - NOTE: Nothing is done to link the sequence unless 'Pop by Trigger' is also 'Y'
    - 'Default' accepts some keywords (SYSDATE, USER, etc.) and will convert other values for CHAR and VARCHAR2 fields to strings
    - 'FK to Table' and 'FK to field' allow for foreign key creation
        - Both must be filled in to register
        - WARNING: This is not validated against other tables to confirm 'table.field' exists
    - 'Gen Audit Columns', 'Gen History Table', and 'Table Comment' are only registered if populated on the first line for a given table.
        - Anything populated on subsequent lines will be ignored
<br/><br/>
- Run EXE again to generate scripts 
    - They will be put into a folder called 'output'
    - Subsequent runs will clear the directory (delete *everything*) and re-output the scripts
        - This is to eliminate cases where a file would not be overwritten because nothing generates with the same name, which would create additional (invalid) scripts and include them in the build script
