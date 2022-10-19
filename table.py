#!/usr/bin/env python
from __future__ import annotations

"""table.py: Module provided classes for structure / organization"""

__author__ = "Cody Putnam (csp05)"
__version__ = "22.10.19.0"

from schema_generator import config, logger, default_schema_row

# Load LOB defaults from config file
lob_defaults = {
    'deduplication': config['lob-defaults']['deduplication']['setting'],
    'compression': config['lob-defaults']['compression']['setting'],
    'caching': config['lob-defaults']['caching']['setting'],
    'logging': config['lob-defaults']['logging']['setting']
}

# Keywords to exclude from wrapping in quotes if used as default values
default_kw = [
    'SYSDATE',
    'USER',
    'SYSTIMESTAMP',
]

class Table:
    """
    A class to represent a database table object

    Attributes:
    -----------
    schema: str
        Schema name 
    name: str
        Table name
    columns: list
        List of Column objects associated with the table
    comment: str
        Table comment to include in database
    tableNumber: int
        Number of the table in the order of all tables to be processed
        Used to prefix script files to run them in a particular order
    tablespace: str
        Tablespace for table. Derived from the schema name (drop '_OWNER' from common schema name)
    sourcetable: str
        If table is history table, this contains the original table's name. Not currently used
    primarykeys: list
        List of all primary keys on a table. If more than a single entry, a compound PK should be generated
    compindex: dict
        Dictionary containing all the compound index combinations requested
    compindexfields: dict
        Simplified dictionary containing just field names from compindex. 
        Stored for caching purposes for repeat use
    columnList: list
        List of tuples containing core column details needed for script generation. 
        Stored for caching purposes for repeat use
    needsaudit: bool
        Flag holding whether audit columns should be injected into table structure. 
        NOTE: Should be done *after* history table generation to exclude audit columns / trigger from history
    needshistory: bool
        Flag holding whether history table should be generated for this table
        NOTE: Should be run before audit column injection to exclude audit columns from history
    ishistory: bool
        Flag for if table is a history table
    indexcount: int
        Count of indexes on the table. Incremented to generate unique scripts
    fkcount: int
        Count of FKs on the table. Incremented to generate unique scripts
    
    Methods:
    -----------
    addColumn(col: Column)
        Adds a Column object to the columns attribute of the Table
        Also compiles compound PKs and Indexes as columns are loaded
    addColumnsForHistory(cols: list)
        For each column in cols, creates a duplicate object, resets most attributes (to reduce constraints for history),
        and appends to the columns attribute of history Table
    genColumnList() -> list
        Generates a list of tuples with each holding core info for a given Column needed for script generation
    hasCompoundPK() -> bool
        Returns True if length of primarykeys attribute greater than 1
    getPKFields() -> list
        Returns as list of all the field names for Columns in the primarykeys attribute
    cleanCompoundIndex()
        Function to walk over compindex and remove any entries with less than 2 indexes.
        These will get processed with the individual columns for indexes
    hasCompoundIndex() -> bool
        Returns True if compindex is not empty. cleanCompoundIndex() should be run prior to using this for best results
    getIndexFields() -> dict
        Returns a dict of all the fields in compound indexes.
        Runs cleanCompoundIndex() and later caches result in compindexfields for quick retrieval on subsequent runs
    isFieldInCompoundIndex(field: str) -> bool
        Compares supplied field to see if in compindexfields.
        If compindexfields is empty, but compindex has data, runs getIndexFields() first
    genAuditColumns()
        Injects U_NAME and U_DATE Columns into Table
    genHistoryTable() -> Table
        Returns new Table that is mirrored from original table, with 2 new Columns added (HIST_ID, CHANGE)
        Creates new instances of all Column objects and updates their attributes to reduce contraints on history
        Returns a None-type object if Table should not have history
    """

    def __init__(self, schema: str, tablename: str, genAudit: str, genHistory: str, comment: str, tableNumber: int = 1, historyTable: bool = False, histSourceTableName: str = ''):
        """
        __init__(schema, tablename, genAudit, genHistory, comment, historyTable, historySourceTableName)

        Creates a new Table object
    
        Parameters:
            schema: str
                Name of the database schema the table will reside in
            tablename: str
                Name of database table
            genAudit: str
                Flag for whether audit columns should be injected into table
                Values: 'Y' == True, 'N' or '' == False
            genHistory: str
                Flag for whether a corresponding history table and support structure should be generated
                Values: 'Y' == True, 'N' or '' == False
            comment: str
                Database level table comment to be added
            tableNumber: int
                Number of the table in the order of all tables to be processed
                Used to prefix script files to run them in a particular order
            historyTable: bool
                Flag to show whether current table is, in fact, a history table
            historySourceTableName: str
                If historyTable is True, this is the name of the original table the history table is based on            
        """

        self.schema = schema
        self.name = tablename
        self.columns = []
        self.comment = comment
        self.tableNumber = tableNumber
        self.tablespace = schema.upper().split('_OWNER')[0]
        self.sourcetable = histSourceTableName
        self.primarykeys = []
        self.compindex = {}
        self.compindexfields = {}
        self.columnList = []
        self.needsaudit = False
        self.needshistory = False
        self.ishistory = historyTable
        self.indexcount = 1
        self.fkcount = 1

        if genAudit.upper() == 'Y':
            self.needsaudit = True
        
        if genHistory.upper() == 'Y':
            self.needshistory = True

    def addColumn(self, col: Column):
        """
        addColumn(col)

        Adds a new Column object to columns attribute. 
        Also checks column and adds to primarykeys or compindex, if appropriate
    
        Parameters:
            col: 'Column'
                Column object to be added to columns attribute
        """

        # Check if primary key first and append to primarykeys attribute
        if col.primarykey:
            self.primarykeys.append(col)

        # Check if it has an indexvalue, which would signify a compound index
        if len(col.indexvalue) > 0:
            for index in col.indexvalue:

                # If index in compindex, add Column to existing list
                if index in self.compindex.keys():
                    self.compindex[index].append(col)
                
                # If index not in compindex yet, start new list and assign to value index
                else:
                    self.compindex[index] = [col]
        # Add Column to columns attribute
        self.columns.append(col)


    def addColumnsForHistory(self, cols: list):
        """
        addColumnsForHistory(cols)

        For Column object in cols, duplicates the column then resets many attributes of Column object.
        This reduces the overall number of constraints on the history table (not null, default, etc.)
    
        Parameters:
            cols: list
                List of Column objects to be added to columns attribute
        """

        for column in cols:
            # Create a copy of the column to avoid modifying original instance
            col = column.duplicateColumn()

            # Remove constraints from history table columns to more freely allow records
            col.notnull = False
            col.primarykey = False
            col.default = None
            col.indexed = False
            col.indexvalue = []
            col.unique = False
            col.sequenced = False
            col.sequencestart = 1
            col.triggered = False
            col.fksourcetable = None
            col.fksourcefield = None
            col.virtual = False
            col.virtualexpr = None

            # Add Column to Table object
            self.addColumn(col)
    

    def genColumnList(self) -> list:
        """
        genColumnList()

        Generates a list of Column details needed for table script generation
    
        Returns:
            list
                List of tuples, each containing core Column details needed to generate table scripts
        """

        # Check if columns and columnList are the same size. 
        # If yes, use cached columnList, otherwise regenerate
        if len(self.columnList) != len(self.columns):
            self.columnList = []
            for col in self.columns:
                # Append tuple of (field, type-string, options-string, lob-options-dict)
                self.columnList.append((col.field, col.getTypeString(), col.getOptionsString(), col.getLobOptions()))
        return self.columnList


    def hasCompoundPK(self) -> bool:
        """
        hasCompoundKey()

        Returns whether the Table object has a compound primary key. 
    
        Returns:
            bool
                True if primarykeys attribute is longer than 1
        """

        # Return True if primarykeys has more than 1 entry, else False
        return (len(self.primarykeys) > 1)
    

    def getPKFields(self) -> list:
        """
        getPKFields()

        Returns a list of all of the field names for any columns in primarykeys
    
        Returns:
            list
                List of field names of columns in primarykeys
        """

        l = []
        # For all columns in primarykeys
        for col in self.primarykeys:
            l.append(col.field)
        return l


    def cleanCompoundIndex(self):
        """
        cleanCompoundIndex()

        Iterates over compindex dict to find any indexes with only one entry and removes them as invalid
        Those removed should later get processed with a regular index alongside the other individual columns

        """

        for key in self.compindex.keys():

            # If less than 2 elements, remove from compindex as invalid
            if len(self.compindex[key]) < 2:
                logger.warn(f'Group Index key {key} on {self.name} does not have enough fields for compound index. Removing to be processed as standard index...')
                self.compindex.pop(key)


    def hasCompoundIndex(self) -> bool:
        """
        hasCompoundIndex()

        Returns True if compindex has any entries in it.
        For best results, run *after* cleanCompoundIndex()
    
        Returns:
            bool
                Returns True if compindex is not empty
        """

        # Return True if compindex is not empty
        return bool(self.compindex)
    

    def getIndexFields(self) -> dict:
        """
        getIndexFields()

        Returns a dict of all compound index combinations' fields
    
        Returns:
            dict
                Dictionary of all fields in combinations in compindex
        """

        # Check if compindex has records and if compindexfields exists
        # If compindexfields exists, just use, else generate compindexfields
        logger.debug('Checking cache and generating Compound Index Fields')
        if self.compindex and not self.compindexfields:

            # Run cleanCompoundIndex() to eliminate any bad data from compindex before processing
            self.cleanCompoundIndex()
            l = {}

            # For record in compindex
            for key in self.compindex.keys():

                # If key not in output, add empty list to initialize
                if key not in l.keys():
                    l[key] = []

                # Append all Column fields to output list in dict
                for col in self.compindex[key]:
                    l[key].append(col.field)
            
            # Cache output dict in compindexfields
            self.compindexfields = l
        
        # Output compindexfields
        return self.compindexfields


    def isFieldInCompoundIndex(self, field: str) -> bool:
        """
        isFieldInCompoundIndex(field)

        Checks field against all records in compindexfields and returns True if found
        Will generate compindexfields if not present
        This is used to determine if Column should be indexed individually or as compound during script generation
    
        Parameters:
            field: str
                Column field to compare to compindexfields

        Returns:
            bool
                Returns True if field is found in compindexfields, else False
        """

        # Is there a compindex at all?
        if self.compindex:
            # If yes, is there a compindexfields attribute yet? If not, generate
            if not self.compindexfields:
                self.getIndexFields()

            # For key in compindexfields, see if field is present in the sublist and return True if it is
            for key in self.compindexfields:
                if field in self.compindexfields[key]:
                    return True
        # Return False if field not found or compindex is empty (no compound indexes)
        return False
    

    def isCompoundIndexUnique(self, key: int) -> bool:
        """
        isCompoundIndexUnique()

        Checks first column in compindex for if the index should be processed as unique

        Parameters:
            key: int
                compindex key to check for unique column property

        Returns:
            bool
                Returns True if first column in compindex[key] is unique, else False
        """

        # Is there a compindex at all?
        if self.compindex:
            try:
                if key in self.compindex.keys():
                    if key[0] == 'U':
                        return True
            except:
                logger.error('Something went wrong trying to determine if compound index is unique. Returning False')
                return False
        # Return False if key not found or compindex is empty (no compound indexes)
        return False


    def genAuditColumns(self):
        """
        genAuditColumns()

        Adds U_NAME and U_DATE columns to end of Table column list
    
        """

        # Confirm audit columns are needed on this table
        if self.needsaudit:
            # Create two columns for audit logging
            cols = spawnAuditColumns(self.schema, self.name)
            # Add columns to Table
            self.addColumn(cols[0])
            self.addColumn(cols[1])


    def genHistoryTable(self, tableNum: int = None) -> Table:
        """
        genHistoryTable()

        Generates a new Table object based on the columns and attributes of the existing Table

        Returns:
            Table
                Returns a new Table object configured as a history table
        """

        # Confirm history table is expected for this table
        if self.needshistory:
            if tableNum is None:
                tableNum = self.tableNumber
            # Create new Table
            # Disable Audit and History, name table H_<sourcetablename>, add comment, set historyTable to True and set source name
            histTable = Table(self.schema, f'H_{self.name}', 'N', 'N', f'History table for {self.name}', tableNum, True, self.name)

            # Spawn 2 history columns (HIST_ID, CHANGE) and add to new table
            # Done first to position them at beginning of table
            cols = spawnHistoryColumns(self.schema, self.name)
            for col in cols:
                histTable.addColumn(col)

            # Add rest of columns to history table
            histTable.addColumnsForHistory(self.columns)
        
        # If history table not requested for this table, return a None-type object
        else:
            histTable = None
        
        # Return Table (or None)
        return histTable


##############################################
class Column:
    """
    A class to represent a database table object

    Attributes:
    -----------
    origrow: dict
        Copy of original dictionary passed to create the column
    field: str
        Name of column to be added to Table in database
    type: str
        Type of data to be recorded in column (i.e. VARCHAR2, NUMBER, DATE, etc.)
    size: str
        Size of field / type. Numeric value
    units: str
        Unit of measure for VARCHAR2 or CHAR fields. Acceptable values include 'CHAR' and 'BYTE'
    notnull: bool
        Flag for whether the field should be nullable in the database
    primarykey: bool
        Flag for whether the field should be processed as a primary key for the Table
    default: str
        Default value for the field in the database. 
        Will wrap strings in " for VARCHAR2 and CHAR type fields unless present in default_kw
    indexed: bool
        Should field be indexed and have those scripts generated
    indexvalue: list
        List of clusters to include this field for compound indexes
    unique: bool
        Flag whether field should be indexed as unique with appropriate constraints
    sequenced: bool
        Flag for whether the field should have a sequence assigned
    sequencestart: int
        Starting value for assigned sequence
    sequencetouse: str
        Existing (or created sequence) to reuse for this field (must be in same schema)
    triggered: bool
        Flag for whether sequence should be populated by a trigger on insert
    invisible: bool
        Flag for whether this is an invisible field. 
    virtual: bool
        Flag for whether this is a virtual field. 
        If True, virtualexpr should contain a string expression column should evaluate to
    virtualexpr: str
        String containing a (hopefully) valid SQL expression that the virtual column should resolve to
    checkconstraint: str
        String containing a condition for this column to be used in a simple Check Constraint
        Examples include: " = 'Y'", " in (0,1)", " <> 'Tetris'" 
        (space at the beginning helps prevent field being parsed as formula if CSV is opened in Excel)
    fksourcetable: str
        Source table name for Foreign Key generation
    fksourcefield: str
        Source field name (in fksourcetable) for Foreign Key generation
    comment: str
        Column level comment to include in database
    lob_dedup: bool
        Flag to toggle deduplication on LOB type fields
    lob_compress: str
        Level to set LOB compression setting to
    lob_cache: bool
        Flag to toggle caching on LOB type fields
    lob_logging: bool
        Flag to toggle logging on LOB type fields
    
    Methods:
    -----------
    load(csvrow: dict)
        Loads in values from provided dict representing 1 row in CSV file
    getTypeString() -> str
        Compiles the type, size, and units attributes into a single string and returns it
    getOptionsString() -> str
        Compiles settings like notnull and default into a single string for use in table script
    getLobOptions() -> dict
        Returns dict of the various LOB type options or a None-type object if field is not a LOB type
    duplicateColumn() -> 'Column'
        Returns a copy of the column rebuilt from origrow attribute
    """

    def __init__(self):
        """
        __init__()

        Creates a new Column object. Sets default values, but needs a call to load() to be properly populated
               
        """

        self.field = ''
        self.type = ''
        self.size = ''
        self.units = ''
        self.notnull = False
        self.primarykey = False
        self.default = None
        self.indexed = False
        self.indexvalue = []
        self.unique = False
        self.sequenced = False
        self.sequencestart = 1
        self.sequencetouse = None
        self.triggered = False
        self.invisible = False
        self.virtual = False
        self.virtualexpr = None
        self.checkconstraint = None
        self.fksourcetable = None
        self.fksourcefield = None
        self.comment = None
        self.lob_dedup = False
        self.lob_compress = None
        self.lob_cache = False
        self.lob_logging = False

    def load(self, csvrow: dict):
        """
        load(csvrow)

        Loads row from CSV file into Column to establish values
    
        Parameters:
            csvrow: dict
                Row from CSV file in keyed dict format           
        """

        # Save original row dict in case Column must be copied later
        self.origrow = csvrow

        # Name of column / field in database
        self.field = csvrow["field"].upper()

        # Type of data represented by column
        self.type = csvrow["type"].upper()

        # Size of column for applicable field types
        if csvrow["size"].isdigit() and self.type in ['VARCHAR2','CHAR']:
            # Convert size to integer to compare with min and max allowed values
            tempsize = int(csvrow["size"])
            if tempsize < 1:
                # Ignore size if less than 1
                self.size = '' 
            elif tempsize > 4000:
                # Max size = 4000 in standard DB config
                self.size = '4000' 
            else:
                self.size = csvrow["size"]
        elif self.type == 'NUMBER' and csvrow["size"] != '':
            self.size = csvrow["size"]

        # Save units if in acceptable values ('BYTE', 'CHAR')
        if csvrow["units"].upper() in ['BYTE', 'CHAR']:
            self.units = csvrow["units"].upper()
        
        # Set notnull to True if flag in CSV
        if csvrow["not_null"].upper() == 'Y':
            self.notnull = True
        
        # Set primarykey to True if flag in CSV
        if csvrow["primary_key"].upper() == 'Y':
            self.primarykey = True
        
        # Set default value to use
        if csvrow["default"] != '': 
            self.default = csvrow["default"]
            # If VARCHAR2 or CHAR and value not in default_kw, wrap in ""
            if self.type in ['VARCHAR2', 'CHAR'] and self.default.upper() not in default_kw:
                self.default = f"'{self.default}'"                

        if csvrow["index"] != '':
            for i in csvrow["index"].upper().split(','):
                # If direct match, no cluster provided and will process as individual index
                if i in ['Y', 'U']:
                    self.indexed = True
                    # If value is U, mark as unique index 
                    if i == 'U':
                        self.unique = True
                # If first char is valid, try to process for cluster numbers for compound index
                elif i[0] in ['Y', 'U']:
                    # Cluster values must be numbers
                    if i[1:].isdigit():
                        self.indexvalue.append(i)

        # If sequence field contains a number, mark as sequenced and save sequencestart
        if csvrow["sequence_start"].isdigit():
            self.sequenced = True
            self.sequencestart = int(csvrow["sequence_start"])
            # If trigger flag is Y, mark triggered = True
            if csvrow["pop_by_trigger"].upper() == 'Y':
                self.triggered = True
        # If sequence field has some other string, assume this is the name of another sequence to use
        elif csvrow["sequence_start"] != '':
            self.sequenced = True
            self.sequencetouse = csvrow["sequence_start"]
            # If trigger flag is Y, mark triggered = True
            if csvrow["pop_by_trigger"].upper() == 'Y':
                self.triggered = True

        # If invisible is 'Y', mark as invisible
        if csvrow['invisible'].upper() == 'Y':
            self.invisible = True

        # If virtual is 'Y' and virtual expression is not empty, 
        # Mark as virtual and save the expression in virtual expression
        # Only certain types allowed to be used as virtual to prevent issues
        if self.type in ['DATE', 'VARCHAR2', 'CHAR', 'NUMBER', 'TIMESTAMP'] \
            and csvrow['virtual'].upper() == 'Y' \
            and csvrow['virtual_expr'] != '':
            self.virtual = True
            self.virtualexpr = csvrow['virtual_expr']

        # If check_constraint is not empty, record checkconstraint condition
        if csvrow['check_constraint'] != '':
            self.checkconstraint = csvrow['check_constraint'].strip()

        # If both FK fields are populated, save to fksource__ attributes
        if csvrow["fk_to_table"] != '' and csvrow["fk_to_field"] != '':
            self.fksourcetable = csvrow["fk_to_table"]
            self.fksourcefield = csvrow["fk_to_field"]
        
        # If comment has a value, save to comment attribute
        if csvrow["column_comment"] != '':
            self.comment = csvrow["column_comment"].strip("'")
        
        # LOB Options - only if type in ('BLOB', 'CLOB')
        if self.type in ['BLOB', 'CLOB']:
            # Set LOB deduplication from CSV
            if csvrow["lob_deduplication"].upper() == 'Y':
                self.lob_dedup = True
            elif csvrow["lob_deduplication"].upper() == 'N':
                self.lob_dedup = False
            # If CSV value is invalid or blank, use default from config file
            elif lob_defaults["deduplication"].upper() == 'Y':
                self.lob_dedup = True
            
            # Set LOB compression from CSV
            if csvrow["lob_compression"].upper() in ['N', 'LOW', 'MEDIUM', 'HIGH']:
                self.lob_compress = csvrow["lob_compression"].upper()
            # If CSV value is invalid or blank, use default from config file
            elif lob_defaults["compression"].upper() in ['N', 'LOW', 'MEDIUM', 'HIGH']:
                self.lob_compress = lob_defaults["compression"].upper()

            # Set LOB compression from CSV
            if csvrow["lob_caching"].upper() == 'Y':
                self.lob_cache = True
            elif csvrow["lob_caching"].upper() == 'N':
                self.lob_cache = False
            # If CSV value is invalid or blank, use default from config file
            elif lob_defaults["caching"].upper() == 'Y':
                self.lob_cache = True

            # Set LOB logging from CSV
            if csvrow["lob_logging"].upper() == 'Y':
                self.lob_logging = True
            elif csvrow["lob_logging"].upper() == 'N':
                self.lob_logging = False
            # If CSV value is invalid or blank, use default from config file
            elif lob_defaults["logging"].upper() == 'Y':
                self.lob_logging = True
    
    def getTypeString(self) -> str:
        """
        getTypeString()

        Compiles a string from the Column type, size, and units and returns it
    
        Returns:
            str
                Formatted string for use in script generation            
        """

        # Start with the value for type
        outString = self.type
        # If type in ('CHAR', 'VARCHAR2', 'NUMBER), include the size and units if available
        if self.type.upper() in ['CHAR', 'VARCHAR2', 'NUMBER']:
            if self.size.isdigit() and self.units != '':
                outString = f'{outString}({self.size} {self.units})'
            # If only size is defined or size is not a number, skip units
            elif self.size != '':
                outString = f'{outString}({self.size})'
        # Return compiled string
        return outString


    def getOptionsString(self) -> str:
        """
        getOptionsString()

        Compiles a string from the Column notnull and default and returns it
    
        Returns:
            str
                Formatted string for use in script generation            
        """

        outString = ''
        if self.invisible:
            # Invisible can be used with other options below
            outString = f'INVISIBLE'
        if self.virtual:
            # Virtual columns preclude other options
            outString = f'{outString}    AS ({self.virtualexpr}) VIRTUAL'
        else:
            # If default has a value, append 'DEFAULT <value>'
            if self.default != None:
                outString = f'{outString}    DEFAULT {self.default}'
            # If notnull == True, add 'NOT NULL' to options string
            if self.notnull:
                outString = f'{outString}    NOT NULL'
            
        # Return compiled string
        return outString.strip()
    

    def getLobOptions(self) -> dict:
        """
        getLobOptions()

        Compiles a dict from the Column lob_dedup, lob_compress, lob_cache, and lob_logging attributes and returns it
    
        Returns:
            dict
                Dict of LOB options to use in script generation            
        """

        d = None
        # Confirm type in ('BLOB', 'CLOB')
        if self.type in ['BLOB', 'CLOB']:
            d = {
                "lob_deduplication": self.lob_dedup,
                "lob_compression": self.lob_compress,
                "lob_caching": self.lob_cache,
                "lob_logging": self.lob_logging
            }
        return d


    def duplicateColumn(self) -> Column:
        """
        duplicateColumn()

        Returns an exact copy of the current Column as it was originally created
    
        Returns:
            'Column'
                Copy of Column object as originally defined            
        """

        # Create new Column and populate with original record from current Column
        col = Column()
        col.load(self.origrow)
        return col



##############################################
def spawnAuditColumns(schema: str, tablename: str) -> tuple:
    """
    spawnAuditColumns(schema, tablename)

    Returns 2 new Column objects in a tuple. These can then be added to Table object

    Returns:
        tuple
            Object 1 is U_NAME Column and object 2 is U_DATE Column           
    """

    #Add U_NAME column
    u_name = default_schema_row.copy()
    u_name["schema"] = schema
    u_name["table"] = tablename
    u_name["field"] = 'U_NAME'
    u_name["type"] = 'VARCHAR2'
    u_name["size"] = '250'
    u_name["units"] = 'CHAR'
    u_name["not_null"] = 'Y'
    u_name["default"] = 'USER'
    u_name["column_comment"] = 'User Name for audit logging purposes'

    col1 = Column()
    col1.load(u_name)
    
    #Add U_DATE column
    u_date = default_schema_row.copy()
    u_date["schema"] = schema
    u_date["table"] = tablename
    u_date["field"] = 'U_DATE'
    u_date["type"] = 'DATE'
    u_date["not_null"] = 'Y'
    u_date["default"] = 'SYSDATE'
    u_date["column_comment"] = 'Date / Time for audit logging purposes'

    col2 = Column()
    col2.load(u_date)

    return (col1, col2)


def spawnHistoryColumns(schema: str, tablename: str) -> tuple:
    """
    spawnHistoryColumns(schema, tablename)

    Returns 2 new Column objects in a tuple. These can then be added to Table object

    Returns:
        tuple
            Object 1 is HIST_ID Column and object 2 is CHANGE Column           
    """

    #Add HIST_ID column
    hist_id = default_schema_row.copy()
    hist_id["schema"] = schema
    hist_id["table"] = tablename
    hist_id["field"] = 'HIST_ID'
    hist_id["type"] = 'NUMBER'
    hist_id["not_null"] = 'Y'
    hist_id["primary_key"] = 'Y'
    hist_id["sequence_start"] = '1'
    hist_id["pop_by_trigger"] = 'Y'
    hist_id["column_comment"] = 'Unique ID for History record'

    col1 = Column()
    col1.load(hist_id)
    
    #Add CHANGE column
    change = default_schema_row.copy()
    change["schema"] = schema
    change["table"] = tablename
    change["field"] = 'CHANGE'
    change["type"] = 'VARCHAR2'
    change["size"] = '10'
    change["units"] = 'CHAR'
    change["not_null"] = 'Y'
    change["column_comment"] = 'Type of change performed'

    col2 = Column()
    col2.load(change)

    #Add CHANGE_DATE column
    changedate = default_schema_row.copy()
    changedate["schema"] = schema
    changedate["table"] = tablename
    changedate["field"] = 'CHANGE_DATE'
    changedate["type"] = 'DATE'
    changedate["not_null"] = 'Y'
    changedate["default"] = 'SYSDATE'
    changedate["column_comment"] = 'Time of change performed'

    col3 = Column()
    col3.load(changedate)

    #Add CHANGE_USER column
    changeuser = default_schema_row.copy()
    changeuser["schema"] = schema
    changeuser["table"] = tablename
    changeuser["field"] = 'CHANGE_USER'
    changeuser["type"] = 'VARCHAR2'
    changeuser["size"] = '50'
    changeuser["units"] = 'CHAR'
    changeuser["not_null"] = 'Y'
    changeuser["default"] = 'USER'
    changeuser["column_comment"] = 'DB USER that performed change'

    col4 = Column()
    col4.load(changeuser)

    return (col1, col2, col3, col4)