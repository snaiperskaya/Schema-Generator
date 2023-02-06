#!/usr/bin/env python

"""sql_script.py: Module containing the strings and code needed to generate and save SQL (Oracle) DDL Scripts"""

__author__ = "Cody Putnam (csp05)"
__version__ = "23.02.06.5"

import os
import copy
from schema_generator import logger, config
from table import Table, Column

# Load settings from config file (config.py -> conf.json)
use_package = config['history-tables']['use-procedures']['setting']
use_sql_logging = config['history-tables']['use-logging']['setting']
loader_package = config['loader-package']['enable']['setting']
loader_package_charlimit = config['loader-package']['enforce-char-lengths']['setting']
loader_package_logging = config['loader-package']['use-logging']['setting']
loader_package_deletes = config['loader-package']['include-delete']['setting']
split_on = config['formatting']['split_on']['setting']
table_min_spacing = config['formatting']['table_min_spacing']['setting']

outputDir = config['files']['output-directory']['setting']

# Template for history package temp storage
package_template = {
    'header': [],
    'body': []
}

# Initialize variables to use
history_package_schema = {}
loader_package_schema = {}
comments = []
grants = []
grantsExec = []

# Tab == 4 spaces to ensure clean and consistent formatting in scripts
tab = '    '

# Trigger types for history triggers. Values represent which data to save in transaction
triggerTypes = {
                'INSERT': 'NEW',
                'UPDATE': 'NEW',
                'DELETE': 'OLD'
                }


def writeTableScript(schema: str, tablename: str, columns: list, tablespace: str, outdirectory: str = outputDir):
    """
    writeTableScript(schema, tablename, columns, tablespace, outDirectory)

    Generates a CREATE TABLE script for table defined and saves to output\TABLES
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to be created
        columns: list
            List of Columns output from Table.getColumnList()
        tablespace: str
            Tablespace to apply to script
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    logger.info(f'Prepping {tablename} script')

    # Pull columns from list and format into line-by-line structure for table script
    lobs = []
    columnsFormatted = ""
    for col in columns:

        # If column has a LOB options defined, create and append LOB subscript to lobs list
        if col[3] != None:
            lobs.append(lobAsSecureFile(col[0], tablespace, col[3]))

        # If column name length is greater than table_min_spacing, apply 2 spaces as minimum
        if len(col[0]) >= table_min_spacing:
            spacing = ' ' * 2
        # If column name length is shorter than table_min_spacing, 
        # apply spaces equal to table_min_spacing less the length of the name
        else:
            spacing = ' ' * (table_min_spacing - len(col[0]))

        # If column has no options, skip and only add name and type
        if col[2] == '':
            columnsFormatted = columnsFormatted + f'{tab}{col[0]}{spacing}{col[1]},\n'
        # Else add name, type, and options with appropriate spacing based on table_min_spacing
        else:
            spacing2 = ' ' * (table_min_spacing - len(col[1]))
            columnsFormatted = columnsFormatted + f'{tab}{col[0]}{spacing}{col[1]}{spacing2}{col[2].lstrip()},\n'

    # Strip extra , and newline character from end of list
    columnsFormatted = columnsFormatted.rstrip(',\n')
    logger.debug(columnsFormatted)

    # Compile all LOB scripts into one block to add to end of table script
    lobString = ''
    if len(lobs) > 0:
        for lob in lobs:
            lobString = f'{lobString}{lob}\n'
    
    # Populate fields in script template
    toWrite = f'prompt --Adding {schema}.{tablename} table\n\n' \
            f'CREATE TABLE {schema}.{tablename}\n' \
            '(\n' \
            f'{columnsFormatted}\n' \
            ')\n' \
            f'{lobString}' \
            f'TABLESPACE {tablespace};\n' \
            '/\n'

    # Write script to file. Create directory if not present
    logger.info(f'Writing {tablename} to file')
    directory = f'{outdirectory}\\TABLES\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}{tablename}.sql', 'w') as f:
        f.write(toWrite)


def lobAsSecureFile(column: str, tablespace: str, loboptions: dict) -> str:
    """
    lobAsSecureFile(column, tablespace, cloboptions)

    Generates a LOB as SECUREFILE subscript and returns it
 
    Parameters:
        column: str
            The name of the LOB column
        tablespace: str
            Database tablespace assigned to LOB object (passed from table script)
        loboptions: dict
            Dictionary of various LOB options to deploy

    Returns:
        str
            Subscript string to be used in CREATE TABLE script
    """

    # Set defaults
    dedup = 'KEEP_DUPLICATES'
    compress = 'NOCOMPRESS'
    cache = 'NOCACHE'
    log = 'NOLOGGING'

    # Deduplication
    if loboptions["lob_deduplication"]:
        dedup = 'DEDUPLICATE'

    # Compression
    if loboptions["lob_compression"] in ['LOW', 'MEDIUM', 'HIGH']:
        compress = f'COMPRESS{tab}{loboptions["lob_compression"]}'
    
    # Caching
    if loboptions["lob_caching"]:
        cache = 'CACHE'
    
    # Logging
    if loboptions["lob_logging"]:
        log = 'LOGGING'
    
    # Populate script template and return it
    to_save = f'LOB ({column}) STORE AS SECUREFILE (\n' \
            f'{tab}TABLESPACE {tablespace}\n' \
            f'{tab}ENABLE{tab}STORAGE IN ROW\n' \
            f'{tab}CHUNK{tab}8192\n' \
            f'{tab}RETENTION\n' \
            f'{tab}{dedup}\n' \
            f'{tab}{compress}\n' \
            f'{tab}{cache}\n' \
            f'{tab}{log}\n' \
            f')'
    return to_save


def writeIndexScript(schema: str, tablename: str, tablespace: str, field: str, tableCount: int = 1,
                    count: int = 1, primaryKey: bool = True, unique: bool = True, 
                    outdirectory: str = outputDir):
    """
    writeIndexScript(schema, tablename, tablespace, field, count, primaryKey, unique, outDirectory)

    Generates a CREATE INDEX script for index defined and saves to output\INDEXES
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table
        tablespace: str
            Tablespace to apply to script
        field: str
            Name of field to be indexed
        tableCount: int
            Order of table in the list of tables to be processed
            Used for determining order to run output scripts in
        count: int
            Number of indexes generated for a given table. Used to append number to make name unique
        primaryKey: bool
            Flag for if field is a primary key (will also generate constraint)
        unique: bool
            Flag for if the field is supposed to be unique (will also generate constraint)
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    logger.info(f'Prepping {tablename} Index script')

    # Determining how to begin script
    if unique and primaryKey:
        create = 'CREATE UNIQUE INDEX'
        key_type = 'PK'
    elif unique:
        create = 'CREATE UNIQUE INDEX'
        key_type = 'UI'
    elif primaryKey:
        logger.warn('Primary Key must be unique. Script will reflect this.')
        create = 'CREATE UNIQUE INDEX'
        key_type = 'PK'
    else:
        create = 'CREATE INDEX'
        key_type = 'NI'
    
    # If count > 1, append number to generate unique name
    if count == 1:
        number = ''
    else:
        number = str(count)
    
    # Populate script template
    toWrite = f'prompt --Adding {schema}.{tablename}_{key_type}{number} index for {field}\n\n' \
                f'{create} {schema}.{tablename}_{key_type}{number} ON {schema}.{tablename}\n' \
                f'({field})\n' \
                f'TABLESPACE {tablespace};\n' \
                '/\n\n'

    # Write script to file in output/INDEXES. Will create directory if missing
    logger.info(f'Writing {tableCount:03}_{tablename}_{key_type}{number} to file')
    directory = f'{outdirectory}\\INDEXES\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}{tableCount:03}_{tablename}_{key_type}{number}.sql', 'w') as f:
        f.write(toWrite)
    
    # If a Primary Key index, generate corresponding constraint script
    if primaryKey:
        writePKConstraintScript(schema, tablename, f'{tablename}_{key_type}{number}', field, tableCount, outdirectory)
    
    # If not a Primary Key index but still needs to enforce uniqueness, generate corresponding constraint script
    elif unique:
        writeUniqueConstraintScript(schema, tablename, f'{tablename}_{key_type}{number}', field, tableCount, outdirectory)


def writePKConstraintScript(schema: str, tablename: str, index: str, field: str, tableCount: int = 1, 
                            outdirectory: str = outputDir):
    """
    writePKConstraintScript(schema, tablename, index, field, outDirectory)

    Generates an ALTER TABLE script for table defined to add constraint and saves to output\CONSTRAINTS
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to be have constraint applied
        index: str
            Name of the index. Will be used as CONSTRAINT name as well
        field: str
            Name of field to be constrained
        tableCount: int
            Order of table in the list of tables to be processed
            Used for determining order to run output scripts in
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    logger.info(f'Prepping {index} PK Constraint script')
    
    # Populate CONSTRAINT script template
    toWrite = f'prompt --Adding {schema}.{index} constraint for {field}\n\n' \
                f'ALTER TABLE {schema}.{tablename} ADD (\n' \
                f'{tab}CONSTRAINT {index}\n' \
                f'{tab}PRIMARY KEY\n' \
                f'{tab}({field})\n' \
                f'{tab}USING INDEX {schema}.{index}\n' \
                ');\n' \
                '/\n\n'

    # Write script to file in output/CONSTRAINTS. Will create directory if missing
    logger.info(f'Writing {tableCount:03}_1_{index} to file')
    directory = f'{outdirectory}\\CONSTRAINTS\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}{tableCount:03}_1_{index}.sql', 'w') as f:
        f.write(toWrite)


def writeUniqueConstraintScript(schema: str, tablename: str, index: str, field: str, tableCount: int = 1,
                                outdirectory: str = outputDir):
    """
    writeUniqueConstraintScript(schema, tablename, field, outDirectory)

    Generates an ALTER TABLE script for table defined to add constraint and saves to output\CONSTRAINTS
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to be have constraint applied
        index: str
            Name of the index. Will be used as CONSTRAINT name as well
        field: str
            Name of field to be constrained
        tableCount: int
            Order of table in the list of tables to be processed
            Used for determining order to run output scripts in
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    logger.info(f'Prepping {tablename}.{field} unique Constraint script')

    # Populate CONSTRAINT script template
    toWrite = f'prompt --Adding {index} unique constraint\n\n' \
                f'ALTER TABLE {schema}.{tablename} ADD (\n' \
                f'{tab}CONSTRAINT {index}\n' \
                f'{tab}UNIQUE\n' \
                f'{tab}({field})\n' \
                ');\n' \
                '/\n\n'

    # Write script to file in output/CONSTRAINTS. Will create directory if missing
    logger.info(f'Writing {tableCount:03}_2_{index} to file')
    directory = f'{outdirectory}\\CONSTRAINTS\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}{tableCount:03}_2_{index}.sql', 'w') as f:
        f.write(toWrite)


def writeSimpleCheckConstraintScript(schema: str, tablename: str, field: str, condition: str,
                                tableCount: int = 1, outdirectory: str = outputDir):
    """
    writeCheckConstraintScript(schema, tablename, field, condition, tableCount, outDirectory)

    Generates an ALTER TABLE script for table defined to add constraint and saves to output\CONSTRAINTS
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to be have constraint applied
        field: str
            Name of field to be constrained
        condition: str
            Condition for Check Constraint to be applied to field
        tableCount: int
            Order of table in the list of tables to be processed
            Used for determining order to run output scripts in
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    logger.info(f'Prepping {tablename}.{field} Check Constraint script')

    clean_field = field.strip('_')

    # Populate CONSTRAINT script template
    toWrite = f'prompt --Adding {tablename}.{field} Check constraint\n\n' \
                f'ALTER TABLE {schema}.{tablename} ADD (\n' \
                f'{tab}CONSTRAINT {tablename}_CHECK_{clean_field}\n' \
                f'{tab}CHECK\n' \
                f'{tab}({field} {condition})\n' \
                ');\n' \
                '/\n\n'

    # Write script to file in output/CONSTRAINTS. Will create directory if missing
    logger.info(f'Writing {tableCount:03}_{tablename}_CHECK_{clean_field} to file')
    directory = f'{outdirectory}\\CONSTRAINTS\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}{tableCount:03}_{tablename}_CHECK_{clean_field}.sql', 'w') as f:
        f.write(toWrite)


def writeCompoundIndexScript(schema: str, tablename: str, tablespace: str, fields: list, tableCount: int = 1, 
                            count: int = 1, primaryKey: bool = True, unique: bool = True, 
                            outdirectory: str = outputDir):
    """
    writeCompoundIndexScript(schema, tablename, tablespace, fields, count, primaryKey, unique, outDirectory)

    Generates a CREATE INDEX script for table defined to add compound index and saves to output\INDEXES
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to be have constraint applied
        tablespace: str
            Name of database tablespace to specify in script
        fields: list
            List of fields to be merged into compound index
        tableCount: int
            Order of table in the list of tables to be processed
            Used for determining order to run output scripts in
        count: int
            Count of indexes on a given table to give number for uniqueness
        primaryKey: bool
            Flag for if this index is for a compound PK
        unique: bool
            Flag for if compound index is unique
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    logger.info(f'Prepping {tablename} Index script')

    # Determining how to begin script
    if unique and primaryKey:
        create = 'CREATE UNIQUE INDEX'
        key_type = 'PK'
    elif unique:
        create = 'CREATE UNIQUE INDEX'
        key_type = 'UI'
    elif primaryKey:
        logger.warn('Primary Key must be unique. Script will reflect this.')
        create = 'CREATE UNIQUE INDEX'
        key_type = 'PK'
    else:
        create = 'CREATE INDEX'
        key_type = 'NI'
    
    # If count > 1, append number to generate unique name
    if count == 1:
        number = ''
    else:
        number = str(count)
    
    # Merge fields into one comma-delimited string to use in script template
    fieldmerge = fields[0]
    for i in range(1, len(fields)):
        fieldmerge = f'{fieldmerge}, {fields[i]}'
    
    # Populate Index script template
    toWrite = f'prompt --Adding {schema}.{tablename}_COMPOUND_{key_type}{number} index for {fieldmerge}\n\n' \
                f'{create} {schema}.{tablename}_COMPOUND_{key_type}{number} ON {schema}.{tablename}\n' \
                f'({fieldmerge})\n' \
                f'TABLESPACE {tablespace};\n' \
                '/\n\n'

    # Write script to file in output/INDEXES. Will create directory if missing
    logger.info(f'Writing {tableCount:03}_{tablename}_COMPOUND_{key_type}{number} to file')
    directory = f'{outdirectory}\\INDEXES\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}{tableCount:03}_{tablename}_COMPOUND_{key_type}{number}.sql', 'w') as f:
        f.write(toWrite)
    
    # If a Primary Key index, generate corresponding constraint script
    if primaryKey:
        writeCompoundPKConstraintScript(schema, tablename, f'{tablename}_COMPOUND_{key_type}{number}', fields, tableCount, outdirectory)
    
    # If not a Primary Key index but still needs to enforce uniqueness, generate corresponding constraint script
    elif unique:
        writeUniqueCompoundConstraintScript(schema, tablename, f'{tablename}_COMPOUND_{key_type}{number}', fields, tableCount, outdirectory)


def writeCompoundPKConstraintScript(schema: str, tablename: str, index: str, fields: list, tableCount: int = 1, 
                                    outdirectory: str = outputDir):
    """
    writeCompoundPKConstraintScript(schema, tablename, index, field, outDirectory)

    Generates an ALTER TABLE script for table defined to add compound constraint and saves to output\CONSTRAINTS
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to be have constraint applied
        index: str
            Name of the index. Will be used as CONSTRAINT name as well
        fields: list
            List of field names to include in compound constraint
        tableCount: int
            Order of table in the list of tables to be processed
            Used for determining order to run output scripts in
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    logger.info(f'Prepping {index} PK Constraint script')

    # Merge field names into comma-delimited string
    fieldmerge = fields[0]
    for i in range(1, len(fields)):
        fieldmerge = f'{fieldmerge}, {fields[i]}'

    # Populate CONSTRAINT script template
    toWrite = f'prompt --Adding {schema}.{index} constraint for {fieldmerge}\n\n' \
                f'ALTER TABLE {schema}.{tablename} ADD (\n' \
                f'{tab}CONSTRAINT {index}\n' \
                f'{tab}PRIMARY KEY\n' \
                f'{tab}({fieldmerge})\n' \
                f'{tab}USING INDEX {schema}.{index}\n' \
                ');\n' \
                '/\n\n'

    # Write script to file in output/CONSTRAINTS. Will create directory if missing
    logger.info(f'Writing {tableCount:03}_1_{index} to file')
    directory = f'{outdirectory}\\CONSTRAINTS\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}{tableCount:03}_1_{index}.sql', 'w') as f:
        f.write(toWrite)


def writeUniqueCompoundConstraintScript(schema: str, tablename: str, index: str, fields: list, tableCount: int = 1, 
                                        outdirectory: str = outputDir):
    """
    writeUniqueCompoundConstraintScript(schema, tablename, index, fields, outDirectory)

    Generates an ALTER TABLE script for table defined to add compound constraint and saves to output\CONSTRAINTS
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to be have constraint applied
        index: str
            Name of the index. Will be used as CONSTRAINT name as well
        fields: list
            List of field names to be included in compound constraint
        tableCount: int
            Order of table in the list of tables to be processed
            Used for determining order to run output scripts in
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    # Merge field names into comma-delimited string
    fieldmerge = fields[0]
    for i in range(1, len(fields)):
        fieldmerge = f'{fieldmerge}, {fields[i]}'
    logger.info(f'Prepping {index} unique Constraint script')

    # Populate CONSTRAINT script template
    toWrite = f'prompt --Adding {index} unique constraint\n\n' \
                f'ALTER TABLE {schema}.{tablename} ADD (\n' \
                f'{tab}CONSTRAINT {index}\n' \
                f'{tab}UNIQUE\n' \
                f'{tab}({fieldmerge})\n' \
                ');\n' \
                '/\n\n'

    # Write script to file in output/CONSTRAINTS. Will create directory if missing
    logger.info(f'Writing {tableCount:03}_2_{index} to file')
    directory = f'{outdirectory}\\CONSTRAINTS\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}{tableCount:03}_2_{index}.sql', 'w') as f:
        f.write(toWrite)


def writeFKConstraintScript(schema: str, sourcetable: str, sourcefield: str, boundtable: str, boundfield: str, 
                            tableCount: int = 1, count: int = 1, outdirectory: str = outputDir):
    """
    writeFKConstraintScript(schema, sourcetable, sourcefield, boundtable, boundfield, count, outDirectory)

    Generates an ALTER TABLE script for table defined to add Foreign Key constraint and saves to output\CONSTRAINTS
 
    Parameters:
        schema: str
            Database schema the table will reside in
        sourcetable: str
            Table name for the FK to link to
        sourcefield: str
            Field name for the FK to link to
        boundtable: str
            Table name for table with the constraint applied
        boundfield: str
            Field name for field with the constraint applied
        tableCount: int
            Order of table in the list of tables to be processed
            Used for determining order to run output scripts in
        count: int
            Number of FKs on table so far. Used to make FK name unique
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    # If count > 1, append number to generate unique name
    if count == 1:
        number = ''
    else:
        number = str(count)

    # Populate CONSTRAINT script template
    logger.info(f'Prepping {boundtable}_{boundfield}_FK{number} FK Constraint script')
    toWrite = f'prompt --Adding {schema}.{boundtable}_{boundfield}_FK{number} constraint for {sourcefield}\n\n' \
                f'ALTER TABLE {schema}.{boundtable} ADD (\n' \
                f'{tab}CONSTRAINT {boundtable}_{boundfield}_FK{number}\n' \
                f'{tab}FOREIGN KEY ({boundfield})\n' \
                f'{tab}REFERENCES {schema}.{sourcetable} ({sourcefield})\n' \
                ');\n' \
                '/\n\n'

    # Write script to file in output/CONSTRAINTS. Will create directory if missing
    logger.info(f'Writing {tableCount:03}_3_{boundtable}_{boundfield}_FK{number} to file')
    directory = f'{outdirectory}\\REF_CONSTRAINTS\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}{tableCount:03}_3_{boundtable}_{boundfield}_FK{number}.sql', 'w') as f:
        f.write(toWrite)


def writeSequenceScript(schema: str, tablename: str, field: str, startnum: int = 1, gentrigger: bool = True, 
                        outdirectory: str = outputDir):
    """
    writeSequenceScript(schema, tablename, field, startnum, gentrigger, outDirectory)

    Generates a CREATE SEQUENCE script and saves to output\SEQUENCES
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to be have constraint applied
        field: str
            Name of field to be constrained
        startnum: int
            Number to start the sequence on. Defaults to 1
        gentrigger: bool
            Flag to determine if trigger needs to also be generated. Defaults to True
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    # Populate SEQUENCE script template
    logger.info(f'Prepping {tablename}_{field}_SEQ Sequence script')
    toWrite = f'prompt --Adding {schema}.{tablename}_{field}_SEQ Sequence for {field}\n\n' \
                f'CREATE SEQUENCE {schema}.{tablename}_{field}_SEQ\n' \
                f'{tab}START WITH {startnum}\n' \
                f'{tab}MINVALUE 1\n' \
                f'{tab}NOMAXVALUE\n' \
                f'{tab}CACHE 20\n' \
                f'{tab}NOORDER\n' \
                ';\n' \
                '/\n\n'
    
    # Write script to file in output/SEQUENCES. Will create directory if missing
    logger.info(f'Writing {tablename}_{field}_SEQ to file')
    directory = f'{outdirectory}\\SEQUENCES\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}{tablename}_{field}_SEQ.sql', 'w') as f:
        f.write(toWrite)
    
    # If gentrigger == True, call writeTriggerScript()
    if gentrigger:
        writeTriggerScript(schema, tablename, field, f'{tablename}_{field}_SEQ', outdirectory)


def writeTriggerScript(schema: str, tablename: str, field: str, sequence: str, outdirectory: str = outputDir):
    """
    writeTriggerScript(schema, tablename, field, sequence, outDirectory)

    Generates a CREATE TRIGGER script for table defined to add sequence and saves to output\TRIGGERS
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to be have trigger applied
        field: str
            Name of field to be populated by trigger / sequence
        sequence: str
            Name of the sequence to use
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    # Populate TRIGGER script template
    logger.info(f'Prepping {tablename}_{field}_TRG Trigger script')
    toWrite = f'prompt --Adding {schema}.{tablename}_{field}_TRG Trigger for {field}\n\n' \
                f'CREATE OR REPLACE TRIGGER {schema}.{tablename}_{field}_TRG\n' \
                f'{tab}BEFORE INSERT\n' \
                f'{tab}ON {schema}.{tablename} REFERENCING NEW AS NEW OLD AS OLD\n' \
                f'{tab}FOR EACH ROW\n' \
                f'{tab}WHEN (new.{field} IS NULL)\n' \
                f'{tab}BEGIN\n' \
                f'{tab}:new.{field} := {sequence}.nextval;\n' \
                f'{tab}END {tablename}_{field}_TRG;\n' \
                '/\n\n' \
                f'show errors trigger {schema}.{tablename}_{field}_TRG'
    
    # Write script to file in output/TRIGGERS. Will create directory if missing
    logger.info(f'Writing {tablename}_{field}_TRG to file')
    directory = f'{outdirectory}\\TRIGGERS\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}{tablename}_{field}_TRG.sql', 'w') as f:
        f.write(toWrite)


def writeAuditTrigger(schema: str, tablename: str, outdirectory: str = outputDir):
    """
    writeAuditTrigger(schema, tablename, outDirectory)

    Generates a CREATE TRIGGER script for table defined to populate audit columns and saves to output\TRIGGERS
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to be have trigger applied
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    # Populate TRIGGER script template
    logger.info(f'Prepping {tablename}_BIU Trigger script')
    toWrite = f'prompt --Adding {schema}.{tablename}_BIU Trigger for audit\n\n' \
                f'CREATE OR REPLACE TRIGGER {schema}.{tablename}_BIU\n' \
                f'{tab}BEFORE INSERT OR UPDATE\n' \
                f'{tab}ON {schema}.{tablename} REFERENCING NEW AS NEW OLD AS OLD\n' \
                f'{tab}FOR EACH ROW\n' \
                f'{tab}BEGIN\n' \
                f'{tab}select sysdate, logging_utl.get_user_info into :new.u_date, :new.u_name from dual;\n' \
                f'{tab}END {tablename}_BIU;\n' \
                '/\n\n' \
                f'show errors trigger {schema}.{tablename}_BIU'
    
    # Write script to file in output/TRIGGERS. Will create directory if missing
    logger.info(f'Writing {tablename}_BIU to file')
    directory = f'{outdirectory}\\TRIGGERS\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}{tablename}_BIU.sql', 'w') as f:
        f.write(toWrite)


def writeHistoryTriggers(schema: str, tablename: str, columns: list, outdirectory: str = outputDir):
    """
    writeTriggerScript(schema, tablename, field, sequence, outDirectory)

    Generates 3 CREATE TRIGGER scripts for table defined to save INSERT, UPDATE, DELETE 
    changes to the history table and saves to output\TRIGGERS
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to be have trigger applied
        columns: list
            List of columns to include in Trigger
        outDirectory: str
            Output directory. Defaults to value from config file
    """

    # Make directory
    directory = f'{outdirectory}\\TRIGGERS\\'
    os.makedirs(directory, exist_ok = True)
    
    # If config option 'use-procedure' is True, generate the procedure to be used first
    if use_package:
        proc_name = saveHistoryProcedure(schema, tablename, columns)

    # For each of the trigger types (INSERT, UPDATE, DELETE), do stuff
    for key in triggerTypes.keys():
        count_split = 1

        # If config option 'use-procedure' is True, generate triggers with procedure calls
        if use_package:

            # Compile columns into a string with formatting
            spacing = ' ' * (table_min_spacing - 4)
            columnsFormatted = f'{tab}{tab}p_in_change =>{spacing}\'{key}\'\n'
            for col in columns:
                if len(col[0]) > table_min_spacing:
                    spacing = ' ' * 2
                else:
                    spacing = ' ' * (table_min_spacing - len(col[0]))
                columnsFormatted = f'{columnsFormatted}{tab}{tab}, p_in_{col[0].lower()} =>{spacing}:{triggerTypes[key]}.{col[0]}\n'
            columnsFormatted = columnsFormatted.rstrip(',\n')
            logger.debug(columnsFormatted)
            
            # Populate TRIGGER script template
            logger.info(f'Writing {tablename}_H_{key[0:3]}_TRG to file')
            toWrite = f'prompt --Adding {tablename}_H_{key[0:3]}_TRG Trigger for automated history\n\n' \
                    f'CREATE OR REPLACE EDITIONABLE TRIGGER {schema}.{tablename}_H_{key[0:3]}_TRG\n' \
                    f'AFTER {key}\n' \
                    f'ON {schema}.{tablename}\n' \
                    'REFERENCING NEW AS NEW OLD AS OLD\n' \
                    'FOR EACH ROW\n' \
                    'BEGIN\n' \
                    f'{tab}{proc_name}\n' \
                    f'{tab}(\n' \
                    f'{columnsFormatted}\n' \
                    f'{tab});\n' \
                    f'END {tablename}_H_{key[0:3]}_TRG;\n' \
                    '/\n' \
                    f'show errors trigger {schema}.{tablename}_H_{key[0:3]}_TRG'
            
            # Write script to file in output/TRIGGERS
            with open(f'{directory}{tablename}_H_{key[0:3]}_TRG.sql', 'w') as f:
                f.write(toWrite)
        
        # If config option 'use-procedure' is False, generate triggers with direct insert statements
        elif not use_package:

            # Compile columns and values into strings with formatting
            columnsFormatted = f'{tab}CHANGE'
            valuesFormatted = f'{tab}\'{key}\''
            for col in columns:
                newline = ''
                if len(valuesFormatted) >= (split_on * count_split):
                    newline = f'\n{tab}'
                    count_split += 1
                columnsFormatted = f'{columnsFormatted}{newline}, {col[0]}'
                valuesFormatted = f'{valuesFormatted}{newline}, :{triggerTypes[key]}.{col[0]}'
            logger.debug(columnsFormatted)
            logger.debug(valuesFormatted)

            # Populate TRIGGER script template
            logger.info(f'Writing {tablename}_H_{key[0:3]}_TRG to file')
            toWrite = f'prompt --Adding {tablename}_H_{key[0:3]}_TRG Trigger for automated history\n\n' \
                    f'CREATE OR REPLACE EDITIONABLE TRIGGER {schema}.{tablename}_H_{key[0:3]}_TRG\n' \
                    f'BEFORE {key}\n' \
                    f'ON {schema}.{tablename}\n' \
                    'REFERENCING NEW AS NEW OLD AS OLD\n' \
                    'FOR EACH ROW\n' \
                    'BEGIN\n' \
                    f'INSERT INTO {schema}.H_{tablename}\n' \
                    '(\n' \
                    f'{columnsFormatted}' \
                    ')\n' \
                    'VALUES\n' \
                    '(\n' \
                    f'{valuesFormatted}' \
                    ');\n' \
                    f'END {tablename}_H_{key[0:3]}_TRG;\n' \
                    '/\n' \
                    f'show errors trigger {schema}.{tablename}_H_{key[0:3]}_TRG'

            # Write script to file in output/TRIGGERS
            with open(f'{directory}{tablename}_H_{key[0:3]}_TRG.sql', 'w') as f:
                f.write(toWrite)


def saveHistoryProcedure(schema: str, tablename: str, columns: list) -> str:
    """
   saveHistoryProcedure(schema, tablename, columns)

    Generates a PROCEDURE to be included in a history package
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to be have procedure applied
        columns: list
            List of columns to be included in embedded INSERT statement

    Returns:
        str
            Returns generated procedure name
    """

    # Define procedure name and tablespace
    proc_name = f'P_H_{tablename}_WRITE'
    tablespace = schema.upper().split('_OWNER')[0]

    # If schema not yet in history_package_schema, create a new entry and apply the history package template
    if schema not in history_package_schema.keys():
        history_package_schema[schema] = copy.deepcopy(package_template)
    history = history_package_schema[schema]

    # Start of error message to include in EXCEPTION block
    log_error = f'{tab}{tab}{tab}message_out := \'Error inserting into history table {schema}.H_{tablename} - Error: \' || sqlerrm;\n'

    # If config option 'use-logging' is True, include additional lines for logging
    if use_sql_logging:
        log_error = f'{log_error}{tab}{tab}{tab}{tablespace}_LOGGER.LOGGING_UTL.LOG(message_out, \'{tablespace}_HISTORY.{proc_name}\');'
    
    # Compile columns into strings for params, values, and columns for insert with formatting
    spacing = ' ' * (table_min_spacing - 4)
    formatted_columns = f'{tab}{tab}CHANGE'
    formatted_params = f'{tab}{tab}p_in_change{spacing}IN{tab}{tab}VARCHAR2\n'
    formatted_values = f'{tab}{tab}p_in_change'
    count_split = 1
    for col in columns:
        type = col[1].split('(')[0]
        if len(col[0]) > table_min_spacing:
            spacing = ' ' * 2
        else:
            spacing = ' ' * (table_min_spacing - len(col[0]))
        newline = ''
        if len(formatted_values) >= (split_on * count_split):
            newline = f'\n{tab}{tab}'
            count_split += 1
        formatted_columns = f'{formatted_columns}{newline}, {col[0]}'
        formatted_params = f'{formatted_params}{tab}{tab}, p_in_{col[0].lower()}{spacing}IN{tab}{tab}{type}\n'
        formatted_values = f'{formatted_values}{newline}, p_in_{col[0].lower()}'

    # Populate HEADER template for PACKAGE
    to_save_header = f'{tab}PROCEDURE {proc_name}\n' \
                    f'{tab}(\n' \
                    f'{formatted_params}' \
                    f'{tab});'

    # Populate BODY template for PACKAGE
    to_save_body = f'{tab}PROCEDURE {proc_name}\n' \
                    f'{tab}(\n' \
                    f'{formatted_params}' \
                    f'{tab}) IS\n' \
                    f'{tab}{tab}message_out VARCHAR2(4000);\n' \
                    f'{tab}BEGIN\n' \
                    f'{tab}INSERT INTO {schema}.H_{tablename} (\n'\
                    f'{formatted_columns}\n' \
                    f'{tab}) VALUES (\n' \
                    f'{formatted_values}\n'\
                    f'{tab});\n' \
                    f'{tab}EXCEPTION\n' \
                    f'{tab}{tab}WHEN OTHERS THEN\n' \
                    f'{log_error}\n' \
                    f'{tab}{tab}{tab}raise_application_error (-20000, message_out);\n' \
                    f'{tab}END {proc_name};\n'
    
    # Add HEADER and BODY to history_package_schema to be written at end
    history['header'].append(to_save_header)
    history['body'].append(to_save_body)

    # Return procedure name so it can be used in Trigger call
    return f'{schema}.{tablespace}_HISTORY.{proc_name}'


def writeHistoryPackage(outdirectory: str = outputDir) -> list:
    """
    writeHistoryPackage(outDirectory)

    Compiles history PACKAGE and outputs to output\PACKAGE and output\PACKAGE_BODIES
 
    Parameters:
        outDirectory: str
            Output directory. Defaults to value from config file
    
    Returns:
        list: List of distinct schemas with packages written
    """

    schemas = []
    # Iterate over schemas in history_package_schema
    for schema in history_package_schema.keys():
        schemas.append(schema)
        # Get basic app account for adding to embedded GRANT statement
        app_account = schema.upper().split('_OWNER')[0]
        logger.info(f'Creating package scripts for {schema}.HISTORY')
        history = history_package_schema[schema]

        # If HEADER and BODY have different numbers of items, an error has occurred
        if len(history['header']) != len(history['body']):
            logger.error('Unable to generate package: Header and Body do not match')
        
        # If there are items in the history_package, continue
        elif len(history['header']) > 0:

            # Populate beginning of PACKAGE script
            to_write = f'prompt -- Adding {schema}.{app_account}_HISTORY package\n\n' \
                        f'CREATE OR REPLACE PACKAGE {schema}.{app_account}_HISTORY AS\n\n'
            
            # Append each procedure header to PACKAGE script
            for proc in history['header']:
                to_write = f'{to_write}{proc}\n\n'

            # Append ending to PACKAGE script
            to_write = f'{to_write}END {app_account}_HISTORY;\n/\n\n' \
                        f'show errors package {schema}.{app_account}_HISTORY'
            
            # Write script to file in output/PACKAGES. Will create directory if missing
            directory = f'{outdirectory}\\PACKAGES\\'
            logger.info(f'Writing {schema}-{app_account}_HISTORY_HEADER to file')
            os.makedirs(directory, exist_ok = True)
            with open(f'{directory}{app_account}_HISTORY_HEADER.sql', 'w') as f:
                f.write(to_write)

            # Populate beginning of PACKAGE BODY script
            to_write = f'prompt -- Adding {schema}.{app_account}_HISTORY package body\n\n' \
                        f'CREATE OR REPLACE PACKAGE BODY {schema}.{app_account}_HISTORY AS\n\n'

            # Append each procedure to PACKAGE script
            for proc in history['body']:
                to_write = f'{to_write}{proc}\n\n'

            # Append ending to PACKAGE BODY script
            to_write = f'{to_write}END {app_account}_HISTORY;\n/\n\n' \
                        f'show errors package {schema}.{app_account}_HISTORY'

            # Write script to file in output/PACKAGE_BODIES. Will create directory if missing
            directory = f'{outdirectory}\\PACKAGE_BODIES\\'
            logger.info(f'Writing {app_account}_HISTORY_BODY to file')
            os.makedirs(directory, exist_ok = True)
            with open(f'{directory}{app_account}_HISTORY_BODY.sql', 'w') as f:
                f.write(to_write)
    return schemas


def addToLoaderPackage(schema: str, tables: list):
    """
    addToLoaderPackage(schema, tables)

    Core method to call to generate Loader scripts. 
    Will start loader package details if first run for schema provided.
    If multiple tables present in 'tables', they should be parent, grandparent, etc. tables in order
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            List of tables listed, in order, from child [0] to highest level of parentage [n]
    """

    # Check if Schema has a record in loader_package_schema. If not, create using template
    if schema not in loader_package_schema.keys():
        loader_package_schema[schema] = copy.deepcopy(package_template)
    addInsertUpdateToLoaderPackage(schema, tables)
    if loader_package_deletes:
        addDeleteToLoaderPackage(schema, tables)


def addInsertUpdateToLoaderPackage(schema: str, tables: list):
    """
    addInsertUpdateToLoaderPackage(schema, tables)

    Generates a DDL statement for an Insert-or-Update statement for a given table. 
    If multiple tables present in 'tables', they should be parent, grandparent, etc. tables in order
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tables: list
            List of tables listed, in order, from child [0] to highest level of parentage [n]
    """
    
    loader = loader_package_schema[schema]
    tablename = tables[0].name
    columns = []
    hasMultiTable = (len(tables) > 1)
    parentLoadColumns = []
    parentLoadParams = ''
    tablenames = []

    # Load columns for all tables passed
    for table in tables:
        isParent = table.name != tablename
        # If parent table
        if isParent:
            tablenames.append(table.name.upper())
            parentLoadColumns = []
        for col in table.columns:
            if not col.virtual or col.primarykey:
                columns.append((col, table.name))
                if isParent:
                    parentLoadColumns.append(col)

    # Define procedure name and tablespace
    proc_name = f'P_LOAD_{tablename}'
    tablespace = schema.upper().split('_OWNER')[0]
    
    # Compile columns into strings for params, values, and columns for insert with formatting
    spacing = ' ' * (table_min_spacing - 4)
    formatted_params_all = ''
    formatted_insert_columns = f'{tab*3}'
    formatted_insert_values = f'{tab*3}'
    formatted_update_columns = ''
    formatted_update_where = ''
    count_split = 1
    firstCol = True
    firstTable = True
    fieldsUsed = []
    primaryKey = None
    firstPrimary = True
    for col in columns:
        name = col[0].field.upper()
        type = col[0].type.upper()

        # Note when you switch from the first table to any subsequent tables
        if firstTable and col[1] != tablename:
            firstTable = False

        # Has field been used yet (i.e. from another table).
        # If so, exclude from all parameters
        if name in fieldsUsed:
            excludeParams = True
        elif col[0].fksourcetable != None and col[0].fksourcetable.upper() in tablenames:
            excludeParams = True
        else:
            excludeParams = False
            fieldsUsed.append(name)
        
        # Set IN OUT Type. Determine if field is primarykey
        io = ('IN', 'in')
        excludeIU = False
        defaultNull = ''
        if firstPrimary and col[0].primarykey:
            io = ('IN OUT', 'io')
            primaryKey = col[0]
            excludeIU = True
            firstPrimary = False
        elif col[0].primarykey:
            io = ('IN OUT', 'io')
            excludeIU = True
            oldio = f'p_in_{name.lower()}'
            newio = f'p_io_{name.lower()}'
            formatted_insert_columns = formatted_insert_columns.replace(oldio, newio)
            formatted_insert_values = formatted_insert_values.replace(oldio, newio)
            formatted_update_columns = formatted_update_columns.replace(oldio, newio)
            formatted_update_where = formatted_update_where.replace(oldio, newio)
        else:
        # Add DEFAULT NULL to parameter for any nullable fields
            if not col[0].notnull or col[0].default != None:
                defaultNull = f'{tab}DEFAULT NULL'
            elif hasMultiTable and col[0].fksourcetable != None and col[0].fksourcetable.upper() in tablenames:
                defaultNull = f'{tab}DEFAULT NULL'
            elif col[0].triggered:
                defaultNull = f'{tab}DEFAULT NULL'
        
        # Format spacing 
        if len(name) > table_min_spacing:
            spacing = ' ' * 2
        else:
            spacing = ' ' * (table_min_spacing - len(name))
        
        # Determine if newline is needed based on length of current line
        newline = ''
        if len(formatted_insert_values) >= (split_on * count_split):
            newline = f'\n{tab*4}'
            count_split += 1

        # Wrap VARCHAR2's in substr(trim()) if char limit needs to be enforced
        if loader_package_charlimit and type == 'VARCHAR2':
            insert_value = f'substr(trim(p_{io[1]}_{name.lower()}), 1, {col[0].size})'
        else:
            insert_value = f'p_{io[1]}_{name.lower()}'
        
        if firstCol:
            formatted_params_all = f'{formatted_params_all}{tab*2}p_{io[1]}_{name.lower()}{spacing}{io[0]}{tab*2}{type}{defaultNull}\n'
            if not excludeIU:
                formatted_insert_columns = f'{formatted_insert_columns}{newline}{tab}{name}'
                formatted_insert_values = f'{formatted_insert_values}{newline}{tab}{insert_value}'
                formatted_update_columns = f'{formatted_update_columns}{tab*3}{name} = {insert_value}\n'
                formatted_update_where = f'{formatted_update_where}{newline}{insert_value}\n'
            firstCol = False
        elif firstTable:
            if not excludeParams:
                formatted_params_all = f'{formatted_params_all}{tab*2}, p_{io[1]}_{name.lower()}{spacing}{io[0]}{tab*2}{type}{defaultNull}\n'
            
            if not excludeIU and formatted_update_columns == '':
                formatted_insert_columns = f'{formatted_insert_columns}{newline}{tab}{name}'
                formatted_insert_values = f'{formatted_insert_values}{newline}{tab}{insert_value}'
                formatted_update_columns = f'{formatted_update_columns}{tab*4}{name} = {insert_value}\n'
                formatted_update_where = f'{formatted_update_where}{tab*4}(({name} IS NULL AND {insert_value} IS NOT NULL) ' \
                                                                        f'OR ({name} IS NOT NULL AND {insert_value} IS NULL) ' \
                                                                        f'OR ({name} <> {insert_value}))\n'
            elif not excludeIU:
                formatted_insert_columns = f'{formatted_insert_columns}{newline}, {name}'
                formatted_insert_values = f'{formatted_insert_values}{newline}, {insert_value}'
                formatted_update_columns = f'{formatted_update_columns}{tab*4}, {name} = {insert_value}\n'
                formatted_update_where = f'{formatted_update_where}{tab*4}OR (({name} IS NULL AND {insert_value} IS NOT NULL) ' \
                                                                        f'OR ({name} IS NOT NULL AND {insert_value} IS NULL) ' \
                                                                        f'OR ({name} <> {insert_value}))\n'
        else:
            if not excludeParams:
                formatted_params_all = f'{formatted_params_all}{tab*2}, p_{io[1]}_{name.lower()}{spacing}{io[0]}{tab*2}{type}{defaultNull}\n'
            if col[0] == parentLoadColumns[0]:
                parentLoadParams = f'{tab*3}p_{io[1]}_{name.lower()} => p_{io[1]}_{name.lower()}\n'
            else:
                parentLoadParams = f'{parentLoadParams}{tab*3}, p_{io[1]}_{name.lower()} => p_{io[1]}_{name.lower()}\n'
    
    paramsAdd = []
    paramsAdd.append(('p_out_return', 'OUT', 'NUMBER', ''))
    paramsAdd.append(('p_out_message', 'OUT', 'VARCHAR2', ''))
    paramsAdd.append(('p_out_rowcount', 'OUT', 'NUMBER', ''))
    paramsAdd.append(('p_in_docommit', 'IN', 'NUMBER', f'{tab}DEFAULT 0'))
    if loader_package_logging:
        paramsAdd.append(('p_in_clientid', 'IN', 'VARCHAR2', f'{tab}DEFAULT NULL'))

    for param in paramsAdd:
        if len(param[0]) > table_min_spacing:
            spacing = ' ' * 2
        else:
            spacing = ' ' * (table_min_spacing - len(param[0]))
        formatted_params_all = f'{formatted_params_all}{tab*2}, {param[0]}{spacing}{param[1]}{tab*2}{param[2]}{param[3]}\n'
        parentLoadParams = f'{parentLoadParams}{tab*3}, {param[0]} => {param[0]}\n'

    load_parents = ''
    if len(tablenames) > 0:
        load_parents = f'{tab*2}P_LOAD_{tablenames[0]} (\n' \
                        f'{parentLoadParams}' \
                        f'{tab*2});\n' \
                        f'{tab*2}if p_out_return = 0 then raise parent_failed; end if;\n' \
                        f'{tab*2}l_cnt := l_cnt + p_out_rowcount;\n'

    log_cdbg = ''
    log_appinfo = ''
    log_error = ''
    log_error_parent = ''
    log_debug = ''
    log_closeappinfo = ''
    # Start of error message to include in EXCEPTION block
    log_error = f'{tab*3}p_out_message := \'Error updating table {schema}.{tablename} - Error: \' || sqlerrm;\n' \
                f'{tab*3}l_stack := substr(dbms_utility.format_error_backtrace,1,4000);\n' \
                f'{tab*3}p_out_return := 0;\n'
    log_error_parent = f'{tab*3}p_out_message := \'Error updating parent table to {schema}.{tablename} - Error: \' || p_out_message;\n' \
                        f'{tab*3}p_out_return := 0;\n'

    # If config option 'use-logging' is True, include additional lines for logging
    if loader_package_logging:
        log_error = f'{log_error}{tab*3}{tablespace}_LOGGER.LOGGING_UTL.LOG(p_out_message, \'{tablespace}_LOADER.{proc_name}\');\n' \
                    f'{tab*3}{tablespace}_LOGGER.LOGGING_UTL.LOG(l_stack, \'{tablespace}_LOADER.{proc_name}\');\n'
        log_error_parent = f'{log_error_parent}{tab*3}{tablespace}_LOGGER.LOGGING_UTL.LOG(p_out_message, \'{tablespace}_LOADER.{proc_name}\');\n'
        log_cdbg = f'{tab}c_dbg constant number(1) := is_debug_(\'{tablespace}_LOADER\',\'LOAD_{tablename}\');\n'
        log_appinfo = f'{tab}if (trim(p_in_clientid) is not null) then do_app_info_(\'{tablespace}_LOADER\', \'LOAD_{tablename}\', p_in_clientid); end if;\n'
        log_debug = f'{tab}if c_dbg = 1 then {tablespace}_LOGGER.LOGGING_UTL.LOG(p_out_message, \'{tablespace}_LOADER.{proc_name}\'); end if;\n'
        log_closeappinfo = f'{tab}if (trim(p_in_clientid) is not null) then do_app_info_; end if;\n'

    # Populate HEADER template for PACKAGE
    to_save_header = f'{tab}PROCEDURE {proc_name}\n' \
                    f'{tab}(\n' \
                    f'{formatted_params_all}' \
                    f'{tab});'
    
    # Populate BODY template for PACKAGE
    to_save_body = f'-------------------------------------------------------------------------------\n' \
                    f'-- INSERT or UPDATE {tablename}\n' \
                    f'-------------------------------------------------------------------------------\n' \
                    f'{tab}PROCEDURE {proc_name}\n' \
                    f'{tab}(\n' \
                    f'{formatted_params_all}' \
                    f'{tab}) IS\n' \
                    f'{tab*2}l_stack VARCHAR2(4000);\n' \
                    f'{tab*2}l_cnt NUMBER;\n' \
                    f'{tab*2}parent_failed EXCEPTION;\n' \
                    f'{tab}{log_cdbg}' \
                    f'{tab}BEGIN\n' \
                    f'{tab}{log_appinfo}' \
                    f'{tab*2}l_cnt := 0;\n' \
                    f'{load_parents}' \
                    f'{tab*2}IF p_io_{primaryKey.field.lower()} IS NOT NULL THEN\n' \
                    f'{tab*3}UPDATE {tablename} SET\n' \
                    f'{formatted_update_columns}' \
                    f'{tab*3}WHERE {primaryKey.field} = p_io_{primaryKey.field.lower()}\n' \
                    f'{tab*3}AND (\n' \
                    f'{formatted_update_where}' \
                    f'{tab*3});\n' \
                    f'{tab*3}l_cnt := l_cnt + sql%rowcount;\n' \
                    f'{tab*3}p_out_message := \'UPDATE \' || l_cnt || \' rows; pk = \' || p_io_{primaryKey.field.lower()};\n' \
                    f'{tab*2}ELSE\n' \
                    f'{tab*3}INSERT INTO {schema}.{tablename} (\n'\
                    f'{formatted_insert_columns}\n' \
                    f'{tab*3}) VALUES (\n' \
                    f'{formatted_insert_values}\n'\
                    f'{tab*3}) RETURNING {primaryKey.field} INTO p_io_{primaryKey.field.lower()};\n' \
                    f'{tab*3}l_cnt := l_cnt + sql%rowcount;\n' \
                    f'{tab*3}p_out_message := \'INSERT \' || l_cnt || \' rows; pk = \' || p_io_{primaryKey.field.lower()};\n' \
                    f'{tab*2}END IF;\n' \
                    f'{tab*2}p_out_rowcount := l_cnt;\n' \
                    f'{tab*2}if p_in_docommit = 1 then COMMIT; end if;\n' \
                    f'{tab*2}p_out_return := 1;\n' \
                    f'{tab}{log_debug}' \
                    f'{tab}{log_closeappinfo}' \
                    f'{tab}EXCEPTION\n' \
                    f'{tab*2}WHEN parent_failed THEN\n' \
                    f'{log_error_parent}' \
                    f'{tab*3}if p_in_docommit = 1 then ROLLBACK; end if;\n' \
                    f'{tab*2}{log_closeappinfo}' \
                    f'{tab*2}WHEN OTHERS THEN\n' \
                    f'{log_error}' \
                    f'{tab*3}if p_in_docommit = 1 then ROLLBACK; end if;\n' \
                    f'{tab*2}{log_closeappinfo}' \
                    f'{tab}END {proc_name};\n' \
                    f'-------------------------------------------------------------------------------\n' \
    
    # Add HEADER and BODY to loader_package_schema to be written at end
    loader['header'].append(to_save_header)
    loader['body'].append(to_save_body)



def addDeleteToLoaderPackage(schema: str, tables: list):
    """
    addDeleteToLoaderPackage(schema, tables)

    Generates a DDL statement for an Delete statement for a given table. 
    If multiple tables present in 'tables', they should be parent, grandparent, etc. tables in order
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tables: list
            List of tables listed, in order, from child [0] to highest level of parentage [n]
    """
    
    loader = loader_package_schema[schema]
    tablename = tables[0].name
    columns = []
    parentLoadColumns = []
    parentLoadParams = ''
    tablenames = []

    # Load columns for all tables passed
    for table in tables:
        isParent = table.name != tablename
        # If parent table
        if isParent:
            tablenames.append(table.name.upper())
            parentLoadColumns = []
        for col in table.columns:
            if col.primarykey:
                columns.append((col, table.name))
                if isParent:
                    parentLoadColumns.append(col)

    # Define procedure name and tablespace
    proc_name = f'P_DELETE_{tablename}'
    tablespace = schema.upper().split('_OWNER')[0]
    
    # Compile columns into strings for params, values, and columns for insert with formatting
    spacing = ' ' * (table_min_spacing - 4)
    formatted_params_all = ''
    firstCol = True
    firstTable = True
    fieldsUsed = []
    primaryKey = None
    firstPrimary = True
    deleteWhere = ''
    for col in columns:
        name = col[0].field.upper()
        type = col[0].type.upper()

        # Note when you switch from the first table to any subsequent tables
        if firstTable and col[1] != tablename:
            firstTable = False

        # Has field been used yet (i.e. from another table).
        # If so, exclude from all parameters
        if name in fieldsUsed:
            excludeParams = True
        else:
            excludeParams = False
            fieldsUsed.append(name)
        
        if firstPrimary:
            primaryKey = col[0]
            firstPrimary = False
            deleteWhere = f'{deleteWhere}{name} = p_in_{name.lower()}\n'
        elif firstTable:
            deleteWhere = f'{deleteWhere}{tab*3}AND {name} = p_in_{name.lower()}\n'
        
        # Add DEFAULT NULL to parameter for any nullable fields
        defaultNull = ''
        if col[1] in tablenames:
            defaultNull = f'{tab}DEFAULT NULL'
        
        # Format spacing 
        if len(name) > table_min_spacing:
            spacing = ' ' * 2
        else:
            spacing = ' ' * (table_min_spacing - len(name))
        
        if firstCol:
            formatted_params_all = f'{formatted_params_all}{tab*2}p_in_{name.lower()}{spacing}IN{tab*2}{type}{defaultNull}\n'
            firstCol = False
        elif firstTable and not excludeParams:
            formatted_params_all = f'{formatted_params_all}{tab*2}, p_in_{name.lower()}{spacing}IN{tab*2}{type}{defaultNull}\n'
        else:
            if not excludeParams:
                formatted_params_all = f'{formatted_params_all}{tab*2}, p_in_{name.lower()}{spacing}IN{tab*2}{type}{defaultNull}\n'
            if col[0] == parentLoadColumns[0]:
                parentLoadParams = f'{tab*3}p_in_{name.lower()} => p_in_{name.lower()}\n'
            else:
                parentLoadParams = f'{parentLoadParams}{tab*3}, p_in_{name.lower()} => p_in_{name.lower()}\n'
    
    paramsAdd = []
    paramsAdd.append(('p_out_return', 'OUT', 'NUMBER', ''))
    paramsAdd.append(('p_out_message', 'OUT', 'VARCHAR2', ''))
    paramsAdd.append(('p_out_rowcount', 'OUT', 'NUMBER', ''))
    paramsAdd.append(('p_in_docommit', 'IN', 'NUMBER', f'{tab}DEFAULT 0'))
    if loader_package_logging:
        paramsAdd.append(('p_in_clientid', 'IN', 'VARCHAR2', f'{tab}DEFAULT NULL'))

    for param in paramsAdd:
        if len(param[0]) > table_min_spacing:
            spacing = ' ' * 2
        else:
            spacing = ' ' * (table_min_spacing - len(param[0]))
        formatted_params_all = f'{formatted_params_all}{tab*2}, {param[0]}{spacing}{param[1]}{tab*2}{param[2]}{param[3]}\n'
        parentLoadParams = f'{parentLoadParams}{tab*3}, {param[0]} => {param[0]}\n'

    load_parents = ''
    if len(tablenames) > 0:
        load_parents = f'{tab*2}P_DELETE_{tablenames[0]} (\n' \
                        f'{parentLoadParams}' \
                        f'{tab*2});\n' \
                        f'{tab*2}if p_out_return = 0 then raise parent_failed; end if;\n' \
                        f'{tab*2}l_cnt := l_cnt + p_out_rowcount;\n'

    log_cdbg = ''
    log_appinfo = ''
    log_error = ''
    log_error_parent = ''
    log_debug = ''
    log_closeappinfo = ''
    # Start of error message to include in EXCEPTION block
    log_error = f'{tab*3}p_out_message := \'Error updating table {schema}.{tablename} - Error: \' || sqlerrm;\n' \
                f'{tab*3}l_stack := substr(dbms_utility.format_error_backtrace,1,4000);\n' \
                f'{tab*3}p_out_return := 0;\n'
    log_error_parent = f'{tab*3}p_out_message := \'Error updating parent table to {schema}.{tablename} - Error: \' || p_out_message;\n' \
                        f'{tab*3}p_out_return := 0;\n'

    # If config option 'use-logging' is True, include additional lines for logging
    if loader_package_logging:
        log_error = f'{log_error}{tab*3}{tablespace}_LOGGER.LOGGING_UTL.LOG(p_out_message, \'{tablespace}_LOADER.{proc_name}\');\n' \
                    f'{tab*3}{tablespace}_LOGGER.LOGGING_UTL.LOG(l_stack, \'{tablespace}_LOADER.{proc_name}\');\n'
        log_error_parent = f'{log_error_parent}{tab*3}{tablespace}_LOGGER.LOGGING_UTL.LOG(p_out_message, \'{tablespace}_LOADER.{proc_name}\');\n'
        log_cdbg = f'{tab}c_dbg constant number(1) := is_debug_(\'{tablespace}_LOADER\',\'LOAD_{tablename}\');\n'
        log_appinfo = f'{tab}if (trim(p_in_clientid) is not null) then do_app_info_(\'{tablespace}_LOADER\', \'LOAD_{tablename}\', p_in_clientid); end if;\n'
        log_debug = f'{tab}if c_dbg = 1 then {tablespace}_LOGGER.LOGGING_UTL.LOG(p_out_message, \'{tablespace}_LOADER.{proc_name}\'); end if;\n'
        log_closeappinfo = f'{tab}if (trim(p_in_clientid) is not null) then do_app_info_; end if;\n'

    # Populate HEADER template for PACKAGE
    to_save_header = f'{tab}PROCEDURE {proc_name}\n' \
                    f'{tab}(\n' \
                    f'{formatted_params_all}' \
                    f'{tab});'

    # Populate BODY template for PACKAGE
    to_save_body = f'-------------------------------------------------------------------------------\n' \
                    f'-- DELETE FROM {tablename}\n' \
                    f'-------------------------------------------------------------------------------\n' \
                    f'{tab}PROCEDURE {proc_name}\n' \
                    f'{tab}(\n' \
                    f'{formatted_params_all}' \
                    f'{tab}) IS\n' \
                    f'{tab*2}l_stack VARCHAR2(4000);\n' \
                    f'{tab*2}l_cnt NUMBER;\n' \
                    f'{tab*2}parent_failed EXCEPTION;\n' \
                    f'{tab}{log_cdbg}' \
                    f'{tab}BEGIN\n' \
                    f'{tab}{log_appinfo}' \
                    f'{tab*2}l_cnt := 0;\n' \
                    f'{tab*2}DELETE FROM {tablename}\n' \
                    f'{tab*2}WHERE {deleteWhere}' \
                    f'{tab*2};\n' \
                    f'{tab*2}l_cnt := l_cnt + sql%rowcount;\n' \
                    f'{tab*2}p_out_message := \'DELETE \' || l_cnt || \' rows; pk = \' || p_in_{primaryKey.field.lower()};\n' \
                    f'{load_parents}' \
                    f'{tab*2}p_out_rowcount := l_cnt;\n' \
                    f'{tab*2}if p_in_docommit = 1 then COMMIT; end if;\n' \
                    f'{tab*2}p_out_return := 1;\n' \
                    f'{tab}{log_debug}' \
                    f'{tab}{log_closeappinfo}' \
                    f'{tab}EXCEPTION\n' \
                    f'{tab*2}WHEN parent_failed THEN\n' \
                    f'{log_error_parent}' \
                    f'{tab*3}if p_in_docommit = 1 then ROLLBACK; end if;\n' \
                    f'{tab*2}{log_closeappinfo}' \
                    f'{tab*2}WHEN OTHERS THEN\n' \
                    f'{log_error}' \
                    f'{tab*3}if p_in_docommit = 1 then ROLLBACK; end if;\n' \
                    f'{tab*2}{log_closeappinfo}' \
                    f'{tab}END {proc_name};\n' \
                    f'-------------------------------------------------------------------------------\n' \
    
    # Add HEADER and BODY to loader_package_schema to be written at end
    loader['header'].append(to_save_header)
    loader['body'].append(to_save_body)


def writeLoaderPackage(outdirectory: str = outputDir) -> list:
    """
    writeLoaderPackage(outDirectory)

    Compiles Loader PACKAGE and outputs to output\PACKAGE and output\PACKAGE_BODIES
 
    Parameters:
        outDirectory: str
            Output directory. Defaults to value from config file

    Returns:
        list: List of distinct schemas with packages written
    """
    
    schemas = []
    # Iterate over schemas in loader_package_schema
    for schema in loader_package_schema.keys():
        schemas.append(schema)
        # Get basic app account for adding to embedded GRANT statement
        app_account = schema.upper().split('_OWNER')[0]
        logger.info(f'Creating package scripts for {schema}.LOADER')
        loader = loader_package_schema[schema]
        
        # If HEADER and BODY have different numbers of items, an error has occurred
        if len(loader['header']) != len(loader['body']):
            logger.error('Unable to generate package: Header and Body do not match')
        
        
        # If there are items in the loader_package, continue
        elif len(loader['header']) > 0:

            # Populate beginning of PACKAGE script
            to_write = f'prompt -- Adding {schema}.{app_account}_LOADER package\n\n' \
                        f'CREATE OR REPLACE PACKAGE {schema}.{app_account}_LOADER AS\n\n'
            
            # Append each procedure header to PACKAGE script
            for proc in loader['header']:
                to_write = f'{to_write}{proc}\n\n'

            # Append ending to PACKAGE script
            to_write = f'{to_write}END {app_account}_LOADER;\n/\n\n' \
                        f'show errors package {schema}.{app_account}_LOADER'
            
            # Write script to file in output/PACKAGES. Will create directory if missing
            directory = f'{outdirectory}\\PACKAGES\\'
            logger.info(f'Writing {schema}-{app_account}_LOADER_HEADER to file')
            os.makedirs(directory, exist_ok = True)
            with open(f'{directory}{app_account}_LOADER_HEADER.sql', 'w') as f:
                f.write(to_write)

            # Populate beginning of PACKAGE BODY script
            to_write = f'prompt -- Adding {schema}.{app_account}_LOADER package body\n\n' \
                        f'CREATE OR REPLACE PACKAGE BODY {schema}.{app_account}_LOADER AS\n\n'
                        
            if loader_package_logging:
                to_write = f'{to_write}' \
                            f'-------------------------------------------------------------------------------\n' \
                            f'-- Local Functions / Procedures\n' \
                            f'-------------------------------------------------------------------------------\n' \
                            f'{tab}FUNCTION is_debug_ (\n' \
                            f'{tab*2}module_in IN VARCHAR2\n'\
                            f'{tab*2}, method_in IN VARCHAR2\n'\
                            f'{tab*2}, client_in IN VARCHAR2 DEFAULT NULL\n'\
                            f'{tab}) RETURN NUMBER IS\n' \
                            f'{tab}BEGIN\n' \
                            f'{tab*2}RETURN {app_account}_logger.logging_utl.is_debug(module_in => module_in, method_in => method_in, client_id_in => client_in );\n' \
                            f'{tab}END is_debug_;\n' \
                            f'-------------------------------------------------------------------------------\n\n' \
                            f'-------------------------------------------------------------------------------\n' \
                            f'{tab}PROCEDURE do_app_info_ (\n' \
                            f'{tab*2}module_in IN VARCHAR2 DEFAULT NULL\n'\
                            f'{tab*2}, method_in IN VARCHAR2 DEFAULT NULL\n'\
                            f'{tab*2}, client_in IN VARCHAR2 DEFAULT NULL\n'\
                            f'{tab}) IS\n' \
                            f'{tab}BEGIN\n' \
                            f'{tab*2}dbms_application_info.set_module(module_name => module_in, action_name => method_in);\n' \
                            f'{tab*2}dbms_application_info.set_client_info(client_info => client_in);\n' \
                            f'{tab}END do_app_info_;\n' \
                            f'-------------------------------------------------------------------------------\n\n'

            # Append each procedure to PACKAGE script
            for proc in loader['body']:
                to_write = f'{to_write}{proc}\n\n'

            # Append ending to PACKAGE BODY script
            to_write = f'{to_write}END {app_account}_LOADER;\n/\n\n' \
                        f'show errors package {schema}.{app_account}_LOADER'

            # Write script to file in output/PACKAGE_BODIES. Will create directory if missing
            directory = f'{outdirectory}\\PACKAGE_BODIES\\'
            logger.info(f'Writing {app_account}_LOADER_BODY to file')
            os.makedirs(directory, exist_ok = True)
            with open(f'{directory}{app_account}_LOADER_BODY.sql', 'w') as f:
                f.write(to_write)
    return schemas


def addColumnComment(schema: str, tablename: str, field: str, comment: str):
    """
    addColumnComment(schema, tablename, field, comment)

    Generates a COMMENT script and saves in global list to be written later
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table field is in
        field: str
            Name of field to have comment
        comment: str
            Comment to be applied to column
    """

    # Append comment script to comments list
    comments.append(f'COMMENT ON COLUMN {schema}.{tablename}.{field} IS \'{comment}\';\n')


def addTableComment(schema: str, tablename: str, comment: str):
    """
    addColumnComment(schema, tablename, comment)

    Generates a COMMENT script and saves in global list to be written later
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to have comment
        comment: str
            Comment to be applied to table
    """

    # Append comment script to comments list. Start table comments on a new line
    comments.append(f'\nCOMMENT ON TABLE {schema}.{tablename} IS \'{comment}\';\n')


def writeComments(outdirectory: str = outputDir):
    """
    writeComments(outDirectory)

    Writes global comments list to file
 
    Parameters:
        outDirectory: str
            Directory to write comments script to
    """

    # If there are comments
    if len(comments) > 0:
        logger.info(f'Writing COMMENTS to file')
        # Write comments to file in output/COMMENTS. Will create directory if missing
        directory = f'{outdirectory}\\COMMENTS\\'
        os.makedirs(directory, exist_ok = True)
        with open(f'{directory}COMMENTS.sql', 'w') as f:
            f.writelines(comments)
    
    # If no comments found, post warning in log
    else:
        logger.warn('No COMMENTS found to write')


def addGrant(schema: str, tablename: str, user: str, level: str):
    """
    addGrant(schema, tablename, user, level)

    Generates a GRANT script and saves in global list to be written later
 
    Parameters:
        schema: str
            Database schema the table will reside in
        tablename: str
            Name of the table to apply grant to
        user: str
            Name of user to be granted permissions
        level: str
            String indicating what level of access to grant
    """

    # All GRANTS will at least have SELECT
    grantedLevel = 'SELECT'
    if 'I' in level:
        grantedLevel = f'{grantedLevel}, INSERT'
    if 'U' in level:
        grantedLevel = f'{grantedLevel}, UPDATE'
    if 'D' in level:
        grantedLevel = f'{grantedLevel}, DELETE'

    # Append to global grants list to be written later
    grants.append(f'GRANT {grantedLevel} ON {schema}.{tablename} TO {user};\n')


def writeGrants(outdirectory = outputDir):
    """
    writeGrants(outDirectory)

    Writes global grants list to file
 
    Parameters:
        outDirectory: str
            Directory to write grants script to
    """

    # If there are grants to write
    if len(grants) > 0:
        logger.info(f'Writing GRANTS to file')
        # Write grants to file in output/GRANTS. Will create directory if missing
        directory = f'{outdirectory}\\GRANTS\\'
        os.makedirs(directory, exist_ok = True)
        with open(f'{directory}GRANTS.sql', 'w') as f:
            f.writelines(grants)
    
    # If no grants, post warning to log
    else:
        logger.warn('No GRANTS found to write')


def addGrantExec(schema: str, procname: str, user: str):
    """
    addGrant(schema, tablename, user)

    Generates a GRANT script and saves in global list to be written later
 
    Parameters:
        schema: str
            Database schema the table will reside in
        procname: str
            Name of the Procedure/Package/Function to apply grant to
        user: str
            Name of user to be granted permissions
    """

    # Append to global grants list to be written later
    grantsExec.append(f'GRANT EXECUTE ON {schema}.{procname} TO {user};\n')


def writeGrantsExec(outdirectory = outputDir):
    """
    writeGrants(outDirectory)

    Writes global grants list to file
 
    Parameters:
        outDirectory: str
            Directory to write grants script to
    """

    # If there are grants to write
    if len(grantsExec) > 0:
        logger.info(f'Writing Execute GRANTS to file')
        # Write grants to file in output/GRANTS. Will create directory if missing
        directory = f'{outdirectory}\\GRANTS\\'
        os.makedirs(directory, exist_ok = True)
        with open(f'{directory}GRANTS_EXECUTE.sql', 'w') as f:
            f.writelines(grantsExec)
    
    # If no grants, post warning to log
    else:
        logger.warn('No GRANTS found to write')