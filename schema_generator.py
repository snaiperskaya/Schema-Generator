#!/usr/bin/env python

"""schema_generator.py: Main module to parse csv and generate and save SQL (Oracle) DDL Scripts"""

__author__ = "Cody Putnam (csp05)"
__version__ = "22.10.19.0"

import logging
import os
import csv
import time
import shutil
from concurrent.futures import ThreadPoolExecutor
from config import Config

threads = os.cpu_count() ^ 2
logger = logging
config = Config().config
# Update changes to CSV file in default_schema_row AND schema_headers
# Columns must be in file order
default_schema_row = { # Order of columns in file
                "schema": '',
                "table": '',
                "field": '',
                "type": '',
                "size": '',
                "units": '',
                "not_null": '',
                "primary_key": '',
                "default": '',
                "index": '',
                "sequence_start": '',
                "pop_by_trigger": '',
                "invisible": '',
                "virtual": '',
                "virtual_expr": '',
                "check_constraint": '',
                "lob_deduplication": '',
                "lob_compression": '',
                "lob_caching": '',
                "lob_logging": '',
                "fk_to_table": '',
                "fk_to_field": '',
                "gen_audit_columns": '',
                "gen_history_tables": '',
                "table_comment": '',
                "column_comment": ''
                }
schema_headers = 'Schema,' \
                'Table,' \
                'Field,' \
                'Type,' \
                'Size,' \
                'Units,' \
                'Not Null,' \
                'Primary Key,' \
                'Default,' \
                'Index,' \
                'Sequence Start,' \
                'Pop by Trigger,' \
                'Invisible,' \
                'Virtual,' \
                'Virtual Expression,' \
                'Simple Check Constraint,' \
                'LOB Deduplication,' \
                '"LOB Compression (LOW, MEDIUM, HIGH)",' \
                'LOB Caching,' \
                'LOB Logging,' \
                'FK to Table,' \
                'FK to Field,' \
                'Gen Audit Columns,' \
                'Gen History Table (Automated),' \
                'Table Comment,' \
                'Column Comment'

import sql_script as sql
import build_script as build
import clean_script as clean
from table import Table, Column

# Logging settings for overall project. Loads logging level to use from config file (config.py -> conf.json)
logger.basicConfig(level= logging.getLevelName(config['logging']['level']['setting']), 
                    filename='schema_generator.log', 
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )

# Load settings from config file (config.py -> conf.json)
outputDir = config['files']['output-directory']['setting']
csvfile = config['files']['schema-file']['setting']
grantsFile = config['files']['grants-file']['setting']

history_package = config['history-tables']['use-procedures']['setting']

doClean = config['clean-script']['setting']

# Dictionary to hold tables to process
tables = {}
tableCount: int


def convertToDict(csvrow: tuple, grantFile: bool) -> dict:
    """
    convertToDict(csvrow, grantFile)

    Takes line from formatted CSV file and converts it to a key-linked dictionary
    Allows for adding (/ removing) columns from CSV document in future and having one place to define new field to use
 
    Parameters:
        csvrow: tuple
            Unprocessed row from formatted CSV file, addressed by numeric index
        grantFile: bool
            If the file is of type Grants, set to True to process appropriately.
            Otherwise will be processed as Schema type file

    Returns:
        dict
            Dictionary containing fields parsed into keys from the supplied row out of the CSV
    """

    d = {}
    if grantFile:
        d['schema'] = csvrow[0].strip()
        d['table'] = csvrow[1].strip()
        d['user'] = csvrow[2].strip()
        d['insert'] = csvrow[3].strip()
        d['update'] = csvrow[4].strip()
        d['delete'] = csvrow[5].strip()
    
    # Process as Schema type file
    else:
        # Populate default_schema_row template in file order
        d = default_schema_row.copy()
        index = 0
        try:
            for key in d.keys():
                d[key] = csvrow[index].strip()
                index += 1
        except:
            logger.error(f'CSV File is missing columns. Columns expected: {schema_headers}')
            raise IndexError
    
    # Return dict row
    return d


def csvRead(filename: str, grantFile: bool = False) -> list:
    """
    csvRead(filename, grantFile)

    Reads a formatted CSV file and parses out the data. 
    Skips blank lines or header rows (so these can and *should* be left in the file)
 
    Parameters:
        filename: str
            Path string of file to be processed
        grantFile: bool, default = False
            If the file is of type Grants, set to True to process appropriately.
            Otherwise will be processed as Schema type file

    Returns:
        list
            List of dictionaries (one per row) in CSV file
    """

    todo = []
    
    # If file defined is not found, generate a new copy with the appropriate headers
    if not os.path.exists(filename):
        logger.info('Generating file...')
        with open(filename, 'w') as file:
            if grantFile:
                file.write(
                            'Schema,' \
                            'Table,' \
                            'User,' \
                            'Insert,' \
                            'Update,' \
                            'Delete'
                            )
            else:
                file.write(schema_headers)
    
    # If file is found, process rows
    else:
        with open(filename, newline='') as file:
            reader = csv.reader(file)
            logger.info('Appending rows to ToDo list')
            firstrow = True
            for row in reader:
                
                #skip header row or blank rows (determined if first cell (Schema) is blank)
                if not firstrow and row[0] not in ['Schema', '']:
    
                    # Convert row to dictionary to allow key-based lookup of values
                    rowdict = convertToDict(row, grantFile)
                    todo.append(rowdict)
                elif firstrow:
                    firstrow = False
    # If file not found and generated, 'todo' will be empty list and have nothing to process
    return todo


def generateHistoryTables(table: Table, tableNum: int):
    schematable = f'{table.schema}.H_{table.name}'
    histTable = table.genHistoryTable(tableNum)
    if histTable is not None:
        tables[schematable] = histTable


def processTable(table: Table):
    """
    processTable(table)

    Processes a given table and generates all the scripts for that table. 
    Also iterates over all columns and generates any column-level scripts

    Parameters:
        table: Table
            Table object to be processed
    """

    logger.info(f'Processing table {table.schema}.{table.name}...')
    
    # Process compound indexes and remove any invalid entries
    table.cleanCompoundIndex()

    # If Gen History Table is True, create history table object and process for scripts
    if table.needshistory:
        logger.info('History Table and Structure requested.')
        sql.writeHistoryTriggers(table.schema, table.name, table.genColumnList())
    
    # If Gen Audit Columns is True, inject audit columns into table
    if table.needsaudit:
        logger.info('Injecting audit columns...')
        table.genAuditColumns()
        sql.writeAuditTrigger(table.schema, table.name)

    # If table has a comment, add to comment queue (written at end)
    if table.comment != '':
        sql.addTableComment(table.schema, table.name, table.comment)
    
    # If table has multiple PKs defined, create compound key scripts
    if table.hasCompoundPK():
        logger.debug(f'Table {table.name} has a compound primary key')
        sql.writeCompoundIndexScript(table.schema, 
                                    table.name, 
                                    table.tablespace, 
                                    table.getPKFields(), 
                                    table.tableNumber)
    
    # If table has any compound indexes defined (after the cleanup), create scripts
    if table.hasCompoundIndex():
        logger.debug(f'Table {table.name} has one or more compound indexes')
        indexFields = table.getIndexFields()
        for key in indexFields.keys():
            sql.writeCompoundIndexScript(table.schema, 
                                        table.name, 
                                        table.tablespace, 
                                        indexFields[key], 
                                        table.tableNumber, 
                                        table.indexcount, 
                                        primaryKey = False, 
                                        unique = table.isCompoundIndexUnique(key))
            table.indexcount += 1

    # Write table script to file
    sql.writeTableScript(table.schema, table.name, table.genColumnList(), table.tablespace)

    # Process all columns individually (cannot be multi-threaded due to shared index and FK counts per table)
    for col in table.columns:
        processColumn(table, col)


def processColumn(table: Table, column: Column):
    """
    processColumn(table, column)

    Processes a given column and generates all the scripts for that column. 

    Parameters:
        table: Table
            Table object to be processed
        column: Column
            Column object to be processed
    """

    logger.debug(f'Writing scripts for {table.name}.{column.field}')
    # If table does not have compound PK and column is marked as PK, generate scripts for PK
    if column.primarykey and not table.hasCompoundPK():
        logger.debug(f'{table.name}.{column.field} is Primary Key')
        sql.writeIndexScript(table.schema, 
                            table.name, 
                            table.tablespace, 
                            column.field, 
                            table.tableNumber, 
                            primaryKey = column.primarykey)
    # If not PK, but marked as indexed, check if in compound index then process if not
    elif column.indexed: ### and not table.isFieldInCompoundIndex(column.field): ### Columns no longer marked as "indexed" unless it's an individual index
        logger.debug(f'{table.name}.{column.field} is Indexed and unique = {column.unique}')
        sql.writeIndexScript(table.schema, 
                            table.name, 
                            table.tablespace, 
                            column.field, 
                            table.tableNumber, 
                            table.indexcount, 
                            primaryKey = False,
                            unique = column.unique)
        table.indexcount += 1
    
    # If column needs a sequence, generate scripts 
        # If column needs a sequence, generate scripts 
    # If column needs a sequence, generate scripts 
    # Flag for if sequence needs to be populated by trigger and generate that script as well
    if column.sequenced:
        if column.sequencetouse == None:
            logger.debug(f'{table.name}.{column.field} has an assigned Sequence. Trigger-fired = {column.triggered}')
            sql.writeSequenceScript(table.schema, 
                                    table.name, 
                                    column.field, 
                                    column.sequencestart, 
                                    column.triggered)
        # If reusing sequence, nothing to do unless trigger population is requested
        elif column.triggered:
            logger.debug(f'{table.name}.{column.field} is re-using sequence {column.sequencetouse}. Trigger-fired = {column.triggered}')
            sql.writeTriggerScript(table.schema, table.name, column.field, column.sequencetouse)
    
    # If column has a foreign key source table defined, generate FK scripts
    # Only need to check for FK Table because field is implied due to column loading logic
    if column.fksourcetable != None:
        logger.debug(f'{table.name}.{column.field} has a foreign key relation to {column.fksourcetable}.{column.fksourcefield}')
        sql.writeFKConstraintScript(table.schema, 
                                    column.fksourcetable, 
                                    column.fksourcefield, 
                                    table.name, 
                                    column.field, 
                                    table.tableNumber, 
                                    table.fkcount)
        table.fkcount += 1
    
    # If column has a simple Check Constraint defined, generate the script for this
    if column.checkconstraint != None:
        logger.debug(f'{table.name}.{column.field} has a simple Check Constraint defined as "{column.field} {column.checkconstraint}"')
        sql.writeSimpleCheckConstraintScript(table.schema,
                                            table.name,
                                            column.field,
                                            column.checkconstraint,
                                            table.tableNumber)
    
    # If column has a comment, add to comment queue to be written at end
    if column.comment != None:
        logger.debug(f'{table.name}.{column.field} has a comment')
        sql.addColumnComment(table.schema, table.name, column.field, column.comment)


def addGrant(grant):
    level = 'S'
    if grant["insert"].upper() == 'X':
        level = f'{level}I'
    if grant["update"].upper() == 'X':
        level = f'{level}U'
    if grant["delete"].upper() == 'X':
        level = f'{level}D'
    logger.debug(f'Adding GRANT of {level} to {grant["user"]} on {grant["schema"]}.{grant["table"]}')
    sql.addGrant(grant["schema"], grant["table"], grant["user"], level)


def main():
    """
    main()
    
    Main application runtime
    """

    cleandir = False
    if os.path.exists(outputDir):
        # Try to delete output directory
        try: 
            shutil.rmtree(outputDir)
            os.makedirs(outputDir)
            cleandir = True
        # Log error and do not update cleandir
        except OSError as e: 
            logger.error(f'Error deleting output directory: {e.strerror}')
    # No output directory, create and continue
    else:
        os.makedirs(outputDir)
        cleandir = True
    # If successfully deleted and recreated the output directory, continue
    if cleandir: 
        # Read Schema CSV (grantFile = False default invoked)
        todo = csvRead(csvfile) 
        tableCount = 1
        for row in todo:
            schematable = f'{row["schema"]}.{row["table"]}'
            logger.debug(f'Loading {schematable}.{row["field"]}')
            # If schema.table already in table dict: add column
            if schematable in tables.keys():
                newcol = Column()
                newcol.load(row)
                tables[schematable].addColumn(newcol)
            # If schema.table not in table dict: add new Table, then add column
            else:
                logger.debug(f'New table: {schematable}')
                tables[schematable] = Table(row["schema"], 
                                            row["table"], 
                                            row["gen_audit_columns"], 
                                            row["gen_history_tables"], 
                                            row["table_comment"],
                                            tableCount)
                tableCount += 1
                newcol = Column()
                newcol.load(row)
                tables[schematable].addColumn(newcol)
        logger.info('All tables and fields loaded')
        holdTables = tables.copy()
        # Extract and create History Tables
        for key in holdTables.keys():
            generateHistoryTables(holdTables[key], tableCount)
            tableCount += 1
        # Process each table and generate scripts
        with ThreadPoolExecutor(threads) as pool:
            pool.map(processTable, (tables[key] for key in tables.keys()))

        # If 'use-procedure' setting is on, complete history package and write to file
        if history_package:
            logger.info('Saving history writing procedures to package')
            sql.writeHistoryPackage()

        # Write comments to file
        logger.info('Saving Comments for all tables and columns')
        sql.writeComments()

        # Read Grants CSV and process contents
        todoGrants = csvRead(grantsFile, True)
        with ThreadPoolExecutor(threads) as pool:
            pool.map(addGrant, todoGrants)
        logger.info('Saving Grants for all tables')
        sql.writeGrants()

        # Generate build.sql script in root of output
        logger.info('Writing final scripts to directory')
        build.genBuildScript()
        # If 'clean-script' setting is enabled, generate clean.sql script in root of output
        if doClean:
            clean.genCleanScript()


if __name__ == '__main__':
    logger.info('Starting application')
    starttime = time.perf_counter()
    main()
    endtime = time.perf_counter()
    close_message = f'All operations completed in {endtime - starttime:0.4f} seconds'
    logger.info(close_message)
    print(close_message)