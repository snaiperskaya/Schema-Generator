#!/usr/bin/env python

"""clean_script.py: Module for clean script generation"""

__author__ = "Cody Putnam (csp05)"
__version__ = "22.07.22.0"


import os
from schema_generator import logger, config

# Load config settings
outputDir = config['files']['output-directory']['setting']
cleanfile = config['files']['clean-file']['setting']

# All types of scripts to look for when cleaning in the order they should be added to clean script
clean_order = {
                'VIEWS': 'DROP VIEW',
                'PROCEDURES': 'DROP PROCEDURE',
                'PACKAGES': 'DROP PACKAGE',
                'PACKAGE_BODIES': 'DROP PACKAGE',
                'FUNCTIONS': 'DROP FUNCTION',
                #'TRIGGERS': 'DROP TRIGGER', # Will get dropped with table
                'REF_CONSTRAINTS': 'DROP CONSTRAINT',
                'CONSTRAINTS': 'DROP CONSTRAINT',
                'INDEXES': 'DROP INDEX',
                'TABLES': 'DROP TABLE',
                'SYNONYMS': 'DROP SYNONYM',
                'SEQUENCES': 'DROP SEQUENCE'
                }

# Keywords to skip when parsing CREATE scripts
script_starts = [
                    'CREATE',
                    'OR',
                    'REPLACE',
                    'EDITIONABLE',
                    'UNIQUE',
                    'BODY',
                    'FORCE'
                ]

# Keywords to skip when parsing ALTER scripts
alter_keywords = [
                    'ALTER',
                    'TABLE',
                    'ADD',
                    'CONSTRAINT'
                ]

# Possible script types to look for without relying on clean_order.keys()
script_types = [
                'SEQUENCE',
                'SYNONYM', 
                'TABLE',
                'INDEX',
                'CONSTRAINT',
                'TRIGGER',
                'FUNCTION',
                'PACKAGE',
                'PROCEDURE',
                'VIEW',
                ]

# Strip these characters from files with ALTER statements to make parsing easier / more reliable
strippable_chars = ['\t', '\n', '(', ')']

# Translator for type to directory
type_to_dir = {
                'SEQUENCE': 'SEQUENCES',
                'SYNONYM': 'SYNONYMS', 
                'TABLE': 'TABLES',
                'INDEX': 'INDEXES',
                'CONSTRAINT': 'CONSTRAINTS',
                'TRIGGER': 'TRIGGERS',
                'FUNCTION': 'FUNCTIONS',
                'PACKAGE': 'PACKAGES',
                'PROCEDURE': 'PROCEDURES',
                'VIEW': 'VIEWS',
            }

# Store packages to avoid duplicating them if also found in PACKAGE_BODIES directory
packages = []

# Tab == 4 spaces to ensure clean and consistent formatting in scripts
tab = '    '

def genCleanScript(filename = cleanfile, outDirectory = outputDir):
    """
    genCleanScript(filename, outDirectory)

    Generates the clean.sql (or other name as defined in config file) for all scripts found in output directory
 
    Parameters:
        filename: str
            File name to use for clean script. Defaults to file name defined in config file
        outDirectory: str
            Output directory to use for clean script. Defaults to directory name defined in config file
    """

    logger.info('Generating clean.sql script')
    # Open file and write first lines
    with open(f'{outDirectory}\\{filename}', 'w') as file:
        file.write('spo clean.log\n\nprompt --Dropping all objects in this release\n\n-------------------------\n')

        # Iterate over items in clean_order and check for a directory named for each
        for item in clean_order.keys():
            path = f'{outDirectory}\\{item}'

            # If directory exists, check for contents
            if os.path.exists(path):
                contents = os.scandir(path)

                # Start a count for the number of SQL scripts found (excludes empty or invalid files)
                countFile = 0
                for obj in contents:
                    # If object in directory is a file, see if the extension is .sql
                    # Process for usable scripts, if so
                    if os.path.isfile(obj) and os.path.splitext(obj)[-1].lower() == '.sql':
                        # Empty alter and create lists
                        alters = []
                        creates = []

                        # If the type is a CONSTRAINT, process for ALTER statements
                        if item in ('CONSTRAINTS', 'REF_CONSTRAINTS'):
                            alters = parseSqlAltersConstraint(obj)

                            # For each DROP statement returned, add to file
                            for drop in alters:
                                file.write(f'{drop}\n')
                        
                        # Parse out CREATE statements and return appropriate DROP statements
                        creates = parseSqlCreates(obj)
                        
                        # For each DROP returned, write to file the DROP statement + the object found
                        for drop in creates:
                            file.write(f'{drop}\n')
                        
                        # If any CREATEs OR ALTERs are found, increment count
                        if len(creates) > 0 or len(alters) > 0:
                            countFile += 1
                
                # If count is 0, write comment to clean script
                if countFile == 0:
                    file.write(f'{tab}-- No {item} to drop (No files or no usable content found)')
            
            # If no valid directory, write comment to clean script
            else:
                file.write(f'{tab}-- No {item} to drop (No directory)')
            
            # Insert comment line between types for readability
            file.write('\n-------------------------\n')
        
        # Finalize File
        file.write('spo off')


def parseSqlCreates(fileobj: os.DirEntry) -> list:
    """
    parseSqlCreates(fileobj)

    Parses contents of a file to extract any CREATE statements
 
    Parameters:
        fileObj: os.DirEntry
            File Object that can be passed to an open() statement
    
    Returns:
        list 
            List Object containing DROP statements for all objects found
    """

    # Initialize empty lists for output and holding
    outList = []
    createStatements = []

    # Temp variable to store last detected type (word in script_types)
    createType = ''

    # Open fileobj and read all contents
    with open(fileobj, 'r') as f:
        filecontent = f.read()
    
    # Run through formatForParsing() to eliminate whitespace, line breaks, tabs, etc.
    filecontent = formatForParsing(filecontent)

    # Initialize isCreate as False
    isCreate = False

    # Iterate over words in script
    for word in filecontent.split():

        # If word is CREATE, congrats, you found the beginning of a CREATE statement!
        if word == 'CREATE':
            isCreate = True
            logger.debug(f'Found CREATE statement. Parsing for details...')

        # If in a CREATE statement
        elif isCreate:
            # If you find a word not in script_starts or script_types, it's probably the object name
            # Save the object name and set isCreate to false to start looking for any additional CREATE statements
            if word not in script_starts and word not in script_types:
                createStatements.append((createType, word))
                isCreate = False
            
            # If word in script_types, save to include with object name to define what kind of DROP is needed
            elif word in script_types:
                createType = word

            # If you happen to reach this and didn't find a object name, you gots issues
            if word.endswith((';','/')):
                isCreate = False
    
    # If you have at least one CREATE object saved
    if len(createStatements) > 0:
        for i in createStatements:
            # This is stupid and kludgy :(
            typedir = type_to_dir[i[0]]

            # If type is PACKAGE, check to confirm you do not already have this object recorded
            # If not, add to list and record DROP statement for output
            if typedir in ('PACKAGES', 'PACKAGE_BODIES') and i[1] not in packages:
                packages.append(i[1])
                outList.append(f'{clean_order[typedir]} {i[1]};\n')
            
            # If not PACKAGE, record for output
            elif typedir not in ('PACKAGES', 'PACKAGE_BODIES'):
                outList.append(f'{clean_order[typedir]} {i[1]};')
    
    # Return list with any and all DROP statements
    return outList


def parseSqlAltersConstraint(fileobj: os.DirEntry) -> list:
    """
    parseSqlAltersConstraint(fileobj)

    Parses contents of a file to extract any ALTER statements.
    In particular, this function is parsing out CONSTRAINTS
 
    Parameters:
        fileObj: os.DirEntry
            File Object that can be passed to an open() statement
    
    Returns:
        list 
            List Object containing DROP statements for all objects found
    """

    # Initialize output list
    outList = []

    # Open fileobj and read contents
    with open(fileobj, 'r') as f:
        filecontent = f.read()
    
    # Run through formatForParsing() to eliminate whitespace, line breaks, tabs, etc.
    filecontent = formatForParsing(filecontent)
    logger.debug(filecontent)

    # Initialize dictionary, index, and several bools for tracking parsing
    alterStatements = {}
    index = 0
    isAlter = False
    isAdd = False
    constraint = False

    # Iterate over all words in formatted file contents
    for word in filecontent.split():

        # If word is ALTER, start parsing ALTER statement
        if word == 'ALTER':
            isAlter = True
            logger.debug('Found ALTER statement. Parsing for CONSTRAINT details...')

        # If within an ALTER statement, continue
        elif isAlter:

            # If word is ADD, note that this is an add statement (as opposed to drop, rename, etc.)
            if word == 'ADD':
                isAdd = True
                logger.debug('isAdd is True')

            # If word is CONTRAINT, note that this is a CONSTRAINT statement
            elif word == 'CONSTRAINT':
                constraint = True
                logger.debug('constraint is True')

            # If word is not in alter_keywords and index has not been established in dict yet:
            # Add index to dict and record word as TABLE name
            elif word not in alter_keywords and index not in alterStatements.keys():
                alterStatements[index] = {'table': word}

            # If word is not in alter_keywords and index HAS been established in dict:
            # Update index record and add CONSTRAINT name to subdict
            elif word not in alter_keywords and index in alterStatements.keys():
                alterStatements[index]['constraint'] = word

                # If ADD or CONSTRAINT not detected, script is not a valid ADD CONSTRAINT script.
                # Pop from dict and ignore
                if not isAdd or not constraint:
                    alterStatements.pop(index) #invalid constraint
                    logger.debug('Invalid CONSTRAINT statement, ignoring...')
                
                # Reset variables to continue looking for additional ALTER statements
                isAlter = False
                isAdd = False
                constraint = False
                index += 1
            
            # If you reach the end of a script without detecting CONSTRAINT name, scrap and restart
            if word.endswith((';','/')) and isAlter:
                if not isAdd or not constraint:
                    alterStatements.pop(index) #invalid constraint
                    logger.debug('Invalid CONSTRAINT statement, ignoring...')
                
                # Reset variables to continue search for ALTER statements
                isAlter = False
                isAdd = False
                constraint = False
                index += 1

    # Iterate over all alterStatements found
    for i in alterStatements.keys():
        tablename = alterStatements[i]['table']
        constraintname = alterStatements[i]['constraint']
        logger.debug(f'Adding DROP for CONSTRAINT {tablename}.{constraintname}')

        # Apply table name and constraint name to reverse ALTER statement template
        outString = f'ALTER TABLE {tablename}\n' \
                    f'{tab}DROP CONSTRAINT {constraintname};\n' \
                    '/'
        
        # Add DROP statement to output
        outList.append(outString)
    
    # Output list of DROP statements from anything found in file
    return outList


def formatForParsing(string: str) -> str:
    """
    formatForParsing(string)

    Takes input string, converts to upper case, replaces all characters from strippable_chars with spaces
    then removes all duplicated spaces.
 
    Parameters:
        string: str
            Any string of characters. Typically pulled from file
    
    Returns:
        str 
            Cleaned string with characters and extraneous spaces removed. Ready for .split()
    """

    # Upper case string
    string = string.upper()

    # For characters in strippable_chars, replace with space
    for char in strippable_chars:
        string = string.replace(char, ' ')
    
    # Initialize tracking variables
    prev_char = ' '
    new_string = ''

    # For character in string
    for i in string:
        
        # If character isn't a space or the previous character isn't a space
        # Add character to new string
        if i != prev_char or prev_char != ' ':
            new_string = f'{new_string}{i}'
        prev_char = i
    
    # Return new string. Squeaky clean!
    return new_string