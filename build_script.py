#!/usr/bin/env python

"""build_script.py: Module for build script generation"""

__author__ = "Cody Putnam (csp05)"
__version__ = "22.08.08.0"


import os
from schema_generator import logger, config

# Load config settings
outputDir = config['files']['output-directory']['setting']
buildfile = config['files']['build-file']['setting']

# All types of scripts to look for in the order they should be added to build script
write_order = [
                'SEQUENCES',
                'DATABASE_LINKS',
                'SYNONYMS', 
                'TABLES',
                'INDEXES',
                'CONSTRAINTS',
                'REF_CONSTRAINTS',
                'FUNCTIONS',
                'PACKAGES',
                'PROCEDURES',
                'TRIGGERS',
                'VIEWS',
                'PACKAGE_BODIES',
                'COMMENTS',
                'GRANTS', 
                'REF_DATA_LOAD',
                'DROPS'
                ]

# Tab == 4 spaces to ensure clean and consistent formatting in scripts
tab = '    '

def genBuildScript(filename: str = buildfile, outDirectory: str = outputDir):
    """
    genBuildScript(filename, outDirectory)

    Generates the build.sql (or other name as defined in config file) for all scripts found in output directory
 
    Parameters:
        filename: str
            File name to use for build script. Defaults to file name defined in config file
        outDirectory: str
            Output directory to use for build script. Defaults to directory name defined in config file
    """
    
    logger.info('Generating build.sql script')
    # Open file and write first line
    with open(f'{outDirectory}\\{filename}', 'w') as file:
        file.write('spo build.log\n\n-------------------------\n')
        
        # Iterate over items in write_order and check for a directory named for each
        for item in write_order:
            path = f'{outDirectory}\\{item}'
            
            # If directory exists, check for contents
            if os.path.exists(path):
                contents = os.scandir(path)
                
                # Start a count for the number of .sql files found
                countFile = 0
                for obj in contents:
                    # If object in directory is a file, see if the extension is .sql. Add to build script if so
                    # NOTE: Does not currently handle nested directories. 
                    #       Could potentially change this to a os.walk call for each directory to resolve
                    if os.path.isfile(obj) and os.path.splitext(obj)[-1].lower() == '.sql':
                        countFile += 1
                        fname = os.path.basename(obj)
                        file.write(f'@{item}/{fname}\n')
                
                # If no (valid) files found, write comment to build script
                if countFile == 0:
                    file.write(f'{tab}-- No {item} to add (No files or no usable content found)')
            
            # If directory not found, write comment to build script
            else:
                file.write(f'{tab}-- No {item} to add (No directory)')
            
            # Insert comment line between types for readability
            file.write('\n-------------------------\n')
        
        # Finalize file
        file.write('spo off')