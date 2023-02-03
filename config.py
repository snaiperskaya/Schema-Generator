#!/usr/bin/env python

"""config.py: Module for creating and loading .json config file"""

__author__ = "Cody Putnam (csp05)"
__version__ = "23.02.03.0"

import json
import os

# Filename to use for configuration
config_file = 'conf.json'

class Config:
    """
    Class to hold configuration to be used across rest of application

    """

    def __init__(self, file = config_file):
        # Initialize values
        self.config = {
            'config-version': __version__,
            'files': {
                'output-directory': {
                    'desc': "Folder to write output scripts to. Default: '.\\output'",
                    'setting': '.\\output'
                },
                'schema-file-type': {
                    'desc': "File-type used for schema outline ('xlsx' OR 'csv'). Default: 'xlsx'",
                    'setting': 'xlsx'
                },
                'schema-file': {
                    'desc': "File for generating Schema and Grants (XLSX). Schema and Grants will go on separate workbook pages. Default: '.\\Schema.xlsx'",
                    'setting': '.\\Schema.xlsx'
                },
                'schema-file-csv': {
                    'desc': "File for generating schema (CSV). Default: '.\\Schema.csv'",
                    'setting': '.\\Schema.csv'
                },
                'grants-file-csv': {
                    'desc': "File for generating grants for a schema (CSV). Default: '.\\Grants.csv'",
                    'setting': '.\\Grants.csv'
                },
                'build-file': {
                    'desc': "File name to write build script to. Default: 'build.sql'",
                    'setting': 'build.sql'
                },
                'clean-file': {
                    'desc': "File name to write clean script to. Default: 'clean.sql'",
                    'setting': 'clean.sql'
                }
            },
            'history-tables': {
                'use-procedures': {
                    'desc': "Use procedures / package structure for history triggers. Default: false",
                    'setting': False
                },
                'use-logging': {
                    'desc': "Implement PL/SQL logging in triggers or procedures. Default: false",
                    'setting': False
                }
            },
            'sorting': {
                'columns-nullable': {
                    'desc': "If true, sort all 'NOT NULL' columns to the beginning of column list. Can improve performance in Oracle. Default: false",
                    'setting': False
                }
            },
            'loader-package': {
                'enable': {
                    'desc': "Use procedures / package structure for table loaders. Default: false",
                    'setting': False
                },
                'enforce-char-lengths': {
                    'desc': "Use substr() to trim strings to fit fields. Default: false",
                    'setting': False
                },
                'use-logging': {
                    'desc': "Implement PL/SQL logging in procedures. Default: false",
                    'setting': False
                },
                'include-delete': {
                    'desc': "Include procedures for processing deletes from loader tables. Default: false",
                    'setting': False
                }
            },
            'logging': {
                'level': {
                    'desc': "Python min logging level. Available options: DEBUG, INFO, WARN, ERROR. Default: 'INFO'",
                    'setting': 'INFO'
                }
            },
            'clean-script': {
                'desc': "Should a clean.sql file be generated for undoing a build script. Default: true",
                'setting': True
            },
            'formatting': {
                'split_on': {
                    'desc': "Approximate number of characters before splitting a list onto a new line. Default: 100",
                    'setting': 100
                },
                'table_min_spacing' : {
                    'desc': "Approximate character distance between beginning of line and 2nd element. Default: 30",
                    'setting': 30
                }
            },
            'lob-defaults': {
                'desc': "Default settings to use for LOB fields when no value defined in CSV. Overridden if CSV defines valid value",
                'deduplication': {
                    'desc': "Apply deduplication to LOB field to reduce overall size due to repeated values. Oracle default value is 'Y'",
                    'setting': 'Y'
                },
                'compression' : {
                    'desc': "Apply compression to LOB field to reduce size, at cost of CPU overhead - Available values: 'N', 'LOW', 'MEDIUM', 'HIGH' - Oracle default value is 'MEDIUM'",
                    'setting': 'MEDIUM'
                },
                'caching': {
                    'desc': "Cache values of LOBs on read/write to improve performance at cost of memory usage. Oracle default is 'N'",
                    'setting': 'N'
                },
                'logging' : {
                    'desc': "Apply logging for LOB fields. Oracle default is 'Y'",
                    'setting': 'Y'
                }
            }
        }
        if os.path.exists(file):
           
            # If file exists already, load that config into temp variable
            with open(file, 'r') as conf:
                loaded_config = json.load(conf)
            
            # If version is present in loaded config (will fail if 'config-version' is not present)
            try:
                
                # Compare loaded config version to default version. 
                # If default is different, use default and apply loaded over it
                # This preserves file-set settings while adding any new settings available
                if loaded_config['config-version'] != __version__:
                    self.config.update(loaded_config)
                
                # If version matches, use loaded config
                else:
                    self.config = loaded_config
            
            # If 'config-version' not found, assume old file and apply loaded over default
            except:
                self.config.update(loaded_config)
       
            # Reset version number before saving
            self.config['config-version'] = __version__

        # Re-save file with any new settings from default
        with open(file, 'w') as conf:
            json.dump(self.config, conf, indent = 4)