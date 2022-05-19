import xml.etree.ElementTree as ET
import configparser
import os
import re
from sqlalchemy import create_engine, exc
from base import *


def main():
    path = 'C:/NMFWRI/Data/FFI test'

    # users need to create their own local config file (see README)
    config = configparser.ConfigParser()
    config.read('config.ini')

    postgres_config = config['POSTGRESQL']
    postgres_url = create_url(**postgres_config)
    postgres_engine = create_engine(postgres_url)

    # find all XML files in the path directory
    for filename in os.scandir(path):
        esc = False
        if filename.is_file() and '.xml' in filename.path:
            file = filename.path
            f_name = re.findall(r'\\([\w._ ]+.xml)', file)
            tree = ET.parse(file)
            root = tree.getroot()

            ffi_data = FFIFile(root)

            # now we just write all the XML data to the database
            with postgres_engine.connect() as postgres_conn:
                # check for already being written
                exist = ffi_data.exists_admin_export(postgres_conn)
                if exist:
                    print('{} v.{} has already been parsed into the specified database.\n'.format(ffi_data.project_name,
                                                                                                  ffi_data.ffi_version))
                    continue

                else:
                    print('Tables for {} v. {} have not yet been created yet.\n'.format(ffi_data.project_name,
                                                                                        ffi_data.ffi_version))
                    data = ffi_data.create_tables()
                    for table in data:
                        table.to_sql(postgres_conn)
                        print('{} written to {} with {} lines of data from {}.\n'.format(table.name,
                                                                                         postgres_config['database'],
                                                                                         len(table.df),
                                                                                         f_name))


if __name__ == "__main__":
    main()
