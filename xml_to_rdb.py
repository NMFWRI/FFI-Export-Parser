import configparser
import sys
import os
import re
import pandas as pd
from sqlalchemy import create_engine, inspect, exc
from ffi_reader.base import *
import logging

logging.basicConfig(level=logging.NOTSET)
LOG_FILE = os.path.join('log', 'parser.log')
if not os.path.isdir('log'):
    os.mkdir('log')

lager = logging.getLogger('ffi_parser')

handler = logging.FileHandler(LOG_FILE)
handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

lager.addHandler(handler)


def main():
    # Fill this in before running!!!!
    path = 'C:/Users/Corey/OneDrive/OneDrive - New Mexico Highlands University/FFI_FinalAdminExports_all'
    debug = True

    # users need to create their own local config file (see README)
    config = configparser.ConfigParser()
    config.read('config.ini')

    pg_config = config['postgresql']
    pg_url = create_url(**pg_config)
    pg_engine = create_engine(pg_url)

    # check whether this database has been setup yet
    # this ddl file contains a bunch of functions to allow tables to preserve their dependencies when they're being
    # rewritten on the fly (see the _add_new_columns function in the XMLFrame class)
    pg_insp = inspect(pg_engine)
    pg_tables = pg_insp.get_table_names()
    if 'deps_saved_ddl' not in pg_tables:

        with open('sql/ddl') as file:
            sql_deps = file.read()

        pg_engine.execute(text(sql_deps))

    if not debug:

        # find all XML files in the path directory
        try:

            xml_files = [f for f in os.scandir(path) if f.is_file() and '.xml' in f.path]

            try:
                dup_query = 'select file from file_info'
                with pg_engine.connect() as pg_con:
                    df = pd.read_sql(dup_query, pg_con)
                existing_files = list(df['file'])

                new_files = [file for file in xml_files if file.path not in existing_files]

                if len(new_files) == 0:
                    lager.info('Parser run - no new files found.')

            except exc.ProgrammingError:
                new_files = xml_files

            for export in new_files:
                file = export.path
                f_name = re.findall(r"\\([\w._ ']+.xml)", file)

                lager.info('Initializing XML parser for {}'.format(f_name))

                ffi_data = FFIFile(file)

                # now we just write all the XML data to the database
                with pg_engine.connect() as pg_con1:
                    # check for already being written
                    exist = ffi_data.exists_admin_export(pg_con1)

                if exist:
                    lager.info('Data from {} has already been parsed into the provided database.'.format(f_name))
                    # print('{} has already been parsed into the specified database.\n'.format(f_name))
                    continue

                else:
                    # print('Tables for {} have not yet been created yet.\n'.format(f_name))
                    lager.info('Creating tables for {}'.format(f_name))
                    data = ffi_data.get_tables()
                    for table in data:
                        with pg_engine.connect() as pg_con2:
                            table.to_sql(pg_con2)
                            if (table_len := len(table.df)) > 0:
                                # print('{} written to {}: {} lines of data.\n'.format(table.name,
                                #                                                      pg_config['database'],
                                #                                                      table_len))
                                lager.info('{} written to {}: {} lines of data.\n'.format(table.name,
                                                                                          pg_config['database'],
                                                                                          table_len))
                    lager.info('Finished parsing {}'.format(f_name))
        except Exception as e:
            lager.exception("An exception occurred")

    else:
        debug_file = 'RGWF_TNC_CPLA_RanchoLobo_adminexport_8.3.17.xml'
        file2 = 'RGWF_TNC_RanchoLobo_2016_adminexport_Rob_3.27.17.xml'
        f_path = os.path.join(path, debug_file)
        f_path2 = os.path.join(path, file2)

        ffi_data1 = FFIFile(f_path)
        ffi_data2 = FFIFile(f_path2)
        # ffi_data.tables_to_csv()
        # data = ffi_data.get_tables()
        print("doing debug things")


if __name__ == "__main__":
    main()
