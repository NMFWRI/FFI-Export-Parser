import configparser
import os
import re
from sqlalchemy import create_engine, inspect, text
from base import *


def main():
    # Fill this in before running!!!!
    path = 'C:/Users/Corey/OneDrive/OneDrive - New Mexico Highlands University/FFI_FinalAdminExports_all'
    debug = False

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
        for filename in os.scandir(path):
            esc = False
            if filename.is_file() and '.xml' in filename.path:
                file = filename.path
                f_name = re.findall(r'\\([\w._ ]+.xml)', file)

                ffi_data = FFIFile(file)

                # now we just write all the XML data to the database
                with pg_engine.connect() as pg_con1:
                    # check for already being written
                    exist = ffi_data.exists_admin_export(pg_con1)

                if exist:
                    print('{} has already been parsed into the specified database.\n'.format(ffi_data.file))
                    continue

                else:
                    print('Tables for {} have not yet been created yet.\n'.format(ffi_data.file))
                    data = ffi_data.create_tables()
                    for table in data:
                        with pg_engine.connect() as pg_con2:
                            table.to_sql(pg_con2)
                            if (table_len := len(table.df)) > 0:
                                print('{} written to {} with {} lines of data from {}.\n'.format(table.name,
                                                                                                 pg_config['database'],
                                                                                                 table_len,
                                                                                                 f_name))
    else:
        debug_file = ''  # '16.12_UpperMoraCFRPWalkerFlats_adminexport_QC\'edSASKM_2019.xml'
        path = os.path.join(path, debug_file)
        file = path
        ffi_data = FFIFile(file)
        # ffi_data.tables_to_csv()
        data = ffi_data.create_tables()
        # print("doing debug things")


if __name__ == "__main__":
    main()
