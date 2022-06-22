from pandas import DataFrame, concat, isna, read_sql, options
from re import sub, findall, match
from datetime import date
from sqlalchemy import exc, MetaData, Table, text, sql
from numpy import nan
from hashlib import sha256
import xml.etree.ElementTree as ET

options.mode.chained_assignment = None


def create_url(**kwargs):
    """
    create a SQLAlchemy URL out of a config file parameters
    The config.ini file is excluded from the git repository for security purposes; you'll have to create your own for
    your own s

    """
    if kwargs['type'] == 'postgresql':
        conn_str = "{}://{}:{}@{}/{}".format(
            kwargs['driver'],
            kwargs['uid'],
            kwargs['pwd'],
            kwargs['host'],
            kwargs['database']
        )
    else:
        return ""
    return conn_str


def parse_camelcase(txt: str):
    """
    convert CamelCase to snake_case

    :param txt: the string in CamelCase to be converted
    :return: the string returned as snake_case
    """
    segments = []
    cur_word = ''
    prev = ''
    for idx, char in enumerate(txt):
        try:
            next_char = txt[idx+1]
        except IndexError:
            next_char = ''

        # find where the word changes from lower case to uppercase or vice-versa
        if (prev.isupper() and char.isupper() and next_char.islower()) or (prev.islower() and char.isupper()):
            segments.append(cur_word)
            cur_word = ''
            cur_word += char
        else:
            cur_word += char
        prev = char
    segments.append(cur_word)

    new_string = '_'.join(word.lower() for word in segments)
    return new_string


def normalize_string(string: str):
    """
    turns strings into the formatting that is standard for postgres

    :param string: string to be formatted
    :return: the formatted string
    """
    temp1 = string.replace(' ', '').replace('.', '').replace('-', '')
    temp2 = sub(r'\(\w+\)', '', temp1)
    snake_case = parse_camelcase(temp2)
    return snake_case


def to_datenum(datetime):
    """
    convert a date to a datetime value (number of seconds since Jan 1, 1900, I think) in the format that SQLServer uses.
    This is different than how other programs do it, but I wanted it to align with MSSQL, since that's what FFI does.

    :param datetime: datetime value to be converted to big int
    :return: the big int of the datetime value
    """

    date_parts = findall(r'(\d{4})-(\d{2})-(\d{2})', datetime)[0]  # regex to parse parts of datetime
    date_key = {'year': int(date_parts[0]), 'month': int(date_parts[1]), 'day': int(date_parts[2])}
    offset = 693595  # datetime int value of 1/1/1900

    this_date = date(date_key['year'], date_key['month'], date_key['day'])
    date_ord = this_date.toordinal()
    date_num = str(date_ord - offset)

    return date_num


def strip_namespace(string):
    """
    strips the namespace off a tag element of an XML file

    :param string: the string from which to remove the namespace string
    :return: another string. but with the namespace removed
    """

    new_string = sub(r'\{http://\w+\.\w{3}[\w/.\d]+\}', '', string, count=1)
    return new_string


class FFIFile:
    """
    this is a class that represents the entire XML file. It can be thought of as a collection of 'tables' represented by
    the element names that appear in the XML file.
    """

    def __init__(self, file):
        """
        parses a ElementTree root element and creates the FFIFile class
        """
        with open(file) as open_file:
            f = open_file.read()
            file_hash = sha256(f.encode())
            file_id = file_hash.hexdigest()

        self._id = file_id
        self._file = file
        self._tree = ET.parse(file)
        self._root = self._tree.getroot()
        self._namespace = findall(r'\{http://\w+\.\w{3}[\w/.\d]+\}', self._root.tag)[0].strip('{}')
        self.tables = list(set([strip_namespace(element.tag) for element in self._root]))
        self._data_map = {}
        self._parse_data()
        self.project_name = self._data_map['RegistrationUnit']['RegistrationUnit_Name'][0]
        self.ffi_version = self._data_map['Schema_Version']['Schema_Version'][0]

    def __getitem__(self, item):
        """
        I needed to create some way to index the FFIFile class, so this will pass the index to the data_map and return
        whatever that operation returns.
        """

        if item in self.tables:
            return self._data_map[item]
        else:
            raise KeyError('{} not in FFI XML file.'.format(item))

    def _parse_data(self):
        """
        Iterates through each element name that was produced in the __init__ operation. This is what actually populates
        the data_map element
        """

        for table in self.tables:
            all_data = self._root.findall(table, namespaces={'': self._namespace})
            dfs = [DataFrame({strip_namespace(attr.tag): [attr.text] for attr in data_set}) for data_set in all_data]
            df = concat(dfs)
            self._data_map[strip_namespace(table)] = df

    def exists_admin_export(self, conn):
        """
        This will use the conn element to check if the specific export has already been written to the database.
        This needs to be fixed, as admin units are proving to be ineffectual.

        :param conn: SQLAlchemy PostgreSQL connection object for the production database
        """

        query = """select file_id, ffi_version from file_info
                     where file_id = '{}' and ffi_version = '{}'""".format(self._id,
                                                                           self.ffi_version)
        try:
            exist = read_sql(query, conn)
            if len(exist) > 0:
                return True
            else:
                return False
        except exc.ProgrammingError:
            return False

    def _create_basic_tables(self):
        """
        This creates some "basic" tables that serve as building blocks for all of the other tables. I really don't like
        this, but it was the most elegant solution I could come up with.

        The basic tables include: Plots, Events, Monitoring Status, Sample Events, and Attribute Data. I do this
        because we need to compute identifiers and only want to run that operation once when the file is parsed

        :return table_dict: a dictionary of all the "basic tables" as XMLFrames (more on that later)
        """

        # we need to create the plot_id early on so we have the proper linking identifiers across relevant tables
        plot_table = self['MacroPlot'].merge(self['RegistrationUnit'],
                                             left_on='MacroPlot_RegistrationUnit_GUID',
                                             right_on='RegistrationUnit_GUID', how='left')
        plot_id = XMLFrame('plot', plot_table)

        # similar to plots, we create the event_id as a unique identifier and that needs to be linked across
        # several disparate tables.
        event_table = self['SampleEvent'].merge(plot_table, left_on='SampleEvent_Plot_GUID',
                                                right_on='MacroPlot_GUID', how='left')\
            .merge(self['MM_MonitoringStatus_SampleEvent'], left_on='SampleEvent_GUID', right_on='MM_SampleEvent_GUID',
                   how='left')\
            .merge(self['MonitoringStatus'], left_on='MM_MonitoringStatus_GUID', right_on='MonitoringStatus_GUID',
                   how='left')
        event_id = XMLFrame('sampling_event', event_table)

        # again, some computations need to be done early on, so we create the values here
        monitoring_table = self['MM_MonitoringStatus_SampleEvent'] \
            .merge(self['MonitoringStatus'], left_on='MM_MonitoringStatus_GUID',
                   right_on='MonitoringStatus_GUID', how='left') \
            .merge(self['SampleEvent'], left_on='MM_SampleEvent_GUID', right_on='SampleEvent_GUID',
                   how='left') \
            .merge(self['MacroPlot'], left_on='SampleEvent_Plot_GUID', right_on='MacroPlot_GUID',
                   how='left')
        monitoring_status = XMLFrame('monitoring_status', monitoring_table)

        # this is how we link events, plots, and the weird columnar attribute name/values
        sample_events = event_table \
            .merge(self['MM_ProjectUnit_MacroPlot'], left_on='MacroPlot_GUID',
                   right_on='MM_MacroPlot_GUID', how='left') \
            .merge(monitoring_table, left_on='MM_MonitoringStatus_GUID', right_on='MonitoringStatus_GUID', how='left') \
            .merge(self['ProjectUnit'], left_on='MM_ProjectUnit_GUID', right_on='ProjectUnit_GUID', how='left')
        sample_events_xml = XMLFrame('sample_events', sample_events)

        # this is the linking info for the "methods" data
        attr_data = self['MethodAttribute'] \
            .merge(self['AttributeData'], left_on='MethodAtt_ID', right_on='AttributeData_MethodAtt_ID',
                   how='left') \
            .merge(self['Method'], left_on='MethodAtt_Method_GUID', right_on='Method_GUID', how='left') \
            .merge(self['LU_DataType'], left_on='MethodAtt_DataType_GUID', right_on='LU_DataType_GUID', how='left')
        attr_data_xml = XMLFrame('attr_data', attr_data)

        table_dict = {'plot_id': plot_id, 'event_id': event_id, 'monitoring_status': monitoring_status,
                      'sample_events': sample_events_xml, 'attr_data': attr_data_xml}

        return table_dict

    def create_tables(self):
        """
        The meat of it.

        I wish there was a more elegant way to do this. Unfortunately, there's a few tables that are used for multiple
        other tables that makes the queries cumbersome if some sort of temp structure isn't used. So I'm just
        going to create them all at once.

        This is just a bunch of specific tables with their specific relationships to each other explicitly defined.
        """
        basic_tables = self._create_basic_tables()
        plot_id = basic_tables['plot_id']
        event_id = basic_tables['event_id']
        monitoring_status = basic_tables['monitoring_status']
        sample_events = basic_tables['sample_events']
        attr_data = basic_tables['attr_data']

        # the names with which these tables will be written
        table_list = ['file_info', 'admin_unit', 'sampling_event', 'monitoring_status', 'project',
                      'species', 'plot', 'project_plot', 'event_detail', 'method_data']
        # table_dict = {}
        frames = []

        # TODO: EVENTUALLY!!! Come up with a neater way of creating these tables.

        for table in table_list:

            if table == 'file_info':
                file_dict = {'file_id': [self._id],
                             'admin_units': [self.project_name],
                             'ffi_version': [self.ffi_version]}
                file_df = DataFrame(file_dict)
                file_info = XMLFrame(table, file_df)

                final = file_info

            elif table == 'admin_unit':
                reg_table = self['RegistrationUnit']
                schema_table = self['Schema_Version']
                schema_version = schema_table['Schema_Version']

                reg_cols = {'RegistrationUnit_Name': 'admin_unit',
                            'RegistrationUnit_Comment': 'details'}
                admin_temp = XMLFrame(table, reg_table)
                admin_unit = admin_temp[reg_cols]
                admin_unit['unit_identifier'] = ''
                admin_unit['ffi_version'] = schema_version

                final = admin_unit

            elif table == 'sampling_event':
                event_cols = {'EventID': 'event_id',
                              'PlotID': 'plot_id',
                              'SampleEvent_Date': 'event_date',
                              'SampleEvent_Who': 'personnel',
                              'SampleEvent_Comment': 'note',
                              'monitoring_status': 'monitoring_status'}
                events = event_id[event_cols]
                events.drop_duplicates()
                final = events

            elif table == 'monitoring_status':
                mon_cols = ['status_prefix', 'monitoring_type', 'time_frame', 'monitoring_status']
                mon_status = monitoring_status[mon_cols]
                mon_status.drop_duplicates()
                final = mon_status

            elif table == 'project':
                project_table = self['ProjectUnit']

                proj_cols = {'ProjectUnit_Name': 'project_unit',
                             'ProjectUnit_DateIn': 'date_created',
                             'ProjectUnit_Description': 'details',
                             'ProjectUnit_Objective': 'treatment_goals',
                             'ProjectUnit_Agency': 'project_agency',
                             'ProjectUnit_Area': 'area',
                             'ProjectUnit_AreaUnits': 'area_units'}
                proj_temp = XMLFrame(table, project_table)
                project_unit = proj_temp[proj_cols]
                project_unit['ffi_version'] = schema_version

                final = project_unit

            elif table == 'admin_project':
                ap_table = self['ProjectUnit'].merge(self['RegistrationUnit'],
                                                     left_on='ProjectUnit_RegistrationUnitGUID',
                                                     right_on='RegistrationUnit_GUID', how='left')
                ap_cols = {'ProjectUnit_Name': 'project_unit',
                           'RegistrationUnit_Name': 'admin_unit'}
                ap_temp = XMLFrame(table, ap_table)
                admin_project = ap_temp[ap_cols]

                final = admin_project

            elif table == 'species':
                species_table = self['MasterSpecies']

                spec_cols = {'MasterSpecies_Symbol': 'symbol',
                             'MasterSpecies_ScientificName': 'scientific_name',
                             'MasterSpecies_CommonName': 'common_name',
                             'MasterSpecies_ITIS_TSN': 'itis_tsn',
                             'MasterSpecies_Genus': 'genus',
                             'MasterSpecies_Family': 'family',
                             'MasterSpecies_Nativity': 'nativity',
                             'MasterSpecies_Lifecycle': 'lifecycle'}
                spec_temp = XMLFrame(table, species_table)
                species = spec_temp[spec_cols]

                final = species

            elif table == 'plot':
                plot_cols = {'PlotID': 'plot_id',
                             'MacroPlot_Name': 'plot_name',
                             'RegistrationUnit_Name': 'admin_unit',
                             'MacroPlot_Purpose': 'purpose',
                             'MacroPlot_Type': 'plot_type',
                             'MacroPlot_DD_Lat': 'lat',
                             'MacroPlot_DD_Long': 'long',
                             'MacroPlot_DateIn': 'date_created',
                             'MacroPlot_Elevation': 'elevation',
                             'MacroPlot_ElevationUnits': 'elevation_units',
                             'MacroPlot_Azimuth': 'azimuth',
                             'MacroPlot_Aspect': 'aspect',
                             'MacroPlot_SlopeHill': 'hill_slope',
                             'MacroPlot_SlopeTransect': 'slope_transect',
                             'MacroPlot_Comment': 'comment',
                             'MacroPlot_Metadata': 'metadata'}
                plots = plot_id[plot_cols]

                final = plots

            elif table == 'project_plot':
                pup_table = self['MM_ProjectUnit_MacroPlot'] \
                    .merge(self['ProjectUnit'], left_on='MM_ProjectUnit_GUID', right_on='ProjectUnit_GUID',
                           how='left') \
                    .merge(plot_id.to_df(), left_on='MM_MacroPlot_GUID', right_on='MacroPlot_GUID', how='left') \
                    .merge(self['RegistrationUnit'], left_on='MacroPlot_RegistrationUnit_GUID',
                           right_on='RegistrationUnit_GUID', how='left')

                pup_cols = {'PlotID': 'plot_id',
                            'ProjectUnit_Name': 'project_name'}
                pup_temp = XMLFrame(table, pup_table)
                project_plot = pup_temp[pup_cols]

                final = project_plot

            elif table == 'event_detail':
                event_data_temp = self['SampleData'] \
                    .merge(sample_events.to_df(), left_on='SampleData_SampleEvent_GUID', right_on='SampleEvent_GUID_y', how='left') \
                    .merge(self['SampleAttribute'], left_on='SampleData_SampleAtt_ID', right_on='SampleAtt_ID',
                           how='left') \
                    .merge(self['Method'], left_on='SampleAtt_Method_ID', right_on='Method_ID', how='left') \
                    .merge(self['LU_DataType'], left_on='SampleAtt_DataType_GUID', right_on='LU_DataType_GUID',
                           how='left')

                ed_cols = {'EventID': 'event_id',
                           'FieldName': 'field_name',
                           'DataValue': 'data_value',
                           'LU_DataType_Name': 'data_type'}
                ed_idx = ['event_id']
                event_data_test = XMLFrame(table, event_data_temp)
                event_data = event_data_test[ed_cols]
                event_data.drop_duplicate_fields(ed_idx)
                event_data.pivot_data(ed_idx)

                final = event_data

            elif table == 'method_data':
                method_data_temp = attr_data.to_df() \
                    .merge(self['SampleData'], left_on='AttributeData_SampleRow_ID',
                           right_on='SampleData_SampleRow_ID', how='left') \
                    .merge(self['LocalSpecies'], left_on='AttributeData_Value', right_on='LocalSpecies_GUID',
                           how='left') \
                    .merge(sample_events.to_df(), left_on='SampleData_SampleEvent_GUID', right_on='SampleEvent_GUID_x',
                           how='left')

                md_cols = {'AttributeData_DataRow_ID': 'data_row_id',
                           'EventID': 'event_id',
                           'Method_Name': 'method',
                           'FieldName': 'field_name',
                           'DataValue': 'data_value',
                           'LU_DataType_Name': 'data_type'}
                md_idx = ['event_id', 'data_row_id']
                method_data_test = XMLFrame(table, method_data_temp)
                method_data = method_data_test[md_cols]
                method_data.drop_duplicate_fields(md_idx)
                method_data.pivot_data(md_idx)

                final = method_data

            else:
                raise EnvironmentError

            if final.pivot is not None:
                for frame in final.pivot:
                    frames.append(frame)
            else:
                frames.append(final)
            print("Processed {} table.".format(table))

        return frames


class XMLFrame:
    """
    Basically a modified DataFrame-style class that represents a table in the XML file.
    I built this out such that you can use the class almost exactly as you would a pandas DataFrame
    """

    def __init__(self, table_name, data, method_type=None, skip_id=False):
        """
        can either pass a DataFrame or dictionary directly to the class

        :param table_name: the name of the XML table
        :param data: the data to take the place of the DataFrame
        :param method_type: this is used for the method data table to keep track of how to name the tables
        :param skip_id: whether an id column is created or not
        """

        self.name = table_name

        if isinstance(data, DataFrame):
            self.df = data
        else:
            self.df = DataFrame(data)

        self.columns = self.df.columns
        self.pivot = None  # this just stores a list of XMLFrames that have been transposed
        self.method_type = method_type

        if not skip_id:
            try:
                self._create_ids()
            except KeyError:
                pass

            try:
                self._create_monitoring_status()
            except ValueError:
                pass

            # the next two blocks normalize attribute names and values for when they're transposed and become columns
            try:
                self._process_attr_name()
            except ValueError:
                pass

            try:
                self._process_attr_value()
            except ValueError:
                pass

    def __getitem__(self, cols):
        """
        allows us to directly index the underlying DataFrame like we would with using pandas, but with some special
        functionality:
        if a dict is passed, the dict will be treated like a column mapping function. That is, the key is the old column
        name to be selected and the value is what that column will get renamed to.
        if a list is passed, just the columns in that list will be selected.
        if any column in either format is encountered that doesn't currently exist in the DataFrame, an empty column
        with that name will be created in the frame.
        """

        if is_dict := isinstance(cols, dict):  # WALRUS
            new_cols = list(cols.keys())
        elif isinstance(cols, list):
            new_cols = cols
        else:
            raise ValueError('{} is not a list or dict.'.format(cols))

        try:
            new_df = self.df[new_cols]
        except KeyError:
            non_cols = [col for col in new_cols if col not in self.df.keys()]
            for col in non_cols:
                self.df[col] = None
            new_df = self.df[new_cols]

        if is_dict:
            temp = new_df.rename(cols, axis=1)
            new_df = temp

        new_frame = XMLFrame(self.name, new_df, skip_id=True)
        return new_frame

    def __setitem__(self, col, value):
        """
        this is meant to be used the same way you would set a value on an entire column in pandas
        """
        if isinstance(col, str):
            self.df[col] = value

    def _create_ids(self):
        """
        creates an id column - attempts to determine which id column will get created based on column names.
        """
        def id_str(row, col_list):
            """
            intended for use as a lambda function with the underlying DataFrame
            """
            if len(col_list) != 3:
                return ''
            else:
                # takes plot name, the datetimenumber, and the first 5 letters of the admin_unit and concatenates them
                vals = [row[col] for col in col_list]
                norm_plot = ''.join(findall(r'\w+', vals[1]))
                norm_date = to_datenum(vals[0])
                norm_admin = vals[2][:5].upper()
                item_id = '-'.join([norm_admin, norm_plot, norm_date])

                return item_id

        if self.name == 'sampling_event':
            cols = ['SampleEvent_Date', 'MacroPlot_Name', 'RegistrationUnit_Name']
            self.df['EventID'] = self.df.apply(lambda row: id_str(row, cols), axis=1)
        elif self.name == 'plot':
            cols = ['MacroPlot_DateIn', 'MacroPlot_Name', 'RegistrationUnit_Name']
            self.df['PlotID'] = self.df.apply(lambda row: id_str(row, cols), axis=1)
        else:
            raise KeyError

        # self.df.apply(lambda row: id_str(row, cols), axis=1)

    def _create_monitoring_status(self):
        """
        divides the current monitoring status up into a few different columns and normalizes all of the values.
        Hopefully this is fairly self-explanatory.
        """

        def prefix_str(row):
            cols = row.index
            if 'MonitoringStatus_Prefix' in cols:
                prefix = str(row['MonitoringStatus_Prefix']).lower()
            else:
                prefix = ''
            if 'MonitoringStatus_Base' in cols:
                base = str(row['MonitoringStatus_Base']).lower()
            else:
                base = ''
            if 'MonitoringStatus_Suffix' in cols:
                suffix = str(row['MonitoringStatus_Suffix']).lower()
            else:
                suffix = ''
            if 'SampleEvent_DefaultMonitoringStatus' in cols:
                default = str(row['SampleEvent_DefaultMonitoringStatus']).lower()
            else:
                default = ''

            if 'post' in prefix or 'post' in base or 'post' in suffix or 'post' in default:
                return 'Post'
            elif 'pre' in prefix or 'pre' in base or 'pre' in suffix or 'pre' in default:
                return 'Pre'
            else:
                return ''

        def base_str(row):
            cols = row.index
            if 'MonitoringStatus_Prefix' in cols:
                prefix = str(row['MonitoringStatus_Prefix']).lower()
            else:
                prefix = ''
            if 'MonitoringStatus_Base' in cols:
                base = str(row['MonitoringStatus_Base']).lower()
            else:
                base = ''
            if 'MonitoringStatus_Suffix' in cols:
                suffix = str(row['MonitoringStatus_Suffix']).lower()
            else:
                suffix = ''
            if 'SampleEvent_DefaultMonitoringStatus' in cols:
                default = str(row['SampleEvent_DefaultMonitoringStatus']).lower()
            else:
                default = ''

            if 'treatment' in base or 'treatment' in suffix or 'treatment' in prefix or 'treatment' or default:
                return 'Treatment'
            elif 'measure' in base or 'measure' in suffix or 'measure' in prefix or 'measure' in default:
                return 'Measure'
            elif 'burn' in base or 'burn' in suffix or 'burn' in prefix or 'burn' in default:
                return 'Burn'
            else:
                return ''

        def time_str(row):
            cols = row.index
            if 'MonitoringStatus_Suffix' in cols:
                suffix = row['MonitoringStatus_Suffix']
            else:
                suffix = nan
            if 'MonitoringStatus_Prefix' in cols:
                prefix = row['MonitoringStatus_Prefix']
            else:
                prefix = nan
            if 'MonitoringStatus_Base' in cols:
                base = str(row['MonitoringStatus_Base']).lower()
            else:
                base = nan
            if 'SampleEvent_DefaultMonitoringStatus' in cols:
                default = str(row['SampleEvent_DefaultMonitoringStatus']).lower()
            else:
                default = nan

            re_str = False
            if not isna(default):
                re_str = findall(r'(\d+)', default)
            if not isna(prefix):
                if re_str and len(re_str) == 0:
                    re_str = findall(r'(\d+)', prefix)
            if not isna(suffix):
                if re_str and len(re_str) == 0:
                    re_str = findall(r'(\d+)', suffix)
            elif not isna(base):
                if re_str and len(re_str) == 0:
                    re_str = findall(r'(\d+)', base)
            else:
                return ''

            if not re_str:
                return ''
            elif re_str and len(re_str) == 0:
                return ''
            else:
                num = re_str[0]
                if len(num) <= 2:
                    return '{}year'.format(re_str[0])
                else:
                    return ''

        if ('MonitoringStatus_Suffix' in self.columns or
            'MonitoringStatus_Prefix' in self.columns or
            'MonitoringStatus_Base' in self.columns or
            'SampleEvent_DefaultMonitoringStatus' in self.columns) and \
                ('status_prefix' not in self.columns and
                 'monitoring_type' not in self.columns and
                 'time_frame' not in self.columns and
                 'monitoring_status' not in self.columns):

            self.df['status_prefix'] = self.df.apply(prefix_str, axis=1)
            self.df['monitoring_type'] = self.df.apply(base_str, axis=1)
            self.df['time_frame'] = self.df.apply(time_str, axis=1)
            self.df['monitoring_status'] = self.df.apply(lambda row: '{}{}{}'.format(row['time_frame'],
                                                                                     row['status_prefix'],
                                                                                     row['monitoring_type']), axis=1)
            self.columns = list(self.df.columns)
            if self.name == 'monitoring_status':
                self.df.drop_duplicates(inplace=True)

        else:
            raise ValueError

    def _process_attr_name(self):
        """
        cleans up attribute names. e.g. adds specifiers for attributes that are shared across multiple methods
        """
        def attr_name(row):
            if self.name == 'event_detail':
                field_name = row['SampleAtt_FieldName']
                method_name = row['Method_Name']
                tree_method = match(r'Trees - .*', method_name)

                if field_name == 'MacroPlotSize' and tree_method:
                    method = findall(r'Trees - (\w+)', method_name)[0]
                    if method == 'Individuals':
                        method = 'Trees'
                    attr = '{}_{}'.format(field_name, method[0])

                elif field_name in ['FieldTeam', 'EntryTeam']:
                    clean_method = method_name.replace(' ', '').replace('-', '')
                    if '(' in clean_method:
                        method = findall(r'([\w -_]+)\([\w ]+\)', clean_method)[0]
                    else:
                        method = clean_method
                    attr = '{}_{}'.format(field_name, method)

                else:
                    attr = field_name

            elif self.name == 'method_data':
                method_attr = row['MethodAtt_FieldName']
                if method_attr == 'Comment':
                    attr = 'note'
                else:
                    attr = method_attr

            else:
                raise ValueError

            return attr

        self.df['FieldName'] = self.df.apply(attr_name, axis=1)
        self.columns = list(self.df.columns)

    def _process_attr_value(self):
        """
        this is almost exclusively for making sure that the Species symbol is correctly populated
        """
        def attr_val(row):
            if self.name == 'event_detail':
                row_val = row['SampleData_Value']
                val = str(row_val)
            elif self.name == 'method_data':
                row_val = row['AttributeData_Value']
                species = row['LocalSpecies_Symbol']
                if not isna(species):
                    val = species
                else:
                    val = str(row_val)
            else:
                raise ValueError
            return val

        self.df['DataValue'] = self.df.apply(attr_val, axis=1)
        self.columns = list(self.df.columns)

    def _clean_col_names(self):
        """
        normalizes all column names (turns them into snake_case)
        """
        temp = self.df.copy()
        clean_cols = (normalize_string(col) for col in temp.columns)
        temp.columns = clean_cols

        self.df = temp
        self.columns = list(temp.columns)

    def _cast_frame(self, type_df):
        """
        uses the data_type column from the FFI data to cast each column appropriately
        """

        type_mapping = {
            'Float': 'float64',
            'Long': 'int64',
            'Boolean': 'bool',
            'Date/Time': 'datetime64',
            'Text': 'str',
            'Index': 'int64',
            'Species': 'str',
            'Memo': 'str',
            'GUID': 'str'
        }

        if self.name == 'method_data' or self.method_type:
            exclude = ['event_id', 'data_row_id']  # these columns are going to get dropped anyway
        else:
            exclude = []
        try:
            df = self.df
            types = type_df[['field_name', 'data_type']]
            field_list = list(types['field_name'])
            type_list = list(types['data_type'])
            type_dict = dict(zip(field_list, type_list))
            columns = list(df.columns)
            if len(exclude) > 0:
                for x in exclude:
                    columns.remove(x)

            for column in columns:
                column_type = type_dict[column]
                df_type = type_mapping[column_type]
                if df_type in ['int64']:
                    row = df[column].fillna(0)
                    type_row = row.astype(df_type)
                    df[column] = type_row
                elif df_type in ['str']:
                    row = df[column].fillna('')
                    type_row = row.astype(df_type)
                    df[column] = type_row
                else:
                    row = df[column].astype(df_type)
                    df[column] = row

            self.df = df

        except KeyError:
            raise KeyError('{} frame is the wrong format to be cast.'.format(self.name))

    def _filter_exists(self, conn):
        """
        checks if specific data has already been written to the databases. This is specifically for species list and
        monitoring status - we don't want to add a bunch of duplicates, so we're just adding values that haven't yet
        been added.

        :param conn: connection to a Postgres database
        """
        if self.name in ['monitoring_status', 'species']:
            if self.name == 'monitoring_status':
                query = """select distinct monitoring_status from monitoring_status"""
                check_col = 'monitoring_status'
            else:
                query = """select distinct symbol from species"""
                check_col = 'symbol'
            try:
                check_df = read_sql(query, conn)
                check_list = list(check_df[check_col])
                df = self.df.loc[~self.df[check_col].isin(check_list)]
            except exc.ProgrammingError:
                df = self.df
            except exc.InteralError:
                df = self.df

            self.df = df

    def _add_new_columns(self, conn, schema='public'):
        """
        this is a little complex, but this is in case other versions of FFI have columns that aren't yet in the data.
        The current table gets copied, a new table gets created with the columns attached, then the old table gets
        copied back to the new one with the new columns being blank.
        """
        with conn.begin() as transaction:
            table_name = self.name
            df = self.df

            # get some info about the current table in the database
            md = MetaData()
            table = Table(table_name, md, autoload=True, autoload_with=conn)
            cols_list = [column.key for column in table.columns]

            # these next two blocks copy the old table to a temp backup table
            conn.execute(
                f"select deps_save_and_drop_dependencies('{schema}', '{table}')"
            )
            conn.execute(
                text(
                    "alter table {} rename to {}".format(sql.quoted_name(table_name, quote=False),
                                                         sql.quoted_name(table_name + "_backup", quote=False))
                )
            )

            # ensure ALL columns (including ones from the old table not in the new table) get added to the DataFrame
            old_cols = [col for col in cols_list if col not in df.columns]
            for col in old_cols:
                df[col] = None

            # write the new dataframe to the database with all columns
            df.to_sql(
                table_name,
                con=conn,
                if_exists="fail",
                index=False,
                chunksize=70,
                method="multi",
            )

            # copy the old data back in
            conn.execute(
                text(
                    "insert into "
                    + sql.quoted_name(table_name, quote=False)
                    + f" ({','.join(cols_list)}) "
                    + " select "
                    + f" {','.join(cols_list)} "
                    + "from "
                    + sql.quoted_name(table_name + "_backup", quote=False)
                )
            )

            # remove backup
            conn.execute(
                text(
                    "drop table {}_backup".format(sql.quoted_name(table_name, quote=False))
                )
            )
            conn.execute(f"select deps_restore_dependencies('{schema}', '{table}')")  # ensures dependencies remain
            transaction.commit()

    def drop_duplicates(self, keep='first', inplace=True):
        """
        replication of pandas DataFrame functionality
        """
        self.df.drop_duplicates(keep=keep, inplace=inplace)

    def drop_duplicate_fields(self, *args):
        """
        because of the strange FFI data format, sometimes duplicate columns get produced and we need to remove them
        """
        if (self.name == 'event_detail' or self.name == 'method_data') and len(args) > 0 and isinstance(args[0], list):
            idx = args[0].copy()
            idx.append('field_name')
            idx_cols = self.df[idx]
            dups = idx_cols.duplicated(keep='first')
            self.df = self.df[~dups]
            self.columns = list(self.df.columns)
        else:
            raise ValueError('{} is not a valid Frame to run the drop_duplicate_fields function on'.format(self.name))

    def pivot_data(self, index, columns='field_name', values='data_value'):
        """
        This will pivot method_data and event_data

        :param index: the index to use for pivoting
        :param columns: which field to use as columns (defaults to field_name)
        :param values: which field to use as columns in pivoting (defaults to event_data)
        """

        if self.name == 'method_data':
            method_data = self.df
            # group the data by method and create separate data frames for them
            all_data = {method_name: df.drop(['method'], axis=1)
                        for method_name, df in method_data.groupby(by='method')}
            method_data_null = method_data.loc[method_data.data_row_id.isna()]

            data_list = []

            # then, pivot data for each method
            for key in all_data.keys():
                temp_type = all_data[key]
                table_name = normalize_string(key)
                temp = temp_type.pivot(index=index,
                                       columns=columns,
                                       values=values).reset_index()

                # add columns with no values to the data
                null_fields = method_data_null.loc[method_data_null.method == key]
                if len(null_fields) > 0:
                    fields = null_fields.field_name.unique()
                    for attr in fields:
                        temp[attr] = None
                        temp_type = concat([temp_type, null_fields])

                temp_df = temp.loc[~isna(temp['data_row_id'])]
                new_frame = XMLFrame(table_name, temp_df, method_type=True)
                new_frame._cast_frame(temp_type)
                new_frame._clean_col_names()
                data_list.append(new_frame)

            self.pivot = data_list

        # not used, but added the functionality for generalizing this. Just check that the columns that are going to be
        # used for 'columns' and 'values' actually exist
        elif columns in self.columns and values in self.columns:
            new_df = self.df.pivot(index=index, columns=columns, values=values).reset_index()
            for col in index:
                temp_df = new_df.loc[~isna(new_df[col])]
                temp_xml = XMLFrame(self.name, temp_df, skip_id=True)
                temp_xml._clean_col_names()
                self.pivot = [temp_xml]

        else:
            raise ValueError('You are attempting to pivot an invalid XML Frame.')

    def to_df(self):
        """
        :return self.df: this just returns the underlying DataFrame
        """
        return self.df

    def to_sql(self, conn, schema='public'):
        """
        replicates the to_sql function with a few differences: only gets written if there's actually data, and duplicate
        data for certain tables will get removed.
        """
        table_name = self.name
        self._filter_exists(conn)
        df = self.df

        try:
            if len(df) > 0:
                df.to_sql(table_name, conn, if_exists='append', index=False, schema=schema)
        except exc.ProgrammingError:
            self._add_new_columns(conn)
