#
# Copyright 2015 LinkedIn Corp. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#

import json
import re
import sys
import time
from com.ziclix.python.sql import zxJDBC
from org.slf4j import LoggerFactory
from wherehows.common.writers import FileWriter
from wherehows.common.schemas import DatasetSchemaRecord, DatasetFieldRecord, HiveDependencyInstanceRecord, DatasetInstanceRecord
from wherehows.common import Constant
# from HiveExtract import TableInfo
from org.apache.hadoop.hive.ql.tools import LineageInfo
from metadata.etl.dataset.hive import HiveViewDependency

# from HiveColumnParser import HiveColumnParser
# from AvroColumnParser import AvroColumnParser


class TableInfo:
    """ Class to define the variable name  """
    table_name = 'name'
    dataset_name = 'dataset_name'
    native_name = 'native_name'
    logical_name = 'logical_name'
    version = 'version'
    type = 'type'
    serialization_format = 'serialization_format'
    create_time = 'create_time'
    schema_url = 'schema_url'
    field_delimiter = 'field_delimiter'
    db_id = 'DB_ID'
    table_id = 'TBL_ID'
    serde_id = 'SD_ID'
    table_type = 'tbl_type'
    location = 'location'
    view_expended_text = 'view_expanded_text'
    input_format = 'input_format'
    output_format = 'output_format'
    is_compressed = 'is_compressed'
    is_storedassubdirectories = 'is_storedassubdirectories'
    etl_source = 'etl_source'
    source_modified_time = 'source_modified_time'

    field_list = 'fields'
    schema_literal = 'schema_literal'

    optional_prop = [create_time, serialization_format, field_delimiter, schema_url, db_id, table_id, serde_id,
                     table_type, location, view_expended_text, input_format, output_format, is_compressed,
                     is_storedassubdirectories, etl_source]

class AvroColumnParser:
    """
    This class is used to parse the avro schema, get a list of columns inside it.
    As avro is nested, we use a recursive way to parse it.
    Currently used in HDFS avro file schema parsing and Hive avro schema parsing.
    """

    def __init__(self, avro_schema, urn = None):
        """
        :param avro_schema: json of schema
        :param urn: optional, could contain inside schema
        :return:
        """
        self.sort_id = 0
        if not urn:
            self.urn = avro_schema['uri'] if 'uri' in avro_schema else None
        else:
            self.urn = urn
        self.result = []
        self.fields_json_to_csv(self.result, '', avro_schema['fields'])

    def get_column_list_result(self):
        """

        :return:
        """
        return self.result

    def fields_json_to_csv(self, output_list_, parent_field_path, field_list_):
        """
            Recursive function, extract nested fields out of avro.
        """
        parent_id = self.sort_id

        for f in field_list_:
            self.sort_id += 1

            o_field_name = f['name']
            o_field_data_type = ''
            o_field_data_size = None
            o_field_nullable = 'N'
            o_field_default = ''
            o_field_namespace = ''
            o_field_doc = ''
            effective_type_index_in_type = -1

            if f.has_key('namespace'):
                o_field_namespace = f['namespace']

            if f.has_key('default') and type(f['default']) != None:
                o_field_default = f['default']

            if not f.has_key('type'):
                o_field_data_type = None
            elif type(f['type']) == list:
                i = effective_type_index = -1
                for data_type in f['type']:
                    i += 1  # current index
                    if type(data_type) is None or (data_type == 'null'):
                        o_field_nullable = 'Y'
                    elif type(data_type) == dict:
                        o_field_data_type = data_type['type']
                        effective_type_index_in_type = i

                        if data_type.has_key('namespace'):
                            o_field_namespace = data_type['namespace']
                        elif data_type.has_key('name'):
                            o_field_namespace = data_type['name']

                        if data_type.has_key('size'):
                            o_field_data_size = data_type['size']
                        else:
                            o_field_data_size = None

                    else:
                        o_field_data_type = data_type
                        effective_type_index_in_type = i
            elif type(f['type']) == dict:
                o_field_data_type = f['type']['type']
            else:
                o_field_data_type = f['type']
                if f.has_key('attributes') and f['attributes'].has_key('nullable'):
                    o_field_nullable = 'Y' if f['attributes']['nullable'] else 'N'
                if f.has_key('attributes') and f['attributes'].has_key('size'):
                    o_field_data_size = f['attributes']['size']

            if f.has_key('doc'):
                if len(f['doc']) == 0 and f.has_key('attributes'):
                    o_field_doc = json.dumps(f['attributes'])
                else:
                    o_field_doc = f['doc']
            elif f.has_key('comment'):
                o_field_doc = f['comment']

            output_list_.append(
                [self.urn, self.sort_id, parent_id, parent_field_path, o_field_name, o_field_data_type, o_field_nullable,
                 o_field_default, o_field_data_size, o_field_namespace,
                 o_field_doc.replace("\n", ' ') if o_field_doc is not None else None])

            # check if this field is a nested record
            if type(f['type']) == dict and f['type'].has_key('fields'):
                current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
                self.fields_json_to_csv(output_list_, current_field_path, f['type']['fields'])
            elif type(f['type']) == dict and f['type'].has_key('items') and type(f['type']['items']) == dict and f['type']['items'].has_key('fields'):
                current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
                self.fields_json_to_csv(output_list_, current_field_path, f['type']['items']['fields'])

            if effective_type_index_in_type >= 0 and type(f['type'][effective_type_index_in_type]) == dict:
                if f['type'][effective_type_index_in_type].has_key('items') and type(
                        f['type'][effective_type_index_in_type]['items']) == list:

                    for item in f['type'][effective_type_index_in_type]['items']:
                        if type(item) == dict and item.has_key('fields'):
                            current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
                            self.fields_json_to_csv(output_list_, current_field_path, item['fields'])
                elif f['type'][effective_type_index_in_type].has_key('items') and type(f['type'][effective_type_index_in_type]['items'])== dict and f['type'][effective_type_index_in_type]['items'].has_key('fields'):
                    # type: [ null, { type: array, items: { name: xxx, type: record, fields: [] } } ]
                    current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
                    self.fields_json_to_csv(output_list_, current_field_path, f['type'][effective_type_index_in_type]['items']['fields'])
                elif f['type'][effective_type_index_in_type].has_key('fields'):
                    # if f['type'][effective_type_index_in_type].has_key('namespace'):
                    # o_field_namespace = f['type'][effective_type_index_in_type]['namespace']
                    current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
                    self.fields_json_to_csv(output_list_, current_field_path, f['type'][effective_type_index_in_type]['fields'])

                    # End of function


class HiveColumnParser:
    """
    This class used for parsing a Hive metastore schema.
    Major effort is parse complex column types :
    Reference : https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
    """
    def string_interface(self, dataset_urn, input):
        schema = json.loads(input)
        return HiveColumnParser(dataset_urn, schema)

    def __init__(self, avro_schema, urn = None):
        """

        :param avro_schema json schema
        :param urn:
        :return:
        """
        self.prefix = ''
        if not urn:
            self.dataset_urn = avro_schema['uri'] if 'uri' in avro_schema else None
        else:
            self.dataset_urn = urn
        self.column_type_dict = avro_schema
        self.column_type_list = []
        self.sort_id = 0
        for index, f in enumerate(avro_schema['fields']):
            avro_schema['fields'][index] = self.parse_column(f['ColumnName'], f['TypeName'], f['Comment'])

        # padding list for rest 4 None, skip  is_nullable, default_value, data_size, namespace for now
        self.column_type_list = [(x[:6] + [None] * 4 + x[6:]) for x in self.column_type_list]

    def parse_column(self, column_name, type_string, comment=None):
        """
        Returns a dictionary of a Hive column's type metadata and
         any complex or nested type info
        """
        simple_type, inner, comment_inside = self._parse_type(type_string)
        comment = comment if comment else comment_inside
        column = {'name': column_name, 'type' : simple_type, 'doc': comment}

        self.sort_id += 1
        self.column_type_list.append([self.dataset_urn, self.sort_id, 0, self.prefix, column_name, simple_type, comment])
        if inner:
            self.prefix = column_name
            column.update(self._parse_complex(simple_type, inner, self.sort_id))

        # reset prefix after each outermost field
        self.prefix = ''
        return column

    def is_scalar_type(self, type_string):
        return not (type_string.startswith('array') or type_string.startswith('map') or type_string.startswith(
            'struct') or type_string.startswith('uniontype'))

    def _parse_type(self, type_string):
        pattern = re.compile(r"^([a-z]+[(),0-9]*)(<(.+)>)?( comment '(.*)')?$", re.IGNORECASE)
        match = re.search(pattern, type_string)
        if match is None:
            return None, None, None
        return match.group(1), match.group(3), match.group(5)

    def _parse_complex(self, simple_type, inner, parent_id):
        complex_type = {}
        if simple_type == "array":
            complex_type['item'] = self._parse_array_item(inner, parent_id)
        elif simple_type == "map":
            complex_type['key'] = self._parse_map_key(inner)
            complex_type['value'] = self._parse_map_value(inner, parent_id)
        elif simple_type == "struct":
            complex_type['fields'] = self._parse_struct_fields(inner, parent_id)
        elif simple_type == "uniontype":
            complex_type['union'] = self._parse_union_types(inner, parent_id)
        return complex_type

    def _parse_array_item(self, inner, parent_id):
        item = {}
        simple_type, inner, comment = self._parse_type(inner)
        item['type'] = simple_type
        item['doc'] = comment
        if inner:
            item.update(self._parse_complex(simple_type, inner, parent_id))
        return item

    def _parse_map_key(self, inner):
        key = {}
        key_type = inner.split(',', 1)[0]
        key['type'] = key_type
        return key

    def _parse_map_value(self, inner, parent_id):
        value = {}
        value_type = inner.split(',', 1)[1]
        simple_type, inner, comment = self._parse_type(value_type)
        value['type'] = simple_type
        value['doc'] = comment
        if inner:
            value.update(self._parse_complex(simple_type, inner, parent_id))
        return value

    def _parse_struct_fields(self, inner, parent_id):
        current_prefix = self.prefix
        fields = []
        field_tuples = self._split_struct_fields(inner)
        for (name, value) in field_tuples:
            field = {}
            name = name.strip(',')
            field['name'] = name
            simple_type, inner, comment = self._parse_type(value)
            field['type'] = simple_type
            field['doc'] = comment
            self.sort_id += 1
            self.column_type_list.append([self.dataset_urn, self.sort_id, parent_id, self.prefix, name, simple_type, comment])
            if inner:
                self.prefix += '.' + name
                field.update(self._parse_complex(simple_type, inner, self.sort_id))
                self.prefix = current_prefix
            fields.append(field)
        return fields

    def _split_struct_fields(self, fields_string):
        fields = []
        remaining = fields_string
        while remaining:
            (fieldname, fieldvalue), remaining = self._get_next_struct_field(remaining)
            fields.append((fieldname, fieldvalue))
        return fields

    def _get_next_struct_field(self, fields_string):
        fieldname, rest = fields_string.split(':', 1)
        balanced = 0
        for pos, char in enumerate(rest):
            balanced += {'<': 1, '>': -1, '(': 100, ')': -100}.get(char, 0)
            if balanced == 0 and char in ['>', ',']:
                if rest[pos + 1:].startswith(' comment '):
                    pattern = re.compile(r"( comment '.*?')(,[a-z_0-9]+:)?", re.IGNORECASE)
                    match = re.search(pattern, rest[pos + 1:])
                    cm_len = len(match.group(1))
                    return (fieldname, rest[:pos + 1 + cm_len].strip(',')), rest[pos + 2 + cm_len:]
                return (fieldname, rest[:pos + 1].strip(',')), rest[pos + 1:]
        return (fieldname, rest), None

    def _parse_union_types(self, inner, parent_id):
        current_prefix = self.prefix
        types = []
        type_tuples = []
        bracket_level = 0
        current = []

        for c in (inner + ","):
            if c == "," and bracket_level == 0:
                type_tuples.append("".join(current))
                current = []
            else:
                if c == "<":
                    bracket_level += 1
                elif c == ">":
                    bracket_level -= 1
                current.append(c)


        for pos, value in enumerate(type_tuples):
            field = {}
            name = 'type' + str(pos)
            field['name'] = name
            #print 'type' + str(pos) + "\t" + value
            simple_type, inner, comment = self._parse_type(value)
            field['type'] = simple_type
            field['doc'] = comment
            self.sort_id += 1
            self.column_type_list.append([self.dataset_urn, self.sort_id, parent_id, self.prefix, name, simple_type, comment])

            if inner:
                self.prefix += '.' + name
                field.update(self._parse_complex(simple_type, inner, self.sort_id))
                self.prefix = current_prefix
            types.append(field)
        return types


class HiveTransform:
  dataset_dict = {}
  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    username = args[Constant.HIVE_METASTORE_USERNAME]
    password = args[Constant.HIVE_METASTORE_PASSWORD]
    jdbc_driver = args[Constant.HIVE_METASTORE_JDBC_DRIVER]
    jdbc_url = args[Constant.HIVE_METASTORE_JDBC_URL]
    self.conn_hms = zxJDBC.connect(jdbc_url, username, password, jdbc_driver)
    self.curs = self.conn_hms.cursor()
    dependency_instance_file = args[Constant.HIVE_DEPENDENCY_CSV_FILE_KEY]
    self.instance_writer = FileWriter(dependency_instance_file)

  def transform(self, input, hive_instance, hive_metadata, hive_field_metadata):
    """
    convert from json to csv
    :param input: input json file
    :param hive_metadata: output data file for hive table metadata
    :param hive_field_metadata: output data file for hive field metadata
    :return:
    """
    all_data = []
    with open(input) as input_file:
        for line in input_file:
            all_data.append(json.loads(line))

    dataset_idx = -1

    instance_file_writer = FileWriter(hive_instance)
    schema_file_writer = FileWriter(hive_metadata)
    field_file_writer = FileWriter(hive_field_metadata)

    lineageInfo = LineageInfo()
    depends_sql = """
      SELECT d.NAME DB_NAME, case when t.TBL_NAME regexp '_[0-9]+_[0-9]+_[0-9]+$'
          then concat(substring(t.TBL_NAME, 1, length(t.TBL_NAME) - length(substring_index(t.TBL_NAME, '_', -3)) - 1),'_{version}')
        else t.TBL_NAME
        end dataset_name,
        concat('/', d.NAME, '/', t.TBL_NAME) object_name,
        case when (d.NAME like '%\_mp' or d.NAME like '%\_mp\_versioned') and d.NAME not like 'dalitest%' and t.TBL_TYPE = 'VIRTUAL_VIEW'
          then 'dalids'
        else 'hive'
        end object_type,
        case when (d.NAME like '%\_mp' or d.NAME like '%\_mp\_versioned') and d.NAME not like 'dalitest%' and t.TBL_TYPE = 'VIRTUAL_VIEW'
          then 'View'
        else
            case when LOCATE('view', LOWER(t.TBL_TYPE)) > 0 then 'View'
          when LOCATE('index', LOWER(t.TBL_TYPE)) > 0 then 'Index'
            else 'Table'
          end
        end object_sub_type,
        case when (d.NAME like '%\_mp' or d.NAME like '%\_mp\_versioned') and t.TBL_TYPE = 'VIRTUAL_VIEW'
          then 'dalids'
        else 'hive'
        end prefix
      FROM TBLS t JOIN DBS d on t.DB_ID = d.DB_ID
      WHERE d.NAME = '{db_name}' and t.TBL_NAME = '{table_name}'
      """

    # one db info : 'type', 'database', 'tables'
    # one table info : required : 'name' , 'type', 'serializationFormat' ,'createTime', 'DB_ID', 'TBL_ID', 'SD_ID'
    #                  optional : 'schemaLiteral', 'schemaUrl', 'fieldDelimiter', 'fieldList'
    for one_db_info in all_data:
      i = 0
      for table in one_db_info['tables']:
        i += 1
        schema_json = {}
        prop_json = {}  # set the prop json

        for prop_name in TableInfo.optional_prop:
          if prop_name in table and table[prop_name] is not None:
            prop_json[prop_name] = table[prop_name]

        view_expanded_text = ''

        if TableInfo.view_expended_text in prop_json:
          view_expanded_text = prop_json[TableInfo.view_expended_text]
          text = prop_json[TableInfo.view_expended_text].replace('`', '')	# this will be fixed after switching to Hive AST
          array = []
          try:
            array = HiveViewDependency.getViewDependency(text)
          except:
            self.logger.error("HiveViewDependency.getViewDependency(%s) failed!" % (table['name']))

          l = []
          for a in array:
            l.append(a)
            names = str(a).split('.')
            if names and len(names) >= 2:
              db_name = names[0].lower()
              table_name = names[1].lower()
              if db_name and table_name:
                self.curs.execute(depends_sql.format(db_name=db_name, table_name=table_name, version='{version}'))
                rows = self.curs.fetchall()
                self.conn_hms.commit()
                if rows and len(rows) > 0:
                  for row_index, row_value in enumerate(rows):
                    dependent_record = HiveDependencyInstanceRecord(
                                          one_db_info['type'],
                                          table['type'],
                                          "/%s/%s" % (one_db_info['database'], table['name']),
                                          'dalids:///' + one_db_info['database'] + '/' + table['dataset_name']
                                          if one_db_info['type'].lower() == 'dalids'
                                          else 'hive:///' + one_db_info['database'] + '/' + table['dataset_name'],
                                          'depends on',
                                          'Y',
                                          row_value[3],
                                          row_value[4],
                                          row_value[2],
                                          row_value[5] + ':///' + row_value[0] + '/' + row_value[1], '')
                    self.instance_writer.append(dependent_record)
          prop_json['view_depends_on'] = l
          self.instance_writer.flush()

        # process either schema
        flds = {}
        field_detail_list = []

        if TableInfo.schema_literal in table and \
           table[TableInfo.schema_literal] is not None and \
           table[TableInfo.schema_literal].startswith('{'):
          sort_id = 0
          urn = "hive:///%s/%s" % (one_db_info['database'], table['dataset_name'])
          self.logger.info("Getting schema literal for: %s" % (urn))
          try:
            schema_data = json.loads(table[TableInfo.schema_literal])
            schema_json = schema_data
            acp = AvroColumnParser(schema_data, urn = urn)
            result = acp.get_column_list_result()
            field_detail_list += result
          except ValueError:
            self.logger.error("Schema Literal JSON error for table: " + str(table))

        elif TableInfo.field_list in table:
          # Convert to avro
          uri = "hive:///%s/%s" % (one_db_info['database'], table['dataset_name'])
          if one_db_info['type'].lower() == 'dalids':
            uri = "dalids:///%s/%s" % (one_db_info['database'], table['dataset_name'])
          else:
            uri = "hive:///%s/%s" % (one_db_info['database'], table['dataset_name'])
          self.logger.info("Getting column definition for: %s" % (uri))
          try:
            hcp = HiveColumnParser(table, urn = uri)
            schema_json = {'fields' : hcp.column_type_dict['fields'], 'type' : 'record', 'name' : table['name'], 'uri' : uri}
            field_detail_list += hcp.column_type_list
          except:
            self.logger.error("HiveColumnParser(%s) failed!" % (uri))
            schema_json = {'fields' : {}, 'type' : 'record', 'name' : table['name'], 'uri' : uri}

        if one_db_info['type'].lower() == 'dalids':
          dataset_urn = "dalids:///%s/%s" % (one_db_info['database'], table['dataset_name'])
        else:
          dataset_urn = "hive:///%s/%s" % (one_db_info['database'], table['dataset_name'])

        dataset_instance_record = DatasetInstanceRecord('dalids:///' + one_db_info['database'] + '/' + table['name']
                                                if one_db_info['type'].lower() == 'dalids'
                                                else 'hive:///' + one_db_info['database'] + '/' + table['name'],
                                                'grid',
                                                '',
                                                '',
                                                '*',
                                                True,
                                                table['native_name'],
                                                table['logical_name'],
                                                table['version'],
                                                table['create_time'],
                                                json.dumps(schema_json),
                                                json.dumps(view_expanded_text),
                                                dataset_urn)
        instance_file_writer.append(dataset_instance_record)

        if dataset_urn not in self.dataset_dict:
          dataset_scehma_record = DatasetSchemaRecord(table['dataset_name'], json.dumps(schema_json),
                                                      json.dumps(prop_json),
                                                      json.dumps(flds), dataset_urn, 'Hive', one_db_info['type'],
                                                      table['type'], '',
                                                      table.get(TableInfo.create_time),
                                                      (int(table.get(TableInfo.source_modified_time,"0"))))
          schema_file_writer.append(dataset_scehma_record)

          dataset_idx += 1
          self.dataset_dict[dataset_urn] = dataset_idx

          for fields in field_detail_list:
            field_record = DatasetFieldRecord(fields)
            field_file_writer.append(field_record)



      instance_file_writer.flush()
      schema_file_writer.flush()
      field_file_writer.flush()
      self.logger.info("%20s contains %6d tables" % (one_db_info['database'], i))

    instance_file_writer.close()
    schema_file_writer.close()
    field_file_writer.close()


if __name__ == "__main__":
  args = sys.argv[1]
  t = HiveTransform()
  try:
    t.transform(args[Constant.HIVE_SCHEMA_JSON_FILE_KEY],
                args[Constant.HIVE_INSTANCE_CSV_FILE_KEY],
                args[Constant.HIVE_SCHEMA_CSV_FILE_KEY],
                args[Constant.HIVE_FIELD_METADATA_KEY])
  finally:
    t.curs.close()
    t.conn_hms.close()
    t.instance_writer.close()



