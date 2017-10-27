CREATE OR REPLACE PROCEDURE drop_if_exists( object_type IN VARCHAR2, object_name IN VARCHAR2 ) AUTHID CURRENT_USER IS
BEGIN
   DBMS_OUTPUT.put_line ('DROP ' || object_type || ' ' || object_name);
   EXECUTE IMMEDIATE 'DROP ' || object_type || ' "' || object_name || '"';
EXCEPTION
   WHEN OTHERS THEN
      IF (object_type='TABLE' AND SQLCODE != -942) OR (object_type='SEQUENCE' AND SQLCODE != -2289) THEN
         RAISE;
      END IF;
END drop_if_exists;

#

CALL drop_if_exists('TABLE','schema_serdes_mapping')#
CALL drop_if_exists('TABLE','schema_serdes_info')#
CALL drop_if_exists('TABLE','schema_field_info')#
CALL drop_if_exists('TABLE','schema_version_info')#
CALL drop_if_exists('TABLE','schema_metadata_info')#

CALL drop_if_exists('SEQUENCE','SCHEMA_METADATA_INFO')#
CALL drop_if_exists('SEQUENCE','SCHEMA_VERSION_INFO')#
CALL drop_if_exists('SEQUENCE','SCHEMA_FIELD_INFO')#
CALL drop_if_exists('SEQUENCE','SCHEMA_SERDES_INFO')#
CALL drop_if_exists('SEQUENCE','SCHEMA_SERDES_MAPPING')#