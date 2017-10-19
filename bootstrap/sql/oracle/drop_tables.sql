CREATE OR REPLACE PROCEDURE drop_if_exists( pv_type IN VARCHAR2, pv_table IN VARCHAR2 ) AS
BEGIN
   DBMS_OUTPUT.put_line ('DROP ' || pv_type || ' ' || pv_table);
   EXECUTE IMMEDIATE 'DROP ' || pv_type || ' "' || pv_table || '"';
EXCEPTION
   WHEN OTHERS THEN
      IF (pv_type='TABLE' AND SQLCODE != -942) OR (pv_type='SEQUENCE' AND SQLCODE != -2289) THEN
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