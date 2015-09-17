CREATE OR REPLACE FUNCTION %SCHEMA%.migration_analysis(v_src_schema varchar)
RETURNS TABLE(str_schema varchar(128)
             ,str_table varchar(128)
             ,str_column varchar(128)
             ,int_total_rows int
             ,flt_perc_diff FLOAT
             ,str_candidate varchar(3))  AS
$$
DECLARE
    my_record RECORD;
    int_records INT := 0;
BEGIN

    FOR my_record IN
           -- Lets get a list of all of the schemas, tables, and first columns from our database
        SELECT t.table_catalog AS str_catalog
              ,t.table_schema AS str_schema
              ,t.table_name AS str_table
              ,t.table_type AS str_table_type
              ,c.column_name AS str_column
        FROM (SELECT table_catalog
                    ,table_schema
                    ,table_name
                    ,table_type
              FROM information_schema.tables
              WHERE table_type NOT IN ('VIEW')
              AND table_schema = v_src_schema) AS t
       INNER JOIN (SELECT table_catalog
                         ,table_schema
                         ,table_name
                         ,MIN(ordinal_position) AS column_order
                   FROM information_schema.columns
                   WHERE table_schema = v_src_schema
                   GROUP BY table_catalog
                           ,table_schema
                           ,table_name) AS o
       ON t.table_catalog = o.table_catalog
       AND t.table_schema = o.table_schema
       AND t.table_name = o.table_name
       INNER JOIN (SELECT table_catalog
                         ,table_schema
                         ,table_name
                         ,column_name
                         ,ordinal_position AS column_order
                   FROM information_schema.columns
                   WHERE table_schema = v_src_schema) AS c
       ON o.table_catalog = c.table_catalog
       AND o.table_schema = c.table_schema
       AND o.table_name = c.table_name
       AND o.column_order = c.column_order
       ORDER BY t.table_catalog
               ,t.table_schema
               ,t.table_name

    LOOP
         int_records = int_records + 1;
         EXECUTE 'INSERT INTO %SCHEMA%.%TABLE%(str_schema
                                    		  ,str_table
                                    		  ,str_column
                                    		  ,int_total_rows
                                    		  ,flt_perc_diff
                                    		  ,str_candidate)
                  SELECT ''' || my_record.str_schema || ''' AS str_schema
                        ,''' || my_record.str_table || ''' AS str_table
                        ,''' || my_record.str_column || ''' AS str_column
                        ,tr.total_rows AS int_total_rows
                        ,CASE
                           WHEN tr.total_rows > 0 THEN ((tr.total_rows - dr.distinct_rows)/tr.total_rows)*100
                           ELSE 0
                         END AS flt_perc_diff
                        ,CASE
                           WHEN tr.total_rows = 0 THEN ''???''
                           WHEN ((tr.total_rows - dr.distinct_rows)/tr.total_rows)*100 > 99 THEN ''No''
                           ELSE ''Yes''
                         END AS str_candidate
                  FROM
                       (SELECT CAST(COUNT(COALESCE(CAST(' || my_record.str_column || ' AS varchar),'''')) AS float) AS total_rows
                        FROM ' || my_record.str_schema || '.' || my_record.str_table || ') AS tr
                            ,(SELECT CAST(COUNT(distinct_rows) AS float) AS distinct_rows
                              FROM (SELECT COALESCE(CAST(' || my_record.str_column || ' AS varchar),'''') AS distinct_rows
                                    FROM ' || my_record.str_schema || '.' || my_record.str_table || '
                        GROUP BY distinct_rows) AS t) AS dr';

    END LOOP;
END;
$$ LANGUAGE plpgsql;
