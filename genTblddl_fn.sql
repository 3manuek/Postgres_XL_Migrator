CREATE OR REPLACE FUNCTION %SCHEMA%.genTblddl(p_schema_name varchar,p_table_name varchar, dist_column varchar, dist_type varchar)
  RETURNS text AS
$$
DECLARE
    v_table_ddl   text;
    v_count       integer;
    column_record record;
    constraint_record record;
BEGIN
    FOR column_record IN 
        SELECT 
            table_name,
            column_name,
            data_type as column_type,
            character_maximum_length,
            column_default,
            CASE WHEN is_nullable = 'NO' THEN 
                'NOT NULL'
            ELSE ''
            END as column_not_nulL,
            ordinal_position,
            (select max(ordinal_position) from information_schema.columns where table_schema = p_schema_name and table_name = p_table_name) as max_ordinal_position
        FROM information_schema.columns where table_schema = p_schema_name and table_name = p_table_name
        ORDER BY ordinal_position
    LOOP

        IF column_record.ordinal_position = 1 THEN
            v_table_ddl:='CREATE TABLE '||p_schema_name||'.'||column_record.table_name||'(';
        ELSE
            v_table_ddl:=v_table_ddl||',';
        END IF;

        IF column_record.column_type = 'character varying' THEN
		column_record.column_type := column_record.column_type||'('||column_record.character_maximum_length||')';
	END IF;
       
        IF column_record.ordinal_position <= column_record.max_ordinal_position THEN
            IF column_record.column_type = 'integer' and column_record.column_default ~ 'nextval' THEN
		column_record.column_type := 'serial';
		column_record.column_default := '';
	    ELSIF column_record.column_type = 'bigint' and column_record.column_default ~ 'nextval' THEN
		column_record.column_type := 'bigserial';
		column_record.column_default := '';
	    END IF;
	    IF column_record.column_default != NULL OR column_record.column_default != '' THEN
		IF column_record.column_not_null != NULL OR column_record.column_not_null != '' THEN
			v_table_ddl:=v_table_ddl||chr(10)||
				'    '||column_record.column_name||' '||column_record.column_type||' DEFAULT '||column_record.column_default||' '||column_record.column_not_null;
                ELSE
                    v_table_ddl:=v_table_ddl||chr(10)||
			'    '||column_record.column_name||' '||column_record.column_type||' DEFAULT '||column_record.column_default;
                END IF;
            ELSIF column_record.column_not_null != NULL OR column_record.column_not_null != '' THEN
		v_table_ddl:=v_table_ddl||chr(10)||
		    '    '||column_record.column_name||' '||column_record.column_type||' '||column_record.column_not_null;
            ELSE
		v_table_ddl:=v_table_ddl||chr(10)||
                     '    '||column_record.column_name||' '||column_record.column_type;
            END IF;
 
        END IF;

    END LOOP;

    -- PostgresXL complains if the "distribute by" is used when there are more than one constraints.
    -- In that case, set the dist_column = 'x' to exclude the "distribute by" statement
    -- in the table creation DDL.
    v_count := (SELECT count(distinct b.constraint_type)
		FROM	information_schema.key_column_usage a
			,information_schema.table_constraints b
		WHERE 	a.table_schema = p_schema_name and a.table_name = p_table_name
		AND	a.constraint_name = b.constraint_name
	       );
	       
    IF v_count > 1 OR v_count < 1 THEN
	dist_column := 'x';
    END IF;

    -- Include the constraints in the table creation DDL
    FOR constraint_record in
	SELECT distinct a.constraint_name
			,a.column_name
			,b.constraint_type 

	FROM	 information_schema.key_column_usage a
		,information_schema.table_constraints b

	WHERE	a.table_schema = p_schema_name and a.table_name = p_table_name
	AND 	a.constraint_name = b.constraint_name

    LOOP
	IF constraint_record.constraint_name IS NOT NULL THEN
		v_table_ddl:=v_table_ddl||',';
		v_table_ddl:=v_table_ddl||chr(10)||
			'    '||'CONSTRAINT '||constraint_record.constraint_name||' '||constraint_record.constraint_type||'('||constraint_record.column_name||')';
	END IF;
    END LOOP;
    
    IF dist_column = 'x' THEN
	v_table_ddl:=v_table_ddl||chr(10)||');';
    ELSIF dist_type = 'replication' OR dist_type = 'roundrobin' THEN
	v_table_ddl:=v_table_ddl||chr(10)||') distribute by '||dist_type||';';
    ELSE
	v_table_ddl:=v_table_ddl||chr(10)||') distribute by '||dist_type||'('||dist_column||');';
    END IF;
    
    RETURN v_table_ddl;
END;
$$ LANGUAGE plpgsql;
