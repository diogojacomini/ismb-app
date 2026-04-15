-- ISMB Database - Utilitários

SET search_path TO public;


-- FUNCTION: describe_formatted(table_name TEXT)

CREATE OR REPLACE FUNCTION describe_formatted(p_table_name TEXT)
RETURNS TABLE (
    category    TEXT,
    attribute   TEXT,
    value       TEXT
) AS $$
DECLARE
    v_schema    TEXT;
    v_table     TEXT;
    v_oid       OID;
    v_owner     TEXT;
    v_tablespace TEXT;
    v_persistence TEXT;
    v_size      TEXT;
    v_row_count BIGINT;
BEGIN
    IF p_table_name LIKE '%.%' THEN
        v_schema := split_part(p_table_name, '.', 1);
        v_table  := split_part(p_table_name, '.', 2);
    ELSE
        v_schema := 'public';
        v_table  := p_table_name;
    END IF;

    SELECT c.oid INTO v_oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = v_schema
      AND c.relname = v_table
      AND c.relkind IN ('r', 'p'); -- table ou partitioned table

    IF v_oid IS NULL THEN
        RAISE EXCEPTION 'Table %.% does not exist', v_schema, v_table;
    END IF;

    SELECT
        pg_get_userbyid(c.relowner),
        COALESCE(ts.spcname, 'default'),
        CASE c.relpersistence
            WHEN 'p' THEN 'PERSISTENT'
            WHEN 'u' THEN 'UNLOGGED'
            WHEN 't' THEN 'TEMPORARY'
        END,
        pg_size_pretty(pg_total_relation_size(c.oid)),
        c.reltuples::bigint
    INTO v_owner, v_tablespace, v_persistence, v_size, v_row_count
    FROM pg_class c
    LEFT JOIN pg_tablespace ts ON ts.oid = c.reltablespace
    WHERE c.oid = v_oid;

    -- Table Information
    RETURN QUERY SELECT '# Table Information'::TEXT, ''::TEXT, ''::TEXT;
    RETURN QUERY SELECT 'Database'::TEXT, 'current_database()'::TEXT, current_database()::TEXT;
    RETURN QUERY SELECT 'Schema'::TEXT, ''::TEXT, v_schema;
    RETURN QUERY SELECT 'Table'::TEXT, ''::TEXT, v_table;
    RETURN QUERY SELECT 'Owner'::TEXT, ''::TEXT, v_owner;
    RETURN QUERY SELECT 'Tablespace'::TEXT, ''::TEXT, v_tablespace;
    RETURN QUERY SELECT 'Persistence'::TEXT, ''::TEXT, v_persistence;
    RETURN QUERY SELECT 'Table Type'::TEXT, ''::TEXT,
        CASE WHEN EXISTS (
            SELECT 1 FROM pg_partitioned_table WHERE partrelid = v_oid
        ) THEN 'PARTITIONED' ELSE 'REGULAR' END;


    -- Descrição da tabela (COMMENT)
    RETURN QUERY
    SELECT 'Table Comment'::TEXT, ''::TEXT, COALESCE(d.description, '')::TEXT
    FROM pg_description d
    WHERE d.objoid = v_oid AND d.objsubid = 0;

    RETURN QUERY SELECT ''::TEXT, ''::TEXT, ''::TEXT; -- linha em branco


    -- Columns
    RETURN QUERY SELECT '# Columns'::TEXT, ''::TEXT, ''::TEXT;
    RETURN QUERY SELECT 'col_name'::TEXT, 'data_type'::TEXT, 'comment'::TEXT;

    RETURN QUERY
    SELECT
        a.attname::TEXT,
        format_type(a.atttypid, a.atttypmod)::TEXT,
        COALESCE(d.description, '')::TEXT
    FROM pg_attribute a
    LEFT JOIN pg_description d
        ON d.objoid = v_oid AND d.objsubid = a.attnum
    WHERE a.attrelid = v_oid
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum;

    RETURN QUERY SELECT ''::TEXT, ''::TEXT, ''::TEXT;

    -- Constraints
    RETURN QUERY SELECT '# Constraints'::TEXT, ''::TEXT, ''::TEXT;
    RETURN QUERY SELECT 'constraint_name'::TEXT, 'type'::TEXT, 'definition'::TEXT;

    RETURN QUERY
    SELECT
        con.conname::TEXT,
        CASE con.contype
            WHEN 'p' THEN 'PRIMARY KEY'
            WHEN 'u' THEN 'UNIQUE'
            WHEN 'f' THEN 'FOREIGN KEY'
            WHEN 'c' THEN 'CHECK'
            ELSE con.contype::TEXT
        END::TEXT,
        pg_get_constraintdef(con.oid)::TEXT
    FROM pg_constraint con
    WHERE con.conrelid = v_oid
    ORDER BY con.contype, con.conname;

    RETURN QUERY SELECT ''::TEXT, ''::TEXT, ''::TEXT;

    -- Indexes
    RETURN QUERY SELECT '# Indexes'::TEXT, ''::TEXT, ''::TEXT;
    RETURN QUERY SELECT 'index_name'::TEXT, 'type'::TEXT, 'definition'::TEXT;

    RETURN QUERY
    SELECT
        i.indexrelid::regclass::TEXT,
        am.amname::TEXT,
        pg_get_indexdef(i.indexrelid)::TEXT
    FROM pg_index i
    JOIN pg_class ic ON ic.oid = i.indexrelid
    JOIN pg_am am ON am.oid = ic.relam
    WHERE i.indrelid = v_oid
    ORDER BY ic.relname;

    RETURN QUERY SELECT ''::TEXT, ''::TEXT, ''::TEXT;

    -- Storage Information
    RETURN QUERY SELECT '# Storage Information'::TEXT, ''::TEXT, ''::TEXT;
    RETURN QUERY SELECT 'Total Size'::TEXT, ''::TEXT, v_size;

    RETURN QUERY
    SELECT
        'Table Size'::TEXT,
        ''::TEXT,
        pg_size_pretty(pg_relation_size(v_oid))::TEXT;

    RETURN QUERY
    SELECT
        'Indexes Size'::TEXT,
        ''::TEXT,
        pg_size_pretty(pg_indexes_size(v_oid))::TEXT;

    RETURN QUERY
    SELECT
        'TOAST Size'::TEXT,
        ''::TEXT,
        pg_size_pretty(pg_total_relation_size(v_oid) - pg_relation_size(v_oid) - pg_indexes_size(v_oid))::TEXT;

    RETURN QUERY SELECT ''::TEXT, ''::TEXT, ''::TEXT;

    -- Partition Information
    IF EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid = v_oid) THEN
        RETURN QUERY SELECT '# Partition Information'::TEXT, ''::TEXT, ''::TEXT;

        RETURN QUERY
        SELECT
            'Partition Strategy'::TEXT,
            ''::TEXT,
            CASE pt.partstrat
                WHEN 'r' THEN 'RANGE'
                WHEN 'l' THEN 'LIST'
                WHEN 'h' THEN 'HASH'
            END::TEXT
        FROM pg_partitioned_table pt
        WHERE pt.partrelid = v_oid;

        RETURN QUERY
        SELECT
            'Partition Key'::TEXT,
            ''::TEXT,
            pg_get_partkeydef(v_oid)::TEXT;

        RETURN QUERY SELECT 'Partitions'::TEXT, 'size'::TEXT, 'rows'::TEXT;

        RETURN QUERY
        SELECT
            inh.inhrelid::regclass::TEXT,
            pg_size_pretty(pg_total_relation_size(inh.inhrelid))::TEXT,
            c.reltuples::bigint::TEXT
        FROM pg_inherits inh
        JOIN pg_class c ON c.oid = inh.inhrelid
        WHERE inh.inhparent = v_oid
        ORDER BY inh.inhrelid::regclass::TEXT;

        RETURN QUERY SELECT ''::TEXT, ''::TEXT, ''::TEXT;
    END IF;

END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION describe_formatted(TEXT) IS
    'Replica o DESCRIBE FORMATTED do Databricks: retorna metadados detalhados de uma tabela (colunas, constraints, índices, partições, tamanho).';

-- SELECT * FROM describe_formatted('stage.stage_cds');
