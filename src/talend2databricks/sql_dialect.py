"""Translate Redshift/PostgreSQL SQL dialect to Spark SQL.

Handles the common incompatibilities between Redshift SQL and Spark SQL
that appear in migrated Talend jobs.
"""
from __future__ import annotations

import re


def translate_redshift_to_spark(stmt: str) -> list[str]:
    """Translate a single Redshift SQL statement to Spark SQL.

    Returns a list because one Redshift statement may expand to multiple
    Spark SQL statements (e.g., DELETE...USING → MERGE or rewrite).
    """
    stripped = stmt.strip()
    if not stripped:
        return []

    # Apply transformations in order
    result = stripped

    # 1. DELETE ... FROM table USING other_table WHERE ...
    #    → DELETE FROM table WHERE EXISTS (SELECT 1 FROM other_table WHERE ...)
    result = _translate_delete_using(result)

    # 2. UPDATE ... FROM ... WHERE ...  (Redshift multi-table UPDATE)
    #    → MERGE INTO or rewritten UPDATE with subquery
    result = _translate_update_from(result)

    # 3. COPY ... FROM 's3://...'  → comment out (handled by DataFrame reads)
    result = _translate_copy(result)

    # 4. VARCHAR(n) → STRING in CREATE TABLE
    result = _translate_data_types(result)

    # 5. column ~ 'pattern' → column RLIKE 'pattern' (regex match)
    result = _translate_regex_operator(result)

    # 6. column !~ 'pattern' → NOT column RLIKE 'pattern'
    result = _translate_negative_regex(result)

    # 7. GRANT ... → comment out (use Unity Catalog permissions)
    result = _translate_grant(result)

    # 8. getdate() → current_timestamp()
    result = _translate_functions(result)

    # 9. ISNULL(x) → x IS NULL  (Redshift function form)
    # (already handled if it appears in SQL; Talend expressions handled elsewhere)

    if not result.strip():
        return []

    return [result]


def _translate_delete_using(stmt: str) -> str:
    """DELETE FROM t1 USING t2 WHERE ... → DELETE FROM t1 WHERE EXISTS (...)"""
    pattern = re.compile(
        r'DELETE\s+FROM\s+(\S+)\s+USING\s+(\S+)\s+WHERE\s+(.*)',
        re.IGNORECASE | re.DOTALL
    )
    # Also handle: DELETE FROM t1 using\n t2 WHERE ...  (newline before table)
    # And: DELETE \nFROM t1  using\n t2 \nWHERE ...
    pattern2 = re.compile(
        r'DELETE\s+FROM\s+(\S+)\s+using\s+(\S+)\s+WHERE\s+(.*)',
        re.IGNORECASE | re.DOTALL
    )

    for pat in [pattern, pattern2]:
        m = pat.match(stmt.strip())
        if m:
            target = m.group(1).strip()
            using_table = m.group(2).strip()
            where_clause = m.group(3).strip()

            # Rewrite join conditions: target.col = using.col → t.col = u.col
            return (
                f"DELETE FROM {target} WHERE EXISTS (\n"
                f"  SELECT 1 FROM {using_table}\n"
                f"  WHERE {where_clause}\n"
                f")"
            )

    return stmt


def _translate_update_from(stmt: str) -> str:
    """UPDATE t1 SET ... FROM t1, t2 WHERE ... → MERGE or subquery UPDATE.

    Redshift allows:
      UPDATE t1 SET col = t2.col FROM t1, t2 WHERE t1.id = t2.id
    Spark SQL requires:
      MERGE INTO t1 USING t2 ON t1.id = t2.id WHEN MATCHED THEN UPDATE SET ...
    """
    # Match: UPDATE table SET ... FROM table, other_table [, ...] WHERE ...
    pattern = re.compile(
        r'UPDATE\s+(\S+)\s+SET\s+(.*?)\s+FROM\s+(.*?)\s+WHERE\s+(.*)',
        re.IGNORECASE | re.DOTALL
    )
    m = pattern.match(stmt.strip())
    if not m:
        return stmt

    target = m.group(1).strip()
    set_clause = m.group(2).strip()
    from_tables = m.group(3).strip()
    where_clause = m.group(4).strip()

    # Parse FROM tables - remove the target table if it appears there
    tables = [t.strip() for t in from_tables.split(',')]
    source_tables = []
    for t in tables:
        # Could be "table alias" or just "table"
        tname = t.split()[0].strip().lower()
        if tname != target.lower():
            source_tables.append(t)

    if not source_tables:
        # No external table in FROM, just a self-referencing UPDATE
        return f"UPDATE {target} SET {set_clause} WHERE {where_clause}"

    if len(source_tables) == 1:
        source = source_tables[0]
        return (
            f"MERGE INTO {target}\n"
            f"USING {source}\n"
            f"ON {where_clause}\n"
            f"WHEN MATCHED THEN UPDATE SET {set_clause}"
        )

    # Multiple source tables - use subquery approach
    subquery_from = ", ".join(source_tables)
    return (
        f"MERGE INTO {target}\n"
        f"USING (SELECT * FROM {subquery_from} WHERE {where_clause}) _src\n"
        f"ON {where_clause}\n"
        f"WHEN MATCHED THEN UPDATE SET {set_clause}"
    )


def _translate_copy(stmt: str) -> str:
    """COPY table FROM 's3://...' ... → commented out with migration note."""
    if re.match(r'\s*copy\s+', stmt, re.IGNORECASE):
        lines = stmt.strip().split('\n')
        commented = '\n'.join(f'-- {line}' for line in lines)
        return (
            "-- TODO: COPY command migrated to DataFrame read.\n"
            "-- Use spark.read.csv() or spark.read.format() instead.\n"
            f"{commented}"
        )
    return stmt


def _translate_data_types(stmt: str) -> str:
    """Convert Redshift data types in CREATE TABLE to Spark SQL types."""
    if not re.match(r'\s*CREATE\s+TABLE', stmt, re.IGNORECASE):
        return stmt

    result = stmt
    # VARCHAR(n) → STRING
    result = re.sub(r'\bvarchar\(\d+\)', 'STRING', result, flags=re.IGNORECASE)
    result = re.sub(r'\bvarchar\b', 'STRING', result, flags=re.IGNORECASE)
    # INT4 → INT
    result = re.sub(r'\bint4\b', 'INT', result, flags=re.IGNORECASE)
    result = re.sub(r'\binteger\b', 'INT', result, flags=re.IGNORECASE)
    # FLOAT8 → DOUBLE
    result = re.sub(r'\bfloat8\b', 'DOUBLE', result, flags=re.IGNORECASE)
    result = re.sub(r'\bfloat4\b', 'FLOAT', result, flags=re.IGNORECASE)
    # BOOL → BOOLEAN (already valid in Spark)
    # TIMESTAMP → TIMESTAMP (already valid)
    # DATE → DATE (already valid)
    # TEXT → STRING
    result = re.sub(r'\btext\b', 'STRING', result, flags=re.IGNORECASE)
    # BIGINT → BIGINT (already valid)
    # SMALLINT → SMALLINT (already valid)
    # CASCADE → remove (Spark doesn't support CASCADE on DROP TABLE within CREATE)

    return result


def _translate_regex_operator(stmt: str) -> str:
    """column ~ 'pattern' → column RLIKE 'pattern'"""
    # Match: word ~ 'pattern'  but not !~
    result = re.sub(
        r"(\w+)\s+~\s+'([^']*)'",
        r"\1 RLIKE '\2'",
        stmt
    )
    return result


def _translate_negative_regex(stmt: str) -> str:
    """column !~ 'pattern' → NOT column RLIKE 'pattern'"""
    result = re.sub(
        r"(\w+)\s+!~\s+'([^']*)'",
        r"NOT \1 RLIKE '\2'",
        stmt
    )
    return result


def _translate_grant(stmt: str) -> str:
    """GRANT ... → comment out."""
    if re.match(r'\s*grant\s+', stmt, re.IGNORECASE):
        lines = stmt.strip().split('\n')
        commented = '\n'.join(f'-- {line}' for line in lines)
        return f"-- TODO: Use Unity Catalog permissions instead.\n{commented}"
    return stmt


def _translate_functions(stmt: str) -> str:
    """Translate Redshift-specific functions to Spark SQL equivalents."""
    result = stmt
    # getdate() → current_timestamp()
    result = re.sub(r'\bgetdate\(\)', 'current_timestamp()', result, flags=re.IGNORECASE)
    # SYSDATE → current_timestamp()
    result = re.sub(r'\bsysdate\b', 'current_timestamp()', result, flags=re.IGNORECASE)
    # NVL(a, b) → COALESCE(a, b)
    result = re.sub(r'\bNVL\(', 'COALESCE(', result, flags=re.IGNORECASE)
    # CONVERT(type, expr) → CAST(expr AS type) — but this is context-dependent
    # LEN(x) → LENGTH(x) (Redshift LEN vs Spark LENGTH)
    result = re.sub(r'\bLEN\(', 'LENGTH(', result, flags=re.IGNORECASE)
    # STRTOL → conv (hex to decimal) — rare, skip for now
    return result


def translate_all_statements(stmts: list[str]) -> list[str]:
    """Translate a list of Redshift SQL statements to Spark SQL.

    Handles multi-statement strings (separated by ;) by splitting first,
    translating each, then returning the flat list.
    """
    result = []
    for stmt in stmts:
        # Split multi-statement strings on semicolons
        parts = _split_sql_statements(stmt)
        for part in parts:
            translated = translate_redshift_to_spark(part.strip())
            result.extend(translated)
    return result


def _split_sql_statements(sql: str) -> list[str]:
    """Split a SQL string on semicolons, respecting quoted strings."""
    statements = []
    current = []
    in_single_quote = False
    in_double_quote = False

    for char in sql:
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
        elif char == ';' and not in_single_quote and not in_double_quote:
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            continue
        current.append(char)

    # Last statement (no trailing semicolon)
    stmt = ''.join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements
