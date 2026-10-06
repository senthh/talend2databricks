"""Translate Talend Java expressions to PySpark F.* expressions.

Handles:
- StringHandling.LEN → F.length
- StringHandling.TRIM → F.trim
- StringHandling.UPCASE → F.upper
- StringHandling.DOWNCASE → F.lower
- StringHandling.LEFT → F.substring(..., 1, n)
- CommonDb.replace → F.regexp_replace
- TalendString.talendTrim → custom ltrim/rtrim
- TalendDate.formatDate → F.date_format
- Relational.ISNULL → F.isnull
- Ternary (cond ? a : b) → F.when(cond, a).otherwise(b)
- Java .equals() → == comparisons
- // and /* */ comments → stripped
- row.field references → col("alias__field")
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class TranslationResult:
    pyspark_expr: str = ""
    confidence: float = 1.0  # 0.0-1.0
    warnings: list[str] = field(default_factory=list)
    original: str = ""
    is_passthrough: bool = False  # simple column ref


def _strip_comments(expr: str) -> str:
    """Remove Java single-line (//) and multi-line (/* */) comments."""
    # Multi-line first
    expr = re.sub(r'/\*.*?\*/', '', expr, flags=re.DOTALL)
    # Single-line (only at start of line or after whitespace)
    expr = re.sub(r'//[^\n]*', '', expr)
    return expr.strip()


def _clean_quoted_strings(expr: str) -> str:
    """Normalize Java string literals to Python."""
    # Already double-quoted in most cases from XML unescape
    return expr


def _translate_column_ref(ref: str, table_aliases: dict[str, str] | None = None) -> str:
    """Translate row2.field_name → col('row2__field_name') for PySpark."""
    m = re.match(r'^(\w+)\.(\w+)\s*$', ref.strip())
    if m:
        table, column = m.group(1), m.group(2)
        return f'F.col("{table}__{column}")'
    return ref.strip()


def _is_simple_ref(expr: str) -> bool:
    """Check if expression is just a simple table.column reference."""
    return bool(re.match(r'^\s*\w+\.\w+\s*$', expr.strip()))


class ExpressionTranslator:
    """Translate Talend Java expressions to PySpark."""

    def __init__(self):
        self._stats = {"total": 0, "auto": 0, "partial": 0, "manual": 0}

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    def translate(self, expr: str, col_name: str = "") -> TranslationResult:
        """Translate a single Talend expression to PySpark.

        Args:
            expr: The Talend expression string.
            col_name: Target column name for context.

        Returns:
            TranslationResult with the PySpark expression.
        """
        self._stats["total"] += 1
        result = TranslationResult(original=expr.strip())

        if not expr or not expr.strip():
            result.pyspark_expr = f'F.lit(None).alias("{col_name}")'
            result.confidence = 1.0
            self._stats["auto"] += 1
            return result

        cleaned = _strip_comments(expr)
        if not cleaned:
            # Expression was all comments, use the last non-comment line
            lines = expr.strip().split('\n')
            for line in reversed(lines):
                stripped = _strip_comments(line)
                if stripped:
                    cleaned = stripped
                    break
            if not cleaned:
                result.pyspark_expr = f'F.lit(None).alias("{col_name}")'
                result.confidence = 0.5
                result.warnings.append("Expression was entirely comments")
                self._stats["partial"] += 1
                return result

        # Simple column reference
        if _is_simple_ref(cleaned):
            ref = _translate_column_ref(cleaned)
            result.pyspark_expr = f'{ref}.alias("{col_name}")'
            result.is_passthrough = True
            result.confidence = 1.0
            self._stats["auto"] += 1
            return result

        # Try complex expression translation
        try:
            translated = self._translate_complex(cleaned)
            result.pyspark_expr = f'{translated}.alias("{col_name}")'
            result.confidence = translated_confidence(cleaned, translated)
            if result.confidence >= 0.8:
                self._stats["auto"] += 1
            elif result.confidence >= 0.5:
                self._stats["partial"] += 1
            else:
                self._stats["manual"] += 1
        except Exception as e:
            result.pyspark_expr = f'F.lit("TODO: {_escape(cleaned)}").alias("{col_name}")'
            result.confidence = 0.0
            result.warnings.append(f"Translation failed: {e}")
            self._stats["manual"] += 1

        return result

    def _translate_complex(self, expr: str) -> str:
        """Translate a complex Talend expression."""
        expr = expr.strip()

        # Handle ternary: cond ? val_true : val_false
        ternary = self._parse_ternary(expr)
        if ternary:
            cond, true_val, false_val = ternary
            cond_t = self._translate_complex(cond)
            true_t = self._translate_complex(true_val)
            false_t = self._translate_complex(false_val)
            return f'F.when({cond_t}, {true_t}).otherwise({false_t})'

        # Handle Java .equals() patterns: "literal".equals(expr)
        expr = self._translate_equals(expr)

        # Handle || (Java string concat or logical OR)
        # Check if it's logical OR (used in conditions)
        if ' || ' in expr and not any(
            f'"{s}".equals' in expr or f"'{s}'.equals" in expr
            for s in re.findall(r'"([^"]*)"', expr)
        ):
            # Logical OR
            parts = self._split_logical(expr, '||')
            if len(parts) > 1:
                translated_parts = [self._translate_complex(p) for p in parts]
                return ' | '.join(f'({p})' for p in translated_parts)

        # Handle && (logical AND)
        if ' && ' in expr:
            parts = self._split_logical(expr, '&&')
            if len(parts) > 1:
                translated_parts = [self._translate_complex(p) for p in parts]
                return ' & '.join(f'({p})' for p in translated_parts)

        # Handle comparison operators
        for op in ['>=', '<=', '!=', '==', '>', '<']:
            if op in expr:
                parts = expr.split(op, 1)
                if len(parts) == 2:
                    left = self._translate_complex(parts[0].strip())
                    right = self._translate_complex(parts[1].strip())
                    py_op = op
                    return f'{left} {py_op} {right}'

        # Handle negation prefix
        if expr.startswith('!') and not expr.startswith('!='):
            inner = expr[1:].strip()
            if inner.startswith('"') and '.equals(' in inner:
                return f'~({self._translate_complex(inner)})'
            return f'~({self._translate_complex(inner)})'

        # Function translations
        expr = self._translate_functions(expr)

        # Column reference
        if _is_simple_ref(expr):
            return _translate_column_ref(expr)

        # String literal
        if (expr.startswith('"') and expr.endswith('"')) or \
           (expr.startswith("'") and expr.endswith("'")):
            return f'F.lit({expr})'

        # Numeric literal
        try:
            float(expr)
            return f'F.lit({expr})'
        except ValueError:
            pass

        # Context variable reference
        if expr.startswith('context.'):
            var_name = expr[8:]
            return f'F.lit(config["{var_name}"])'

        return expr

    def _translate_functions(self, expr: str) -> str:
        """Translate Talend function calls to PySpark."""
        result = expr

        # StringHandling.LEN(x) → F.length(x)
        result = re.sub(
            r'StringHandling\.LEN\(([^)]+)\)',
            lambda m: f'F.length({self._translate_complex(m.group(1))})',
            result,
        )

        # StringHandling.TRIM(x) → F.trim(x)
        result = re.sub(
            r'StringHandling\.TRIM\(([^)]+)\)',
            lambda m: f'F.trim({self._translate_complex(m.group(1))})',
            result,
        )

        # StringHandling.UPCASE(x) → F.upper(x)
        result = re.sub(
            r'StringHandling\.UPCASE\(([^)]+)\)',
            lambda m: f'F.upper({self._translate_complex(m.group(1))})',
            result,
        )

        # StringHandling.DOWNCASE(x) → F.lower(x)
        result = re.sub(
            r'StringHandling\.DOWNCASE\(([^)]+)\)',
            lambda m: f'F.lower({self._translate_complex(m.group(1))})',
            result,
        )

        # StringHandling.LEFT(x, n) → F.substring(x, 1, n)
        result = re.sub(
            r'StringHandling\.LEFT\(([^,]+),\s*(\d+)\)',
            lambda m: f'F.substring({self._translate_complex(m.group(1))}, 1, {m.group(2)})',
            result,
        )

        # CommonDb.replace(x, old, new) → F.regexp_replace(x, old, new)
        m = re.search(r'CommonDb\.replace\(', result)
        if m:
            result = self._translate_replace(result)

        # TalendString.talendTrim(x, char, side) → custom trim
        m = re.search(r'TalendString\.talendTrim\(', result)
        if m:
            result = self._translate_talend_trim(result)

        # TalendDate.formatDate(pattern, date) → F.date_format(date, pattern)
        result = re.sub(
            r'TalendDate\.formatDate\(([^,]+),\s*([^)]+)\)',
            lambda m: f'F.date_format({self._translate_complex(m.group(2))}, {m.group(1).strip()})',
            result,
        )

        # Relational.ISNULL(x) → F.isnull(x)
        result = re.sub(
            r'Relational\.ISNULL\(([^)]+)\)',
            lambda m: f'F.isnull({self._translate_complex(m.group(1))})',
            result,
        )

        return result

    def _translate_replace(self, expr: str) -> str:
        """Handle nested CommonDb.replace calls."""
        pattern = r'CommonDb\.replace\('
        m = re.search(pattern, expr)
        if not m:
            return expr

        start = m.start()
        # Find matching closing paren
        depth = 0
        args_start = m.end()
        i = args_start
        args = []
        arg_start = i
        while i < len(expr):
            if expr[i] == '(':
                depth += 1
            elif expr[i] == ')':
                if depth == 0:
                    args.append(expr[arg_start:i].strip())
                    break
                depth -= 1
            elif expr[i] == ',' and depth == 0:
                args.append(expr[arg_start:i].strip())
                arg_start = i + 1
            i += 1

        if len(args) >= 3:
            target = self._translate_complex(args[0])
            old = args[1].strip()
            new = args[2].strip()
            replacement = f'F.regexp_replace({target}, {old}, {new})'
            result = expr[:start] + replacement + expr[i + 1:]
            # Recurse for nested calls
            if 'CommonDb.replace(' in result:
                result = self._translate_replace(result)
            return result
        return expr

    def _translate_talend_trim(self, expr: str) -> str:
        """Translate TalendString.talendTrim(str, char, side)."""
        pattern = r"TalendString\.talendTrim\("
        m = re.search(pattern, expr)
        if not m:
            return expr

        start = m.start()
        depth = 0
        args_start = m.end()
        i = args_start
        args = []
        arg_start = i
        while i < len(expr):
            if expr[i] == '(':
                depth += 1
            elif expr[i] == ')':
                if depth == 0:
                    args.append(expr[arg_start:i].strip())
                    break
                depth -= 1
            elif expr[i] == ',' and depth == 0:
                args.append(expr[arg_start:i].strip())
                arg_start = i + 1
            i += 1

        if len(args) >= 3:
            target = self._translate_complex(args[0])
            trim_char = args[1].strip().strip("'\"")
            side = args[2].strip()
            # side: 0=both, 1=leading, 2=trailing
            if side == '1':
                replacement = f'F.ltrim({target}, "{trim_char}")'  # leading
            elif side == '2':
                replacement = f'F.rtrim({target}, "{trim_char}")'  # trailing
            else:
                replacement = f'F.trim({target}, "{trim_char}")'
            result = expr[:start] + replacement + expr[i + 1:]
            return result
        return expr

    def _translate_equals(self, expr: str) -> str:
        """Translate Java .equals() to PySpark == comparison."""
        # "literal".equals(col_ref) → col_ref == "literal"
        # Pattern: "value".equals(something) || "value2".equals(something)
        def replace_equals(m):
            literal = m.group(1)
            inner = m.group(2)
            translated_inner = self._translate_complex(inner)
            return f'({translated_inner} == "{literal}")'

        # Handle !"str".equals(x) - negated
        expr = re.sub(
            r'!"([^"]+)"\.equals\(([^)]+)\)',
            lambda m: f'({self._translate_complex(m.group(2))} != "{m.group(1)}")',
            expr,
        )

        # Handle "str".equals(x)
        expr = re.sub(
            r'"([^"]+)"\.equals\(([^)]+)\)',
            replace_equals,
            expr,
        )

        return expr

    def _parse_ternary(self, expr: str) -> tuple[str, str, str] | None:
        """Parse a ternary expression: cond ? true : false.

        Handles nested ternaries and function calls with balanced parens.
        """
        depth = 0
        q_pos = -1
        in_string = False
        string_char = None

        i = 0
        while i < len(expr):
            c = expr[i]

            # Track string literals
            if c in ('"', "'") and (i == 0 or expr[i - 1] != '\\'):
                if in_string and c == string_char:
                    in_string = False
                elif not in_string:
                    in_string = True
                    string_char = c
            elif not in_string:
                if c == '(':
                    depth += 1
                elif c == ')':
                    depth -= 1
                elif c == '?' and depth == 0:
                    q_pos = i
                    break
            i += 1

        if q_pos < 0:
            return None

        cond = expr[:q_pos].strip()

        # Now find the matching : for this ?
        rest = expr[q_pos + 1:]
        depth = 0
        in_string = False
        colon_pos = -1
        i = 0
        while i < len(rest):
            c = rest[i]
            if c in ('"', "'") and (i == 0 or rest[i - 1] != '\\'):
                if in_string and c == string_char:
                    in_string = False
                elif not in_string:
                    in_string = True
                    string_char = c
            elif not in_string:
                if c == '(':
                    depth += 1
                elif c == ')':
                    depth -= 1
                elif c == '?' and depth == 0:
                    # Nested ternary - skip its : by finding it
                    pass
                elif c == ':' and depth == 0:
                    colon_pos = i
                    break
            i += 1

        if colon_pos < 0:
            return None

        true_val = rest[:colon_pos].strip()
        false_val = rest[colon_pos + 1:].strip()

        return cond, true_val, false_val

    def _split_logical(self, expr: str, op: str) -> list[str]:
        """Split on logical operator respecting parens and strings."""
        parts = []
        depth = 0
        in_string = False
        string_char = None
        current_start = 0
        i = 0
        while i < len(expr):
            c = expr[i]
            if c in ('"', "'") and (i == 0 or expr[i - 1] != '\\'):
                if in_string and c == string_char:
                    in_string = False
                elif not in_string:
                    in_string = True
                    string_char = c
            elif not in_string:
                if c == '(':
                    depth += 1
                elif c == ')':
                    depth -= 1
                elif depth == 0 and expr[i:i + len(op)] == op:
                    # Check it's surrounded by spaces
                    if (i > 0 and expr[i - 1] == ' ') and \
                       (i + len(op) < len(expr) and expr[i + len(op)] == ' '):
                        parts.append(expr[current_start:i].strip())
                        current_start = i + len(op)
                        i += len(op)
                        continue
            i += 1
        parts.append(expr[current_start:].strip())
        return parts

    def translate_filter(self, expr: str) -> str:
        """Translate a tMap expression filter to PySpark filter condition."""
        cleaned = _strip_comments(expr)
        if not cleaned:
            return "True"
        try:
            return self._translate_complex(cleaned)
        except Exception:
            return f'# TODO: translate filter: {cleaned}'


def translated_confidence(original: str, translated: str) -> float:
    """Estimate confidence of a translation."""
    if 'TODO' in translated:
        return 0.0
    if 'F.lit("TODO' in translated:
        return 0.0

    confidence = 1.0

    # Untranslated Talend functions
    talend_funcs = ['StringHandling.', 'CommonDb.', 'TalendString.', 'TalendDate.', 'Relational.']
    for func in talend_funcs:
        if func in translated:
            confidence -= 0.3

    # Remaining Java patterns
    if '.equals(' in translated:
        confidence -= 0.2
    if 'context.' in translated and 'config[' not in translated:
        confidence -= 0.1

    return max(0.0, min(1.0, confidence))


def _escape(s: str) -> str:
    """Escape string for embedding in code."""
    return s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
