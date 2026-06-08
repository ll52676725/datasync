"""
data_transformer.py — 数据异构转换引擎

为同步任务提供三种数据转换能力：
1. 条件过滤（Condition Filter）：按 SQL WHERE 风格表达式过滤行
2. 行级脚本转换（Row Script）：通过 Python 脚本逐行转换/过滤
3. 聚合计算（Aggregation）：按 GROUP BY 分组 + SUM/COUNT/AVG/MAX/MIN

采用管道式设计，转换顺序：
    源行 → [条件过滤] → [行级脚本] → [聚合收集] → 目标行

对于聚合同步，采用"两阶段"策略：
    阶段一：收集所有行并应用前两级过滤
    阶段二：执行聚合计算并批量输出结果
"""

import re
import ast
import logging
import operator
from typing import List, Dict, Any, Optional, Tuple, Callable
from collections import defaultdict

logger = logging.getLogger(__name__)


# ====================================================================== #
#                       1. 条件过滤引擎 (Condition Filter)
# ====================================================================== #

class ConditionFilterEngine:
    """
    SQL WHERE 风格的条件表达式过滤引擎。

    支持的运算符：
        比较：=, !=, <>, >, <, >=, <=
        逻辑：AND, OR, NOT (不区分大小写)
        集合：IN (val1, val2, ...), NOT IN (...)
        模糊：LIKE 'pattern', NOT LIKE 'pattern'
        空值：IS NULL, IS NOT NULL
        括号分组

    示例：
        "age >= 18 AND status = 'active'"
        "category IN ('A', 'B') AND price < 100.0"
        "name LIKE '张%' AND email IS NOT NULL"
    """

    _OP_MAP = {
        '=': operator.eq,
        '==': operator.eq,
        '!=': operator.ne,
        '<>': operator.ne,
        '>': operator.gt,
        '<': operator.lt,
        '>=': operator.ge,
        '<=': operator.le,
    }

    def __init__(self, expression: str):
        self.expression = expression.strip()
        self._tokens = self._tokenize(self.expression)
        self._pos = 0

    # ---- 词法分析 ---- #
    @staticmethod
    def _tokenize(expr: str) -> List[Tuple[str, Any]]:
        tokens = []
        i = 0
        n = len(expr)
        while i < n:
            c = expr[i]

            # 跳过空白
            if c.isspace():
                i += 1
                continue

            # 字符串字面量 (单引号)
            if c == "'":
                j = i + 1
                val_chars = []
                while j < n:
                    if expr[j] == "'" and j + 1 < n and expr[j + 1] == "'":
                        val_chars.append("'")
                        j += 2
                    elif expr[j] == "'":
                        break
                    else:
                        val_chars.append(expr[j])
                        j += 1
                tokens.append(('STRING', ''.join(val_chars)))
                i = j + 1
                continue

            # 数字字面量
            if c.isdigit() or (c == '.' and i + 1 < n and expr[i + 1].isdigit()):
                j = i
                has_dot = False
                while j < n and (expr[j].isdigit() or (expr[j] == '.' and not has_dot)):
                    if expr[j] == '.':
                        has_dot = True
                    j += 1
                num_str = expr[i:j]
                tokens.append(('NUMBER', float(num_str) if has_dot else int(num_str)))
                i = j
                continue

            # 标识符 (列名) 或 关键字
            if c.isalpha() or c == '_':
                j = i
                while j < n and (expr[j].isalnum() or expr[j] == '_'):
                    j += 1
                word = expr[i:j]
                upper = word.upper()
                if upper in ('AND', 'OR', 'NOT', 'IN', 'LIKE', 'IS', 'NULL', 'TRUE', 'FALSE'):
                    tokens.append(('KW', upper))
                else:
                    tokens.append(('IDENT', word))
                i = j
                continue

            # 运算符
            if c in '=<>!':
                two = expr[i:i + 2]
                if two in ('!=', '<>', '>=', '<=', '=='):
                    tokens.append(('OP', two))
                    i += 2
                    continue
                if c in '=<>':
                    tokens.append(('OP', c))
                    i += 1
                    continue

            # 括号 / 逗号
            if c in '(),':
                tokens.append(('PUNCT', c))
                i += 1
                continue

            raise ValueError(f"条件表达式 '{expr}' 在位置 {i} 处存在非法字符: '{c}'")

        return tokens

    def _peek(self) -> Optional[Tuple[str, Any]]:
        return self._tokens[self._pos] if self._pos < len(self._tokens) else None

    def _eat(self, expected_type: str, expected_value: Any = None) -> Tuple[str, Any]:
        tok = self._peek()
        if tok is None:
            raise ValueError(f"条件表达式意外结束，期望 {expected_type}")
        if tok[0] != expected_type or (expected_value is not None and tok[1] != expected_value):
            raise ValueError(f"条件表达式解析错误，在位置 {self._pos} 期望 {expected_type}"
                             f"{f'={expected_value}' if expected_value else ''}，实际 {tok}")
        self._pos += 1
        return tok

    # ---- 语法分析 (递归下降) ---- #
    def evaluate(self, row: Dict[str, Any]) -> bool:
        """对单行数据求值，返回 True 表示保留该行。"""
        if not self._tokens:
            return True
        self._pos = 0
        result = self._parse_or(row)
        if self._pos < len(self._tokens):
            raise ValueError(f"条件表达式在位置 {self._pos} 处有多余的 token: {self._tokens[self._pos:]}")
        return bool(result)

    def _parse_or(self, row):
        left = self._parse_and(row)
        while self._peek() and self._peek() == ('KW', 'OR'):
            self._eat('KW', 'OR')
            right = self._parse_and(row)
            left = bool(left) or bool(right)
        return left

    def _parse_and(self, row):
        left = self._parse_not(row)
        while self._peek() and self._peek() == ('KW', 'AND'):
            self._eat('KW', 'AND')
            right = self._parse_not(row)
            left = bool(left) and bool(right)
        return left

    def _parse_not(self, row):
        if self._peek() == ('KW', 'NOT'):
            self._eat('KW', 'NOT')
            return not bool(self._parse_not(row))
        return self._parse_primary(row)

    def _parse_primary(self, row):
        tok = self._peek()
        if tok is None:
            raise ValueError("条件表达式意外结束")

        # 括号分组
        if tok == ('PUNCT', '('):
            self._eat('PUNCT', '(')
            val = self._parse_or(row)
            self._eat('PUNCT', ')')
            return val

        # NOT ( expr ) 已在 _parse_not 中处理，此处处理普通表达式
        return self._parse_comparison(row)

    def _parse_comparison(self, row):
        """解析比较表达式：列 运算符 值/集合/NULL/LIKE"""
        left_tok = self._peek()
        if left_tok is None or left_tok[0] != 'IDENT':
            raise ValueError(f"条件表达式期望列名，实际: {left_tok}")
        self._eat('IDENT')
        col_name = left_tok[1]

        tok = self._peek()
        if tok is None:
            raise ValueError(f"条件表达式在列 '{col_name}' 后期望运算符")

        col_value = row.get(col_name)

        # IS [NOT] NULL
        if tok == ('KW', 'IS'):
            self._eat('KW', 'IS')
            negate = False
            if self._peek() == ('KW', 'NOT'):
                self._eat('KW', 'NOT')
                negate = True
            self._eat('KW', 'NULL')
            is_null = col_value is None or (isinstance(col_value, str) and col_value == '')
            return (not is_null) if negate else is_null

        # [NOT] IN (...)
        if tok == ('KW', 'IN'):
            self._eat('KW', 'IN')
            self._eat('PUNCT', '(')
            values = []
            while True:
                item_tok = self._peek()
                if item_tok is None:
                    raise ValueError("IN 列表未闭合")
                if item_tok[0] in ('STRING', 'NUMBER'):
                    self._eat(item_tok[0])
                    values.append(item_tok[1])
                else:
                    raise ValueError(f"IN 列表中只允许常量值，实际: {item_tok}")
                nxt = self._peek()
                if nxt == ('PUNCT', ','):
                    self._eat('PUNCT', ',')
                    continue
                break
            self._eat('PUNCT', ')')
            return col_value in values

        if tok == ('KW', 'NOT') and self._pos + 1 < len(self._tokens) \
                and self._tokens[self._pos + 1] == ('KW', 'IN'):
            self._eat('KW', 'NOT')
            self._eat('KW', 'IN')
            self._eat('PUNCT', '(')
            values = []
            while True:
                item_tok = self._peek()
                if item_tok is None:
                    raise ValueError("NOT IN 列表未闭合")
                if item_tok[0] in ('STRING', 'NUMBER'):
                    self._eat(item_tok[0])
                    values.append(item_tok[1])
                else:
                    raise ValueError(f"NOT IN 列表中只允许常量值，实际: {item_tok}")
                nxt = self._peek()
                if nxt == ('PUNCT', ','):
                    self._eat('PUNCT', ',')
                    continue
                break
            self._eat('PUNCT', ')')
            return col_value not in values

        # [NOT] LIKE 'pattern'
        if tok == ('KW', 'LIKE'):
            self._eat('KW', 'LIKE')
            pat_tok = self._eat('STRING')
            return self._like_match(str(col_value) if col_value is not None else '', pat_tok[1])

        if tok == ('KW', 'NOT') and self._pos + 1 < len(self._tokens) \
                and self._tokens[self._pos + 1] == ('KW', 'LIKE'):
            self._eat('KW', 'NOT')
            self._eat('KW', 'LIKE')
            pat_tok = self._eat('STRING')
            return not self._like_match(str(col_value) if col_value is not None else '', pat_tok[1])

        # 普通比较运算符
        if tok and tok[0] == 'OP':
            self._eat('OP')
            op = tok[1]
            rhs_tok = self._peek()
            if rhs_tok is None or rhs_tok[0] not in ('STRING', 'NUMBER'):
                raise ValueError(f"运算符 {op} 右侧只允许常量值，实际: {rhs_tok}")
            self._eat(rhs_tok[0])
            rhs = rhs_tok[1]

            try:
                lhs = col_value
                # 类型兼容：字符串 vs 数字 自动转换
                if isinstance(lhs, (int, float)) and isinstance(rhs, str):
                    try:
                        rhs = type(lhs)(rhs)
                    except ValueError:
                        pass
                elif isinstance(rhs, (int, float)) and isinstance(lhs, str):
                    try:
                        lhs = type(rhs)(lhs)
                    except ValueError:
                        pass
                return self._OP_MAP[op](lhs, rhs)
            except Exception as e:
                logger.warning("条件比较失败 '%s %s %s' (行值=%r): %s", col_name, op, rhs, col_value, e)
                return False

        raise ValueError(f"列 '{col_name}' 后期望运算符，实际: {tok}")

    @staticmethod
    def _like_match(text: str, pattern: str) -> bool:
        """简单的 SQL LIKE 匹配：% 任意字符，_ 单个字符。"""
        regex_parts = []
        i = 0
        while i < len(pattern):
            ch = pattern[i]
            if ch == '%':
                regex_parts.append('.*')
            elif ch == '_':
                regex_parts.append('.')
            elif ch in '.^$*+?{}[]|()\\':
                regex_parts.append('\\' + ch)
            else:
                regex_parts.append(re.escape(ch))
            i += 1
        return re.fullmatch(''.join(regex_parts), text, re.DOTALL) is not None


# ====================================================================== #
#                      2. 行级脚本转换引擎 (Row Script)
# ====================================================================== #

class RowScriptEngine:
    """
    安全沙箱执行用户自定义的 Python 行级转换脚本。

    用户脚本需要定义以下函数中的一个或两个：

        def filter_row(row: dict) -> bool:
            # 返回 False 跳过该行，返回 True 保留该行
            return row.get('status') == 'active'

        def transform_row(row: dict) -> dict:
            # 对行进行字段转换/新增/删除
            row['full_name'] = row['first_name'] + ' ' + row['last_name']
            return row

    安全限制：
    - 仅提供有限的 builtins (len/str/int/float/bool/max/min/sum/round/sorted/abs)
    - 禁止 __builtins__/__import__/eval/exec/open 等危险调用
    - 脚本执行设置最大递归和超时（通过上层线程控制）
    """

    _SAFE_BUILTINS = {
        'len': len, 'str': str, 'int': int, 'float': float, 'bool': bool,
        'max': max, 'min': min, 'sum': sum, 'round': round,
        'sorted': sorted, 'abs': abs, 'list': list, 'dict': dict,
        'tuple': tuple, 'set': set, 'True': True, 'False': False, 'None': None,
        'isinstance': isinstance, 'type': type, 'ValueError': ValueError,
        'KeyError': KeyError, 'Exception': Exception,
    }

    def __init__(self, script_code: str):
        self.script_code = script_code
        self._filter_fn = None
        self._transform_fn = None
        self._static_ast_scan()  # 先做 AST 静态安全扫描
        self._compile()

    _FORBIDDEN_CALL_NAMES = {
        'eval', 'exec', '__import__', 'open', 'compile', 'input',
        'getattr', 'setattr', 'delattr', 'hasattr',
        'globals', 'locals', 'vars', 'dir',
        'breakpoint', 'memoryview', 'super', 'classmethod', 'staticmethod',
        'property', 'type',  # type 允许 (白名单已包含), 但这里只做二次拦
    }

    _FORBIDDEN_CALL_NAMES.remove('type')  # type 是安全的且在白名单中

    _DANGEROUS_ATTR_PREFIXES = ('__',)

    def _static_ast_scan(self):
        """静态 AST 扫描，拒绝包含危险调用或 __xxx__ 属性访问的脚本。"""
        try:
            tree = ast.parse(self.script_code)
        except SyntaxError as e:
            raise ValueError(f"脚本语法错误 (行 {e.lineno}): {e.msg}")

        forbidden_hits = []
        dangerous_attr_hits = []

        for node in ast.walk(tree):
            # ---- 检测危险函数调用 ---- #
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    name = func.id
                elif isinstance(func, ast.Attribute):
                    # 比如 obj.eval(...) 也拦住
                    name = func.attr
                else:
                    continue
                if name in self._FORBIDDEN_CALL_NAMES:
                    forbidden_hits.append(f"{name}(...) @ 行 {node.lineno}")

            # ---- 检测危险属性访问 (如 __builtins__、row.__class__.__bases__) ---- #
            elif isinstance(node, ast.Attribute):
                attr = node.attr
                if attr.startswith('__') and attr.endswith('__'):
                    # __xxx__ 魔法方法/属性访问，极可能是沙箱逃逸
                    dangerous_attr_hits.append(f".{attr} @ 行 {node.lineno}")
                elif attr in ('__dict__', '__class__', '__bases__', '__mro__',
                              '__subclasses__', '__globals__', '__closure__',
                              '__code__', '__func__'):
                    dangerous_attr_hits.append(f".{attr} @ 行 {node.lineno}")

            # ---- 禁止 import 语句 ---- #
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                forbidden_hits.append(
                    f"{'import' if isinstance(node, ast.Import) else 'from ... import'} @ 行 {node.lineno}"
                )

        if forbidden_hits:
            raise ValueError(
                "脚本包含禁止的操作: " + "; ".join(forbidden_hits)
                + "。安全沙箱仅允许有限的内置函数，文件/网络/eval 等危险操作被禁用。"
            )
        if dangerous_attr_hits:
            raise ValueError(
                "脚本包含可疑的属性访问: " + "; ".join(dangerous_attr_hits)
                + "。禁止访问 __xxx__ 魔法属性或 __dict__/__class__ 等逃逸点。"
            )

    def _compile(self):
        globals_env = {'__builtins__': self._SAFE_BUILTINS}
        try:
            exec(compile(self.script_code, '<row_script>', 'exec'), globals_env)
        except SyntaxError as e:
            raise ValueError(f"脚本语法错误 (行 {e.lineno}): {e.msg}")

        self._filter_fn = globals_env.get('filter_row')
        self._transform_fn = globals_env.get('transform_row')

        if self._filter_fn is None and self._transform_fn is None:
            raise ValueError(
                "脚本必须定义 filter_row(row) 或 transform_row(row) 函数，"
                "或两者都定义。"
            )

    def apply(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        对单行应用脚本。

        Returns:
            过滤掉 → None；保留并（可能）转换 → 新行字典
        """
        try:
            # 1. filter 阶段
            if self._filter_fn is not None:
                result = self._filter_fn(dict(row))
                if not bool(result):
                    return None

            # 2. transform 阶段
            if self._transform_fn is not None:
                new_row = self._transform_fn(dict(row))
                if not isinstance(new_row, dict):
                    raise ValueError("transform_row 必须返回 dict 类型")
                return new_row

            return row
        except Exception as e:
            logger.error("行级脚本执行失败 (行=%r): %s", row, e)
            raise


# ====================================================================== #
#                       3. 聚合引擎 (Aggregation)
# ====================================================================== #

class AggregationEngine:
    """
    分组聚合引擎。

    配置示例：
    {
        "groupBy": ["category", "region"],
        "metrics": [
            {"source": "amount", "target": "total_amount", "func": "SUM"},
            {"source": "id",     "target": "order_count",  "func": "COUNT"},
            {"source": "price",  "target": "avg_price",    "func": "AVG"},
            {"source": "amount", "target": "max_amount",   "func": "MAX"},
            {"source": "amount", "target": "min_amount",   "func": "MIN"},
        ]
    }
    """

    SUPPORTED_FUNCS = {'SUM', 'COUNT', 'AVG', 'MAX', 'MIN'}

    def __init__(self, group_by: List[str], metrics: List[Dict[str, str]]):
        self.group_by = [c.strip() for c in (group_by or [])]
        self.metrics = metrics or []

        if not self.group_by:
            logger.warning("聚合未指定 GROUP BY 列，将对全表聚合为一行")

        for m in self.metrics:
            func = m.get('func', '').upper()
            if func not in self.SUPPORTED_FUNCS:
                raise ValueError(f"不支持的聚合函数: {func}，支持: {sorted(self.SUPPORTED_FUNCS)}")
            if func != 'COUNT' and not m.get('source'):
                raise ValueError(f"聚合函数 {func} 需要指定 source 列")
            if not m.get('target'):
                raise ValueError("聚合指标需要指定 target 列名")

        # 状态：group_key → {metric_target → [累计值, 计数(用于AVG), ...]}
        self._groups: Dict[Tuple, Dict[str, List[Any]]] = defaultdict(lambda: defaultdict(list))

    def add_row(self, row: Dict[str, Any]) -> None:
        """收集一行数据用于后续聚合。"""
        key = tuple(row.get(c) for c in self.group_by) if self.group_by else tuple()
        for metric in self.metrics:
            func = metric['func'].upper()
            target = metric['target']
            source = metric.get('source')

            if func == 'COUNT':
                # COUNT(*) 或 COUNT(source) - 非空计数
                if source:
                    val = row.get(source)
                    self._groups[key][target].append(0 if val is None else 1)
                else:
                    self._groups[key][target].append(1)
            else:
                val = row.get(source)
                if val is None:
                    continue
                try:
                    num_val = float(val)
                except (TypeError, ValueError):
                    logger.warning("聚合值无法转为数值: %s=%r (func=%s)", source, val, func)
                    continue
                self._groups[key][target].append(num_val)

    def finalize(self) -> List[Dict[str, Any]]:
        """
        执行聚合计算，返回聚合结果行列表。

        每行包含：
            - GROUP BY 各列的值
            - 每个指标的计算结果列 (target)
        """
        results = []
        for key, metrics_map in self._groups.items():
            row = {}
            # 填充 GROUP BY 列
            for i, col in enumerate(self.group_by):
                row[col] = key[i]

            # 计算各聚合指标
            for metric in self.metrics:
                func = metric['func'].upper()
                target = metric['target']
                values = metrics_map.get(target, [])

                if func == 'COUNT':
                    row[target] = int(sum(values))
                elif func == 'SUM':
                    s = sum(values)
                    row[target] = int(s) if all(isinstance(v, int) or (isinstance(v, float) and v.is_integer()) for v in values) else s
                elif func == 'AVG':
                    row[target] = (sum(values) / len(values)) if values else 0.0
                elif func == 'MAX':
                    row[target] = max(values) if values else None
                elif func == 'MIN':
                    row[target] = min(values) if values else None
                else:
                    row[target] = None

            results.append(row)

        logger.info("聚合完成: %d 个分组, %d 个指标", len(results), len(self.metrics))
        return results

    @property
    def output_columns(self) -> List[str]:
        """聚合输出列列表。"""
        cols = list(self.group_by)
        for m in self.metrics:
            cols.append(m['target'])
        return cols


# ====================================================================== #
#                         4. 转换管道 (Pipeline)
# ====================================================================== #

class TransformPipeline:
    """
    数据转换管道，协调三种转换能力按序执行。

    支持两种执行模式：
    1. **流式模式** (streaming_mode=True)：
       - 逐行处理 → 逐行输出
       - 仅支持条件过滤 + 行级脚本（无聚合）
       - 适用于大表、无聚合的场景

    2. **聚合模式** (streaming_mode=False)：
       - 逐行收集 → 聚合 → 批量输出
       - 支持全部三种转换
       - 适用于需要聚合同步的场景（内存占用取决于行数）
    """

    def __init__(self, rules: Dict[str, Any], source_columns: Optional[List[str]] = None):
        """
        Args:
            rules: 转换规则字典，结构：
                {
                    "condition": "age >= 18",                  # 条件过滤表达式
                    "rowScript": "def filter_row(row):...",    # 行级脚本
                    "aggregation": {                           # 聚合配置
                        "groupBy": ["category"],
                        "metrics": [...]
                    }
                }
            source_columns: 源表列名列表（用于校验）
        """
        self.rules = rules or {}
        self.condition_engine = None
        self.row_script_engine = None
        self.aggregation_engine = None
        self._source_columns = source_columns or []
        self._is_aggregation_mode = False
        self._aggregation_output_rows: List[Dict[str, Any]] = []
        self._aggregation_finished = False

        # 初始化各级转换引擎
        cond_expr = (self.rules.get('condition') or '').strip()
        if cond_expr:
            try:
                self.condition_engine = ConditionFilterEngine(cond_expr)
                logger.debug("转换管道: 启用条件过滤: %s", cond_expr[:80])
            except Exception as e:
                raise ValueError(f"条件表达式解析失败: {e}")

        script_code = (self.rules.get('rowScript') or '').strip()
        if script_code:
            try:
                self.row_script_engine = RowScriptEngine(script_code)
                logger.debug("转换管道: 启用行级脚本")
            except Exception as e:
                raise ValueError(f"行级脚本编译失败: {e}")

        agg_cfg = self.rules.get('aggregation') or {}
        if agg_cfg and agg_cfg.get('metrics'):
            try:
                self.aggregation_engine = AggregationEngine(
                    agg_cfg.get('groupBy', []),
                    agg_cfg.get('metrics', [])
                )
                self._is_aggregation_mode = True
                logger.debug("转换管道: 启用聚合模式 (groupBy=%s, metrics=%d)",
                             self.aggregation_engine.group_by, len(self.aggregation_engine.metrics))
            except Exception as e:
                raise ValueError(f"聚合配置错误: {e}")

    # ---- 属性 ---- #
    @property
    def streaming_mode(self) -> bool:
        return not self._is_aggregation_mode

    @property
    def has_any_transform(self) -> bool:
        return any([
            self.condition_engine is not None,
            self.row_script_engine is not None,
            self.aggregation_engine is not None,
        ])

    @property
    def output_columns(self) -> List[str]:
        if self._is_aggregation_mode and self.aggregation_engine:
            return self.aggregation_engine.output_columns
        return self._source_columns

    # ---- 执行接口 ---- #
    def process_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理单行数据。

        流式模式：返回过滤/转换后的行，或 None 表示过滤掉
        聚合模式：始终返回 None（先收集，由 finalize 输出）
        """
        # 1. 条件过滤
        if self.condition_engine is not None:
            try:
                if not self.condition_engine.evaluate(row):
                    return None
            except Exception as e:
                logger.error("条件过滤执行失败 (行=%r): %s", row, e)
                return None

        # 2. 行级脚本
        if self.row_script_engine is not None:
            row = self.row_script_engine.apply(row)
            if row is None:
                return None

        # 3. 聚合 (收集阶段)
        if self._is_aggregation_mode:
            self.aggregation_engine.add_row(row)
            return None

        return row

    def finalize(self) -> List[Dict[str, Any]]:
        """
        结束管道，输出最终结果。

        流式模式：返回空列表
        聚合模式：返回所有聚合结果行
        """
        if self._is_aggregation_mode and not self._aggregation_finished:
            self._aggregation_output_rows = self.aggregation_engine.finalize()
            self._aggregation_finished = True
        return self._aggregation_output_rows


# ====================================================================== #
#                       5. 便捷工厂函数
# ====================================================================== #

def build_pipeline_for_table(
    sync_config_sync_section: Dict[str, Any],
    table_name: str,
    source_columns: Optional[List[str]] = None,
) -> Optional[TransformPipeline]:
    """
    根据 sync 配置节构造指定表的转换管道。

    配置结构（兼容现有 fieldMappings）：
    {
        "fieldMappings": {
            "orders": {
                "targetTable": "orders_target",
                "mappings": [...],
                "transformRules": {                       ← 新增
                    "condition": "amount > 100",
                    "rowScript": "...",
                    "aggregation": {
                        "groupBy": ["user_id"],
                        "metrics": [
                            {"source":"amount","target":"total","func":"SUM"}
                        ]
                    }
                }
            }
        },
        "transformRules": {                             ← 全局回退
            "orders": { ... }
        }
    }
    """
    rules = None
    fm = sync_config_sync_section.get('fieldMappings', {}) or {}
    entry = fm.get(table_name, {}) if isinstance(fm, dict) else {}
    rules = entry.get('transformRules')

    if not rules:
        global_rules = sync_config_sync_section.get('transformRules', {}) or {}
        rules = global_rules.get(table_name)

    if not rules:
        return None

    if not any(rules.get(k) for k in ('condition', 'rowScript', 'aggregation')):
        return None

    return TransformPipeline(rules, source_columns)


def validate_transform_rules(rules: Dict[str, Any]) -> Dict[str, Any]:
    """
    校验转换规则，返回校验结果字典。

    Returns:
        {"success": True/False, "errors": [...], "warnings": [...]}
    """
    errors = []
    warnings = []

    if not isinstance(rules, dict):
        return {"success": False, "errors": ["规则必须是对象"], "warnings": []}

    # 条件表达式校验
    condition = (rules.get('condition') or '').strip()
    if condition:
        try:
            eng = ConditionFilterEngine(condition)
            eng.evaluate({})  # 用空行试解析（不代表运行正确）
        except Exception as e:
            errors.append(f"条件表达式错误: {e}")

    # 脚本校验
    script = (rules.get('rowScript') or '').strip()
    if script:
        try:
            RowScriptEngine(script)
        except Exception as e:
            errors.append(f"行级脚本错误: {e}")

    # 聚合校验
    agg = rules.get('aggregation') or {}
    if agg and (agg.get('metrics') or agg.get('groupBy')):
        try:
            AggregationEngine(agg.get('groupBy', []), agg.get('metrics', []))
        except Exception as e:
            errors.append(f"聚合配置错误: {e}")

    return {
        "success": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }
