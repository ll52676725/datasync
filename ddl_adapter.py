"""
ddl_adapter.py — DDL 跨数据库适配层

提供跨数据库类型的 DDL 转换能力，解决 MySQL / DB2 / Oracle 之间
数据类型不兼容、语法差异等问题，实现真正的跨库 DDL 同步。

核心设计：
1. 通用表结构元数据模型（中间表示）
2. 数据类型映射系统（标准类型 + 数据库特定类型映射）
3. 各数据库 DDL 生成器（从中间模型生成目标库 DDL）
"""

import logging
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# ====================================================================== #
#                    通用数据类型定义（标准中间类型）
# ====================================================================== #

class StandardDataType:
    """标准数据类型枚举（中间表示）。
    
    所有数据库的特定类型都映射到这些标准类型，
    然后再从标准类型映射到目标数据库的特定类型。
    """
    # 整数类型
    TINYINT = "TINYINT"
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    
    # 精确数值类型
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    
    # 浮点类型
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    REAL = "REAL"
    
    # 字符串类型
    CHAR = "CHAR"
    VARCHAR = "VARCHAR"
    LONGVARCHAR = "LONGVARCHAR"
    CLOB = "CLOB"
    TEXT = "TEXT"
    
    # Unicode 字符串类型
    NCHAR = "NCHAR"
    NVARCHAR = "NVARCHAR"
    NCLOB = "NCLOB"
    
    # 二进制类型
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    LONGVARBINARY = "LONGVARBINARY"
    BLOB = "BLOB"
    
    # 日期时间类型
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    DATETIME = "DATETIME"
    
    # 布尔类型
    BOOLEAN = "BOOLEAN"
    BIT = "BIT"
    
    # 其他
    JSON = "JSON"
    XML = "XML"
    UUID = "UUID"


# ====================================================================== #
#                         元数据数据类
# ====================================================================== #

@dataclass
class ColumnSchema:
    """列的元数据描述（中间表示）。"""
    name: str
    data_type: str  # StandardDataType 中的值
    length: Optional[int] = None  # 字符长度或精度
    precision: Optional[int] = None  # 数值总位数
    scale: Optional[int] = None  # 小数位数
    nullable: bool = True
    default_value: Optional[str] = None
    is_primary_key: bool = False
    comment: Optional[str] = None
    auto_increment: bool = False
    original_db_type: Optional[str] = None  # 原始数据库类型，用于调试


@dataclass
class TableSchema:
    """表的元数据描述（中间表示）。"""
    table_name: str
    columns: List[ColumnSchema] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    comment: Optional[str] = None
    source_db_type: Optional[str] = None  # 来源数据库类型

    def get_column(self, name: str) -> Optional[ColumnSchema]:
        for col in self.columns:
            if col.name.upper() == name.upper():
                return col
        return None


# ====================================================================== #
#                         数据类型映射系统
# ====================================================================== #

class DataTypeMapper:
    """数据类型映射器：负责各数据库类型与标准类型之间的双向转换。"""

    # MySQL 类型 → 标准类型 映射
    MYSQL_TO_STANDARD: Dict[str, Tuple[str, Dict[str, Any]]] = {
        # 整数
        "TINYINT": (StandardDataType.TINYINT, {}),
        "TINYINT UNSIGNED": (StandardDataType.SMALLINT, {}),
        "SMALLINT": (StandardDataType.SMALLINT, {}),
        "SMALLINT UNSIGNED": (StandardDataType.INTEGER, {}),
        "MEDIUMINT": (StandardDataType.INTEGER, {}),
        "MEDIUMINT UNSIGNED": (StandardDataType.INTEGER, {}),
        "INT": (StandardDataType.INTEGER, {}),
        "INTEGER": (StandardDataType.INTEGER, {}),
        "INT UNSIGNED": (StandardDataType.BIGINT, {}),
        "INTEGER UNSIGNED": (StandardDataType.BIGINT, {}),
        "BIGINT": (StandardDataType.BIGINT, {}),
        "BIGINT UNSIGNED": (StandardDataType.DECIMAL, {"precision": 20, "scale": 0}),
        
        # 精确数值
        "DECIMAL": (StandardDataType.DECIMAL, {}),
        "DEC": (StandardDataType.DECIMAL, {}),
        "NUMERIC": (StandardDataType.NUMERIC, {}),
        "FIXED": (StandardDataType.DECIMAL, {}),
        
        # 浮点
        "FLOAT": (StandardDataType.FLOAT, {}),
        "DOUBLE": (StandardDataType.DOUBLE, {}),
        "DOUBLE PRECISION": (StandardDataType.DOUBLE, {}),
        "REAL": (StandardDataType.REAL, {}),
        
        # 字符串
        "CHAR": (StandardDataType.CHAR, {}),
        "VARCHAR": (StandardDataType.VARCHAR, {}),
        "TINYTEXT": (StandardDataType.TEXT, {}),
        "TEXT": (StandardDataType.TEXT, {}),
        "MEDIUMTEXT": (StandardDataType.LONGVARCHAR, {}),
        "LONGTEXT": (StandardDataType.CLOB, {}),
        "ENUM": (StandardDataType.VARCHAR, {"length": 255}),
        "SET": (StandardDataType.VARCHAR, {"length": 255}),
        
        # Unicode 字符串
        "NCHAR": (StandardDataType.NCHAR, {}),
        "NVARCHAR": (StandardDataType.NVARCHAR, {}),
        
        # 二进制
        "BINARY": (StandardDataType.BINARY, {}),
        "VARBINARY": (StandardDataType.VARBINARY, {}),
        "TINYBLOB": (StandardDataType.BLOB, {}),
        "BLOB": (StandardDataType.BLOB, {}),
        "MEDIUMBLOB": (StandardDataType.BLOB, {}),
        "LONGBLOB": (StandardDataType.BLOB, {}),
        
        # 日期时间
        "DATE": (StandardDataType.DATE, {}),
        "TIME": (StandardDataType.TIME, {}),
        "DATETIME": (StandardDataType.DATETIME, {}),
        "TIMESTAMP": (StandardDataType.TIMESTAMP, {}),
        "YEAR": (StandardDataType.INTEGER, {}),
        
        # 布尔
        "BOOLEAN": (StandardDataType.BOOLEAN, {}),
        "BOOL": (StandardDataType.BOOLEAN, {}),
        "BIT": (StandardDataType.BIT, {}),
        
        # 其他
        "JSON": (StandardDataType.JSON, {}),
        "XML": (StandardDataType.XML, {}),
    }

    # DB2 类型 → 标准类型 映射
    DB2_TO_STANDARD: Dict[str, Tuple[str, Dict[str, Any]]] = {
        # 整数
        "SMALLINT": (StandardDataType.SMALLINT, {}),
        "INTEGER": (StandardDataType.INTEGER, {}),
        "INT": (StandardDataType.INTEGER, {}),
        "BIGINT": (StandardDataType.BIGINT, {}),
        
        # 精确数值
        "DECIMAL": (StandardDataType.DECIMAL, {}),
        "DEC": (StandardDataType.DECIMAL, {}),
        "NUMERIC": (StandardDataType.NUMERIC, {}),
        
        # 浮点
        "REAL": (StandardDataType.REAL, {}),
        "FLOAT": (StandardDataType.FLOAT, {}),
        "DOUBLE": (StandardDataType.DOUBLE, {}),
        "DOUBLE PRECISION": (StandardDataType.DOUBLE, {}),
        "DECFLOAT": (StandardDataType.DECIMAL, {"precision": 34, "scale": 6}),
        
        # 字符串
        "CHARACTER": (StandardDataType.CHAR, {}),
        "CHAR": (StandardDataType.CHAR, {}),
        "VARCHAR": (StandardDataType.VARCHAR, {}),
        "CHAR VARYING": (StandardDataType.VARCHAR, {}),
        "LONG VARCHAR": (StandardDataType.LONGVARCHAR, {}),
        "CLOB": (StandardDataType.CLOB, {}),
        "DBCLOB": (StandardDataType.NCLOB, {}),
        
        # 图形字符串（DB2 特定）
        "GRAPHIC": (StandardDataType.NCHAR, {}),
        "VARGRAPHIC": (StandardDataType.NVARCHAR, {}),
        "LONG VARGRAPHIC": (StandardDataType.LONGVARCHAR, {}),
        
        # 二进制
        "BINARY": (StandardDataType.BINARY, {}),
        "VARBINARY": (StandardDataType.VARBINARY, {}),
        "BLOB": (StandardDataType.BLOB, {}),
        
        # 日期时间
        "DATE": (StandardDataType.DATE, {}),
        "TIME": (StandardDataType.TIME, {}),
        "TIMESTAMP": (StandardDataType.TIMESTAMP, {}),
        
        # 布尔
        "BOOLEAN": (StandardDataType.BOOLEAN, {}),
        
        # 其他
        "XML": (StandardDataType.XML, {}),
    }

    # Oracle 类型 → 标准类型 映射
    ORACLE_TO_STANDARD: Dict[str, Tuple[str, Dict[str, Any]]] = {
        # 数值（Oracle 的 NUMBER 是万能类型）
        "NUMBER": (StandardDataType.DECIMAL, {}),
        "NUMERIC": (StandardDataType.NUMERIC, {}),
        "DECIMAL": (StandardDataType.DECIMAL, {}),
        "DEC": (StandardDataType.DECIMAL, {}),
        "INTEGER": (StandardDataType.INTEGER, {}),
        "INT": (StandardDataType.INTEGER, {}),
        "SMALLINT": (StandardDataType.SMALLINT, {}),
        "BINARY_INTEGER": (StandardDataType.INTEGER, {}),
        "PLS_INTEGER": (StandardDataType.INTEGER, {}),
        "FLOAT": (StandardDataType.FLOAT, {}),
        "BINARY_FLOAT": (StandardDataType.FLOAT, {}),
        "BINARY_DOUBLE": (StandardDataType.DOUBLE, {}),
        "REAL": (StandardDataType.REAL, {}),
        "DOUBLE PRECISION": (StandardDataType.DOUBLE, {}),
        
        # 字符串
        "CHAR": (StandardDataType.CHAR, {}),
        "CHARACTER": (StandardDataType.CHAR, {}),
        "VARCHAR2": (StandardDataType.VARCHAR, {}),
        "VARCHAR": (StandardDataType.VARCHAR, {}),
        "LONG": (StandardDataType.LONGVARCHAR, {}),
        "NCHAR": (StandardDataType.NCHAR, {}),
        "NVARCHAR2": (StandardDataType.NVARCHAR, {}),
        "CLOB": (StandardDataType.CLOB, {}),
        "NCLOB": (StandardDataType.NCLOB, {}),
        "LONGRAW": (StandardDataType.LONGVARBINARY, {}),
        
        # 二进制
        "RAW": (StandardDataType.VARBINARY, {}),
        "BLOB": (StandardDataType.BLOB, {}),
        "BFILE": (StandardDataType.BLOB, {}),
        
        # 日期时间
        "DATE": (StandardDataType.DATETIME, {}),
        "TIMESTAMP": (StandardDataType.TIMESTAMP, {}),
        "TIMESTAMP WITH TIME ZONE": (StandardDataType.TIMESTAMP, {}),
        "TIMESTAMP WITH LOCAL TIME ZONE": (StandardDataType.TIMESTAMP, {}),
        "INTERVAL YEAR TO MONTH": (StandardDataType.VARCHAR, {"length": 30}),
        "INTERVAL DAY TO SECOND": (StandardDataType.VARCHAR, {"length": 30}),
        
        # 布尔
        "BOOLEAN": (StandardDataType.BOOLEAN, {}),
        
        # 其他
        "XMLTYPE": (StandardDataType.XML, {}),
        "ROWID": (StandardDataType.VARCHAR, {"length": 18}),
        "UROWID": (StandardDataType.VARCHAR, {"length": 4000}),
    }

    # 标准类型 → MySQL 类型 映射
    STANDARD_TO_MYSQL: Dict[str, Tuple[str, Dict[str, Any]]] = {
        StandardDataType.TINYINT: ("TINYINT", {}),
        StandardDataType.SMALLINT: ("SMALLINT", {}),
        StandardDataType.INTEGER: ("INT", {}),
        StandardDataType.BIGINT: ("BIGINT", {}),
        StandardDataType.DECIMAL: ("DECIMAL", {"precision": 10, "scale": 0}),
        StandardDataType.NUMERIC: ("DECIMAL", {"precision": 10, "scale": 0}),
        StandardDataType.FLOAT: ("FLOAT", {}),
        StandardDataType.DOUBLE: ("DOUBLE", {}),
        StandardDataType.REAL: ("DOUBLE", {}),
        StandardDataType.CHAR: ("CHAR", {"length": 1}),
        StandardDataType.VARCHAR: ("VARCHAR", {"length": 255}),
        StandardDataType.LONGVARCHAR: ("MEDIUMTEXT", {}),
        StandardDataType.TEXT: ("TEXT", {}),
        StandardDataType.CLOB: ("LONGTEXT", {}),
        StandardDataType.NCHAR: ("CHAR", {"length": 1}),
        StandardDataType.NVARCHAR: ("VARCHAR", {"length": 255}),
        StandardDataType.NCLOB: ("LONGTEXT", {}),
        StandardDataType.BINARY: ("BINARY", {"length": 1}),
        StandardDataType.VARBINARY: ("VARBINARY", {"length": 255}),
        StandardDataType.LONGVARBINARY: ("MEDIUMBLOB", {}),
        StandardDataType.BLOB: ("BLOB", {}),
        StandardDataType.DATE: ("DATE", {}),
        StandardDataType.TIME: ("TIME", {}),
        StandardDataType.TIMESTAMP: ("TIMESTAMP", {}),
        StandardDataType.DATETIME: ("DATETIME", {}),
        StandardDataType.BOOLEAN: ("TINYINT(1)", {}),
        StandardDataType.BIT: ("BIT", {}),
        StandardDataType.JSON: ("JSON", {}),
        StandardDataType.XML: ("TEXT", {}),
        StandardDataType.UUID: ("VARCHAR(36)", {}),
    }

    # 标准类型 → DB2 类型 映射
    STANDARD_TO_DB2: Dict[str, Tuple[str, Dict[str, Any]]] = {
        StandardDataType.TINYINT: ("SMALLINT", {}),
        StandardDataType.SMALLINT: ("SMALLINT", {}),
        StandardDataType.INTEGER: ("INTEGER", {}),
        StandardDataType.BIGINT: ("BIGINT", {}),
        StandardDataType.DECIMAL: ("DECIMAL", {"precision": 10, "scale": 0}),
        StandardDataType.NUMERIC: ("DECIMAL", {"precision": 10, "scale": 0}),
        StandardDataType.FLOAT: ("FLOAT", {}),
        StandardDataType.DOUBLE: ("DOUBLE", {}),
        StandardDataType.REAL: ("REAL", {}),
        StandardDataType.CHAR: ("CHAR", {"length": 1}),
        StandardDataType.VARCHAR: ("VARCHAR", {"length": 255}),
        StandardDataType.LONGVARCHAR: ("CLOB", {}),
        StandardDataType.TEXT: ("CLOB", {}),
        StandardDataType.CLOB: ("CLOB", {}),
        StandardDataType.NCHAR: ("GRAPHIC", {"length": 1}),
        StandardDataType.NVARCHAR: ("VARGRAPHIC", {"length": 127}),
        StandardDataType.NCLOB: ("DBCLOB", {}),
        StandardDataType.BINARY: ("CHAR", {"length": 1}),
        StandardDataType.VARBINARY: ("VARCHAR", {"length": 255}),
        StandardDataType.LONGVARBINARY: ("BLOB", {}),
        StandardDataType.BLOB: ("BLOB", {}),
        StandardDataType.DATE: ("DATE", {}),
        StandardDataType.TIME: ("TIME", {}),
        StandardDataType.TIMESTAMP: ("TIMESTAMP", {}),
        StandardDataType.DATETIME: ("TIMESTAMP", {}),
        StandardDataType.BOOLEAN: ("SMALLINT", {}),
        StandardDataType.BIT: ("SMALLINT", {}),
        StandardDataType.JSON: ("CLOB", {}),
        StandardDataType.XML: ("XML", {}),
        StandardDataType.UUID: ("VARCHAR(36)", {}),
    }

    # 标准类型 → Oracle 类型 映射
    STANDARD_TO_ORACLE: Dict[str, Tuple[str, Dict[str, Any]]] = {
        StandardDataType.TINYINT: ("NUMBER", {"precision": 3, "scale": 0}),
        StandardDataType.SMALLINT: ("NUMBER", {"precision": 5, "scale": 0}),
        StandardDataType.INTEGER: ("NUMBER", {"precision": 10, "scale": 0}),
        StandardDataType.BIGINT: ("NUMBER", {"precision": 19, "scale": 0}),
        StandardDataType.DECIMAL: ("NUMBER", {"precision": 10, "scale": 0}),
        StandardDataType.NUMERIC: ("NUMBER", {"precision": 10, "scale": 0}),
        StandardDataType.FLOAT: ("BINARY_FLOAT", {}),
        StandardDataType.DOUBLE: ("BINARY_DOUBLE", {}),
        StandardDataType.REAL: ("BINARY_FLOAT", {}),
        StandardDataType.CHAR: ("CHAR", {"length": 1}),
        StandardDataType.VARCHAR: ("VARCHAR2", {"length": 255}),
        StandardDataType.LONGVARCHAR: ("CLOB", {}),
        StandardDataType.TEXT: ("CLOB", {}),
        StandardDataType.CLOB: ("CLOB", {}),
        StandardDataType.NCHAR: ("NCHAR", {"length": 1}),
        StandardDataType.NVARCHAR: ("NVARCHAR2", {"length": 255}),
        StandardDataType.NCLOB: ("NCLOB", {}),
        StandardDataType.BINARY: ("RAW", {"length": 2000}),
        StandardDataType.VARBINARY: ("RAW", {"length": 2000}),
        StandardDataType.LONGVARBINARY: ("BLOB", {}),
        StandardDataType.BLOB: ("BLOB", {}),
        StandardDataType.DATE: ("DATE", {}),
        StandardDataType.TIME: ("DATE", {}),
        StandardDataType.TIMESTAMP: ("TIMESTAMP", {}),
        StandardDataType.DATETIME: ("DATE", {}),
        StandardDataType.BOOLEAN: ("NUMBER", {"precision": 1, "scale": 0}),
        StandardDataType.BIT: ("NUMBER", {"precision": 1, "scale": 0}),
        StandardDataType.JSON: ("CLOB", {}),
        StandardDataType.XML: ("XMLTYPE", {}),
        StandardDataType.UUID: ("VARCHAR2(36)", {}),
    }

    @classmethod
    def _normalize_type_name(cls, type_name: str) -> str:
        """规范化类型名称，便于查找映射。去掉括号内的参数。"""
        name = type_name.strip().upper()
        if '(' in name:
            name = name[:name.index('(')].strip()
        return name

    @classmethod
    def _parse_type_params(cls, type_name: str) -> Tuple[str, List[int]]:
        """解析类型名称中的参数，如 DECIMAL(10,2) → ('DECIMAL', [10, 2])。"""
        name = type_name.strip().upper()
        params = []
        if '(' in name and ')' in name:
            param_str = name[name.index('(')+1:name.index(')')].strip()
            name = name[:name.index('(')].strip()
            if param_str:
                params = [int(p.strip()) for p in param_str.split(',') if p.strip().isdigit()]
        return name, params

    @classmethod
    def to_standard(cls, db_type: str, original_type: str,
                    length: Optional[int] = None,
                    precision: Optional[int] = None,
                    scale: Optional[int] = None) -> Tuple[str, Dict[str, Any]]:
        """
        将数据库特定类型转换为标准类型。
        
        Args:
            db_type: 数据库类型 (mysql/db2/oracle)
            original_type: 原始数据库类型名称
            length: 原始长度
            precision: 原始精度
            scale: 原始小数位数
            
        Returns:
            (标准类型名, 类型参数字典) 元组
        """
        original_norm, inline_params = cls._parse_type_params(original_type)
        
        if inline_params and length is None:
            if len(inline_params) >= 1:
                length = inline_params[0]
            if len(inline_params) >= 2 and precision is None:
                precision = inline_params[0]
                scale = inline_params[1]
        
        mapping = None
        
        if db_type == "mysql":
            mapping = cls.MYSQL_TO_STANDARD
        elif db_type == "db2":
            mapping = cls.DB2_TO_STANDARD
        elif db_type == "oracle":
            mapping = cls.ORACLE_TO_STANDARD
        else:
            logger.warning("未知数据库类型: %s，使用 VARCHAR 作为默认类型", db_type)
            return StandardDataType.VARCHAR, {"length": length or 255}
        
        if original_norm in mapping:
            std_type, defaults = mapping[original_norm]
            params = dict(defaults)
            if length is not None:
                params["length"] = length
            if precision is not None:
                params["precision"] = precision
            if scale is not None:
                params["scale"] = scale
            return std_type, params
        
        logger.warning("未找到类型映射: %s (数据库: %s)，使用 VARCHAR 作为默认", original_type, db_type)
        return StandardDataType.VARCHAR, {"length": length or 255}

    @classmethod
    def from_standard(cls, db_type: str, standard_type: str,
                      length: Optional[int] = None,
                      precision: Optional[int] = None,
                      scale: Optional[int] = None) -> Tuple[str, str]:
        """
        将标准类型转换为数据库特定类型。
        
        Args:
            db_type: 目标数据库类型 (mysql/db2/oracle)
            standard_type: 标准类型名称
            length: 长度
            precision: 精度
            scale: 小数位数
            
        Returns:
            (数据库类型名, 完整类型定义字符串) 元组
        """
        std_norm = cls._normalize_type_name(standard_type)
        mapping = None
        
        if db_type == "mysql":
            mapping = cls.STANDARD_TO_MYSQL
        elif db_type == "db2":
            mapping = cls.STANDARD_TO_DB2
        elif db_type == "oracle":
            mapping = cls.STANDARD_TO_ORACLE
        else:
            logger.warning("未知数据库类型: %s，使用 VARCHAR 作为默认类型", db_type)
            return "VARCHAR", f"VARCHAR({length or 255})"
        
        if std_norm in mapping:
            db_base_type, defaults = mapping[std_norm]
            params = dict(defaults)
            if length is not None:
                params["length"] = length
            if precision is not None:
                params["precision"] = precision
            if scale is not None:
                params["scale"] = scale
            
            type_def = cls._build_type_definition(db_type, db_base_type, params)
            return db_base_type, type_def
        
        logger.warning("未找到标准类型映射: %s (目标数据库: %s)，使用 VARCHAR 作为默认", standard_type, db_type)
        return "VARCHAR", f"VARCHAR({length or 255})"

    @classmethod
    def _build_type_definition(cls, db_type: str, base_type: str, params: Dict[str, Any]) -> str:
        """根据参数构建完整的类型定义字符串。"""
        if base_type.upper() in ("DECIMAL", "NUMERIC", "NUMBER"):
            precision = params.get("precision", 10)
            scale = params.get("scale", 0)
            if precision and scale >= 0:
                return f"{base_type}({precision},{scale})"
            elif precision:
                return f"{base_type}({precision})"
            return base_type
        
        if base_type.upper() in ("CHAR", "VARCHAR", "VARCHAR2", "GRAPHIC", "VARGRAPHIC",
                                   "NCHAR", "NVARCHAR", "NVARCHAR2", "BINARY", "VARBINARY", "RAW"):
            length = params.get("length")
            if length:
                return f"{base_type}({length})"
            return base_type
        
        return base_type


# ====================================================================== #
#                         DDL 生成器基类
# ====================================================================== #

class DDLGenerator:
    """DDL 生成器基类。"""
    
    def __init__(self, db_type: str):
        self.db_type = db_type

    def quote_identifier(self, name: str) -> str:
        """引用标识符（表名、列名等）。"""
        raise NotImplementedError

    def generate_create_table(self, schema: TableSchema) -> str:
        """从表元数据生成 CREATE TABLE DDL。"""
        raise NotImplementedError

    def generate_column_def(self, column: ColumnSchema) -> str:
        """生成单列的定义。"""
        raise NotImplementedError


# ====================================================================== #
#                       MySQL DDL 生成器
# ====================================================================== #

class MySQLDDLGenerator(DDLGenerator):
    """MySQL DDL 生成器。"""
    
    def __init__(self):
        super().__init__("mysql")

    def quote_identifier(self, name: str) -> str:
        return f"`{name}`"

    def generate_create_table(self, schema: TableSchema) -> str:
        lines = []
        lines.append(f"CREATE TABLE {self.quote_identifier(schema.table_name)} (")
        
        col_defs = []
        for col in schema.columns:
            col_defs.append(f"  {self.generate_column_def(col)}")
        
        if schema.primary_keys:
            pk_cols = ", ".join(self.quote_identifier(pk) for pk in schema.primary_keys)
            col_defs.append(f"  PRIMARY KEY ({pk_cols})")
        
        lines.append(",\n".join(col_defs))
        lines.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
        
        if schema.comment:
            lines.append(f" COMMENT='{schema.comment}'")
        
        return "\n".join(lines)

    def generate_column_def(self, column: ColumnSchema) -> str:
        _, type_def = DataTypeMapper.from_standard(
            "mysql", column.data_type,
            length=column.length,
            precision=column.precision,
            scale=column.scale
        )
        
        parts = [self.quote_identifier(column.name), type_def]
        
        if not column.nullable:
            parts.append("NOT NULL")
        
        if column.auto_increment:
            parts.append("AUTO_INCREMENT")
        
        if column.default_value is not None:
            parts.append(f"DEFAULT {column.default_value}")
        
        if column.comment:
            parts.append(f"COMMENT '{column.comment}'")
        
        return " ".join(parts)


# ====================================================================== #
#                        DB2 DDL 生成器
# ====================================================================== #

class DB2DDLGenerator(DDLGenerator):
    """DB2 DDL 生成器。"""
    
    def __init__(self):
        super().__init__("db2")

    def quote_identifier(self, name: str) -> str:
        return f'"{name.upper()}"'

    def generate_create_table(self, schema: TableSchema) -> str:
        lines = []
        lines.append(f"CREATE TABLE {self.quote_identifier(schema.table_name)} (")
        
        col_defs = []
        for col in schema.columns:
            col_defs.append(f"  {self.generate_column_def(col)}")
        
        if schema.primary_keys:
            pk_cols = ", ".join(self.quote_identifier(pk) for pk in schema.primary_keys)
            constraint_name = f"PK_{schema.table_name.upper()[:20]}"
            col_defs.append(f"  CONSTRAINT {constraint_name} PRIMARY KEY ({pk_cols})")
        
        lines.append(",\n".join(col_defs))
        lines.append(")")
        
        return "\n".join(lines)

    def generate_column_def(self, column: ColumnSchema) -> str:
        _, type_def = DataTypeMapper.from_standard(
            "db2", column.data_type,
            length=column.length,
            precision=column.precision,
            scale=column.scale
        )
        
        parts = [self.quote_identifier(column.name), type_def]
        
        if column.auto_increment:
            parts.append("GENERATED ALWAYS AS IDENTITY")
        elif not column.nullable:
            parts.append("NOT NULL")
        
        if not column.auto_increment and column.default_value is not None:
            parts.append(f"WITH DEFAULT {column.default_value}")
        
        return " ".join(parts)


# ====================================================================== #
#                       Oracle DDL 生成器
# ====================================================================== #

class OracleDDLGenerator(DDLGenerator):
    """Oracle DDL 生成器。"""
    
    def __init__(self):
        super().__init__("oracle")

    def quote_identifier(self, name: str) -> str:
        return f'"{name.upper()}"'

    def generate_create_table(self, schema: TableSchema) -> str:
        lines = []
        lines.append(f"CREATE TABLE {self.quote_identifier(schema.table_name)} (")
        
        col_defs = []
        for col in schema.columns:
            col_defs.append(f"  {self.generate_column_def(col)}")
        
        if schema.primary_keys:
            pk_cols = ", ".join(self.quote_identifier(pk) for pk in schema.primary_keys)
            constraint_name = f"PK_{schema.table_name.upper()[:20]}"
            col_defs.append(f"  CONSTRAINT {constraint_name} PRIMARY KEY ({pk_cols})")
        
        lines.append(",\n".join(col_defs))
        lines.append(")")
        
        if schema.comment:
            lines.append(f"COMMENT ON TABLE {self.quote_identifier(schema.table_name)} IS '{schema.comment}'")
        
        return "\n".join(lines)

    def generate_column_def(self, column: ColumnSchema) -> str:
        _, type_def = DataTypeMapper.from_standard(
            "oracle", column.data_type,
            length=column.length,
            precision=column.precision,
            scale=column.scale
        )
        
        parts = [self.quote_identifier(column.name), type_def]
        
        if column.auto_increment:
            parts.append("GENERATED ALWAYS AS IDENTITY")
        elif not column.nullable:
            parts.append("NOT NULL")
        
        if not column.auto_increment and column.default_value is not None:
            parts.append(f"DEFAULT {column.default_value}")
        
        return " ".join(parts)


# ====================================================================== #
#                         DDL 生成器工厂
# ====================================================================== #

_DDL_GENERATORS = {
    "mysql": MySQLDDLGenerator,
    "db2": DB2DDLGenerator,
    "oracle": OracleDDLGenerator,
}


def get_ddl_generator(db_type: str) -> DDLGenerator:
    """
    获取指定数据库类型的 DDL 生成器。
    
    Args:
        db_type: 数据库类型 (mysql/db2/oracle)
        
    Returns:
        DDLGenerator 实例
    """
    db_type_lower = db_type.lower()
    generator_cls = _DDL_GENERATORS.get(db_type_lower)
    if generator_cls is None:
        supported = ", ".join(_DDL_GENERATORS.keys())
        raise ValueError(f"不支持的数据库类型: '{db_type}'，当前支持: {supported}")
    return generator_cls()


def convert_ddl(src_schema: TableSchema, target_db_type: str) -> str:
    """
    将源表结构转换为目标数据库的 DDL。
    
    这是跨库 DDL 同步的核心函数。
    
    Args:
        src_schema: 源表的元数据（中间表示）
        target_db_type: 目标数据库类型
        
    Returns:
        目标数据库的 CREATE TABLE DDL 字符串
    """
    generator = get_ddl_generator(target_db_type)
    return generator.generate_create_table(src_schema)
