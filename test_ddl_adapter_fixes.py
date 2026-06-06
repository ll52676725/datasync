"""
单元测试：DDL 适配器 SQL 注入防护修复验证
"""
import unittest
from ddl_adapter import (
    MySQLDDLGenerator, DB2DDLGenerator, OracleDDLGenerator,
    TableSchema, ColumnSchema, StandardDataType
)


class TestDDLSQLInjectionProtection(unittest.TestCase):
    """测试 DDL 生成器的 SQL 注入防护"""

    def test_mysql_table_comment_escape(self):
        """测试 MySQL 表注释中的单引号被正确转义"""
        gen = MySQLDDLGenerator()
        schema = TableSchema(
            table_name="users",
            columns=[
                ColumnSchema(name="id", data_type=StandardDataType.INTEGER, is_primary_key=True)
            ],
            comment="用户表'; DROP TABLE users; --"
        )
        ddl = gen.generate_create_table(schema)
        self.assertIn("COMMENT='用户表''; DROP TABLE users; --'", ddl)
        self.assertIn("''; DROP TABLE", ddl)

    def test_mysql_column_comment_escape(self):
        """测试 MySQL 列注释中的单引号被正确转义"""
        gen = MySQLDDLGenerator()
        schema = TableSchema(
            table_name="users",
            columns=[
                ColumnSchema(
                    name="id", data_type=StandardDataType.INTEGER, is_primary_key=True,
                    comment="主键'; DROP TABLE users; --"
                )
            ]
        )
        ddl = gen.generate_create_table(schema)
        self.assertIn("COMMENT '主键''; DROP TABLE users; --'", ddl)
        self.assertIn("''; DROP TABLE", ddl)

    def test_mysql_default_value_string_escape(self):
        """测试 MySQL 字符串默认值中的单引号被正确转义"""
        gen = MySQLDDLGenerator()
        schema = TableSchema(
            table_name="users",
            columns=[
                ColumnSchema(
                    name="name", data_type=StandardDataType.VARCHAR, length=50,
                    default_value="test'; DROP TABLE users; --"
                )
            ]
        )
        ddl = gen.generate_create_table(schema)
        self.assertIn("DEFAULT 'test''; DROP TABLE users; --'", ddl)
        self.assertIn("''; DROP TABLE", ddl)

    def test_oracle_table_comment_escape(self):
        """测试 Oracle 表注释中的单引号被正确转义"""
        gen = OracleDDLGenerator()
        schema = TableSchema(
            table_name="users",
            columns=[
                ColumnSchema(name="id", data_type=StandardDataType.INTEGER, is_primary_key=True)
            ],
            comment="用户表'; DROP TABLE USERS; --"
        )
        ddl = gen.generate_create_table(schema)
        self.assertIn("IS '用户表''; DROP TABLE USERS; --'", ddl)
        self.assertIn("''; DROP TABLE", ddl)

    def test_oracle_default_value_string_escape(self):
        """测试 Oracle 字符串默认值中的单引号被正确转义"""
        gen = OracleDDLGenerator()
        schema = TableSchema(
            table_name="users",
            columns=[
                ColumnSchema(
                    name="name", data_type=StandardDataType.VARCHAR, length=50,
                    default_value="test'; DROP TABLE USERS; --"
                )
            ]
        )
        ddl = gen.generate_create_table(schema)
        self.assertIn("DEFAULT 'test''; DROP TABLE USERS; --'", ddl)
        self.assertIn("''; DROP TABLE", ddl)

    def test_db2_default_value_string_escape(self):
        """测试 DB2 字符串默认值中的单引号被正确转义"""
        gen = DB2DDLGenerator()
        schema = TableSchema(
            table_name="users",
            columns=[
                ColumnSchema(
                    name="name", data_type=StandardDataType.VARCHAR, length=50,
                    default_value="test'; DROP TABLE USERS; --"
                )
            ]
        )
        ddl = gen.generate_create_table(schema)
        self.assertIn("WITH DEFAULT 'test''; DROP TABLE USERS; --'", ddl)
        self.assertIn("''; DROP TABLE", ddl)

    def test_invalid_table_name_raises_error(self):
        """测试非法的表名会抛出异常"""
        for gen_cls in [MySQLDDLGenerator, DB2DDLGenerator, OracleDDLGenerator]:
            gen = gen_cls()
            with self.assertRaises(ValueError):
                gen.quote_identifier("users; DROP TABLE")
            with self.assertRaises(ValueError):
                gen.quote_identifier("`users`")
            with self.assertRaises(ValueError):
                gen.quote_identifier("users--")

    def test_invalid_column_name_raises_error(self):
        """测试非法的列名会抛出异常"""
        for gen_cls in [MySQLDDLGenerator, DB2DDLGenerator, OracleDDLGenerator]:
            gen = gen_cls()
            with self.assertRaises(ValueError):
                schema = TableSchema(
                    table_name="users",
                    columns=[
                        ColumnSchema(name="id; DROP TABLE", data_type=StandardDataType.INTEGER)
                    ]
                )
                gen.generate_create_table(schema)

    def test_valid_identifiers_work(self):
        """测试合法的标识符能正常工作"""
        valid_names = ["users", "user_table", "t123", "_test", "user123"]
        for gen_cls in [MySQLDDLGenerator, DB2DDLGenerator, OracleDDLGenerator]:
            gen = gen_cls()
            for name in valid_names:
                try:
                    gen.quote_identifier(name)
                except ValueError:
                    self.fail(f"Valid identifier '{name}' should not raise error for {gen_cls.__name__}")

    def test_backslash_escape(self):
        """测试反斜杠被正确转义"""
        gen = MySQLDDLGenerator()
        schema = TableSchema(
            table_name="users",
            columns=[
                ColumnSchema(
                    name="name", data_type=StandardDataType.VARCHAR, length=50,
                    comment="路径\\test\\dir"
                )
            ]
        )
        ddl = gen.generate_create_table(schema)
        self.assertIn("COMMENT '路径\\\\test\\\\dir'", ddl)

    def test_mysql_numeric_default_no_quotes(self):
        """测试 MySQL 数值型默认值不需要加引号"""
        gen = MySQLDDLGenerator()
        schema = TableSchema(
            table_name="users",
            columns=[
                ColumnSchema(
                    name="age", data_type=StandardDataType.INTEGER,
                    default_value=18
                )
            ]
        )
        ddl = gen.generate_create_table(schema)
        self.assertIn("DEFAULT 18", ddl)
        self.assertNotIn("DEFAULT '18'", ddl)

    def test_oracle_numeric_default_no_quotes(self):
        """测试 Oracle 数值型默认值不需要加引号"""
        gen = OracleDDLGenerator()
        schema = TableSchema(
            table_name="users",
            columns=[
                ColumnSchema(
                    name="age", data_type=StandardDataType.INTEGER,
                    default_value=18
                )
            ]
        )
        ddl = gen.generate_create_table(schema)
        self.assertIn("DEFAULT 18", ddl)
        self.assertNotIn("DEFAULT '18'", ddl)

    def test_db2_numeric_default_no_quotes(self):
        """测试 DB2 数值型默认值不需要加引号"""
        gen = DB2DDLGenerator()
        schema = TableSchema(
            table_name="users",
            columns=[
                ColumnSchema(
                    name="age", data_type=StandardDataType.INTEGER,
                    default_value=18
                )
            ]
        )
        ddl = gen.generate_create_table(schema)
        self.assertIn("WITH DEFAULT 18", ddl)
        self.assertNotIn("WITH DEFAULT '18'", ddl)

    def test_escape_sql_string_method(self):
        """测试 _escape_sql_string 方法"""
        for gen_cls in [MySQLDDLGenerator, DB2DDLGenerator, OracleDDLGenerator]:
            gen = gen_cls()
            self.assertEqual(gen._escape_sql_string("test"), "test")
            self.assertEqual(gen._escape_sql_string("it's"), "it''s")
            self.assertEqual(gen._escape_sql_string("a'b'c"), "a''b''c")
            self.assertEqual(gen._escape_sql_string("path\\dir"), "path\\\\dir")


if __name__ == "__main__":
    unittest.main()
