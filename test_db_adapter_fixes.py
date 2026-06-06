"""
单元测试：验证 db_adapter.py 中的修复
1. batch_delete 分批逻辑
2. SQL 标识符验证
3. 边界条件处理
"""

import unittest
from unittest.mock import MagicMock, call, patch
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_adapter import MySQLAdapter, DB2Adapter, OracleAdapter, BaseDBAdapter


class TestBatchDeleteChunking(unittest.TestCase):
    """测试 batch_delete 的分批逻辑"""

    def test_mysql_batch_delete_chunking(self):
        """测试 MySQL batch_delete 自动分批"""
        cfg = {"host": "localhost", "port": 3306, "user": "test", 
               "password": "test", "database": "test", "type": "mysql"}
        adapter = MySQLAdapter(cfg)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        pk_values = list(range(1250))
        
        adapter.batch_delete(mock_conn, "test_table", "id", pk_values, 
                           auto_commit=True, chunk_size=500)
        
        self.assertEqual(mock_cursor.execute.call_count, 3)
        
        calls = mock_cursor.execute.call_args_list
        for i, call_args in enumerate(calls):
            sql, params = call_args[0]
            expected_start = i * 500
            expected_end = min((i + 1) * 500, 1250)
            self.assertEqual(len(params), expected_end - expected_start)
            self.assertIn("WHERE `id` IN (", sql)
        
        mock_conn.commit.assert_called_once()

    def test_db2_batch_delete_chunking(self):
        """测试 DB2 batch_delete 自动分批"""
        cfg = {"host": "localhost", "port": 50000, "user": "test", 
               "password": "test", "database": "test", "type": "db2"}
        adapter = DB2Adapter(cfg)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        pk_values = list(range(750))
        
        adapter.batch_delete(mock_conn, "test_table", "id", pk_values, 
                           auto_commit=True, chunk_size=250)
        
        self.assertEqual(mock_cursor.execute.call_count, 3)
        
        calls = mock_cursor.execute.call_args_list
        for i, call_args in enumerate(calls):
            sql, params = call_args[0]
            expected_start = i * 250
            expected_end = min((i + 1) * 250, 750)
            self.assertEqual(len(params), expected_end - expected_start)
            self.assertIn('WHERE "id" IN (', sql)
        
        mock_conn.commit.assert_called_once()

    def test_oracle_batch_delete_chunking(self):
        """测试 Oracle batch_delete 自动分批"""
        cfg = {"host": "localhost", "port": 1521, "user": "test", 
               "password": "test", "database": "test", "type": "oracle"}
        adapter = OracleAdapter(cfg)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        pk_values = list(range(2000))
        
        adapter.batch_delete(mock_conn, "test_table", "id", pk_values, 
                           auto_commit=True, chunk_size=1000)
        
        self.assertEqual(mock_cursor.execute.call_count, 2)
        
        calls = mock_cursor.execute.call_args_list
        for i, call_args in enumerate(calls):
            sql, params = call_args[0]
            expected_start = i * 1000
            expected_end = min((i + 1) * 1000, 2000)
            self.assertEqual(len(params), expected_end - expected_start)
            self.assertIn('WHERE "id" IN (', sql)
        
        mock_conn.commit.assert_called_once()

    def test_batch_delete_empty_list(self):
        """测试空列表时 batch_delete 不执行任何操作"""
        cfg = {"host": "localhost", "port": 3306, "user": "test", 
               "password": "test", "database": "test", "type": "mysql"}
        adapter = MySQLAdapter(cfg)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        adapter.batch_delete(mock_conn, "test_table", "id", [], auto_commit=True)
        
        mock_cursor.execute.assert_not_called()
        mock_conn.commit.assert_not_called()

    def test_batch_delete_no_auto_commit(self):
        """测试 auto_commit=False 时不调用 commit"""
        cfg = {"host": "localhost", "port": 3306, "user": "test", 
               "password": "test", "database": "test", "type": "mysql"}
        adapter = MySQLAdapter(cfg)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        pk_values = [1, 2, 3]
        adapter.batch_delete(mock_conn, "test_table", "id", pk_values, 
                           auto_commit=False, chunk_size=2)
        
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_conn.commit.assert_not_called()


class TestIdentifierValidation(unittest.TestCase):
    """测试 SQL 标识符验证功能"""

    def test_valid_identifiers(self):
        """测试合法的标识符"""
        cfg = {"host": "localhost", "port": 3306, "user": "test", 
               "password": "test", "database": "test", "type": "mysql"}
        adapter = MySQLAdapter(cfg)
        
        valid_names = [
            "table_name",
            "column123",
            "USER_DATA",
            "test_2024",
            "a",
            "id"
        ]
        
        for name in valid_names:
            result = adapter._validate_identifier(name)
            self.assertEqual(result, name)

    def test_invalid_identifiers(self):
        """测试非法的标识符应抛出异常"""
        cfg = {"host": "localhost", "port": 3306, "user": "test", 
               "password": "test", "database": "test", "type": "mysql"}
        adapter = MySQLAdapter(cfg)
        
        invalid_names = [
            "table; DROP TABLE users--",
            "column' OR '1'='1",
            "test table",
            "data--comment",
            "`injection`",
            "",
            "table_name; DELETE * FROM users",
            "name' AND 1=1--"
        ]
        
        for name in invalid_names:
            with self.assertRaises(ValueError, msg=f"Should reject: {name}"):
                adapter._validate_identifier(name)

    def test_db2_identifier_validation(self):
        """测试 DB2 适配器的标识符验证"""
        cfg = {"host": "localhost", "port": 50000, "user": "test", 
               "password": "test", "database": "test", "type": "db2"}
        adapter = DB2Adapter(cfg)
        
        self.assertEqual(adapter._validate_identifier("valid_col"), "valid_col")
        with self.assertRaises(ValueError):
            adapter._validate_identifier("invalid;col")

    def test_oracle_identifier_validation(self):
        """测试 Oracle 适配器的标识符验证"""
        cfg = {"host": "localhost", "port": 1521, "user": "test", 
               "password": "test", "database": "test", "type": "oracle"}
        adapter = OracleAdapter(cfg)
        
        self.assertEqual(adapter._validate_identifier("VALID_COL"), "VALID_COL")
        with self.assertRaises(ValueError):
            adapter._validate_identifier("INVALID'COL")


class TestCursorClosure(unittest.TestCase):
    """测试游标正确关闭（try-finally）"""

    def test_upsert_row_cursor_closed_on_error(self):
        """测试 upsert_row 出错时游标仍会关闭"""
        cfg = {"host": "localhost", "port": 3306, "user": "test", 
               "password": "test", "database": "test", "type": "mysql"}
        adapter = MySQLAdapter(cfg)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("DB Error")
        
        with self.assertRaises(Exception):
            adapter.upsert_row(mock_conn, "test_table", 
                             {"id": 1, "name": "test"}, "id", auto_commit=True)
        
        mock_cursor.close.assert_called_once()

    def test_delete_row_cursor_closed_on_error(self):
        """测试 delete_row 出错时游标仍会关闭"""
        cfg = {"host": "localhost", "port": 3306, "user": "test", 
               "password": "test", "database": "test", "type": "mysql"}
        adapter = MySQLAdapter(cfg)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("DB Error")
        
        with self.assertRaises(Exception):
            adapter.delete_row(mock_conn, "test_table", "id", 1, auto_commit=True)
        
        mock_cursor.close.assert_called_once()

    def test_batch_upsert_cursor_closed_on_error(self):
        """测试 batch_upsert 出错时游标仍会关闭"""
        cfg = {"host": "localhost", "port": 3306, "user": "test", 
               "password": "test", "database": "test", "type": "mysql"}
        adapter = MySQLAdapter(cfg)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.executemany.side_effect = Exception("DB Error")
        
        with self.assertRaises(Exception):
            adapter.batch_upsert(mock_conn, "test_table", ["id", "name"], 
                               [(1, "test")], "id", auto_commit=True)
        
        mock_cursor.close.assert_called_once()


class TestEdgeCases(unittest.TestCase):
    """测试其他边界情况"""

    def test_batch_delete_single_batch(self):
        """测试数据量小于 chunk_size 时只执行一次"""
        cfg = {"host": "localhost", "port": 3306, "user": "test", 
               "password": "test", "database": "test", "type": "mysql"}
        adapter = MySQLAdapter(cfg)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        pk_values = list(range(100))
        adapter.batch_delete(mock_conn, "test_table", "id", pk_values, 
                           chunk_size=500)
        
        self.assertEqual(mock_cursor.execute.call_count, 1)
        sql, params = mock_cursor.execute.call_args[0]
        self.assertEqual(len(params), 100)

    def test_batch_delete_exact_chunk_size(self):
        """测试数据量正好等于 chunk_size 倍数"""
        cfg = {"host": "localhost", "port": 3306, "user": "test", 
               "password": "test", "database": "test", "type": "mysql"}
        adapter = MySQLAdapter(cfg)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        pk_values = list(range(1000))
        adapter.batch_delete(mock_conn, "test_table", "id", pk_values, 
                           chunk_size=500)
        
        self.assertEqual(mock_cursor.execute.call_count, 2)

    def test_temp_table_col_defs_use_clob(self):
        """测试临时表列定义使用 CLOB 避免截断"""
        cfg = {"host": "localhost", "port": 1521, "user": "test", 
               "password": "test", "database": "test", "type": "oracle"}
        adapter = OracleAdapter(cfg)
        
        columns = ["id", "name", "description", "long_text"]
        col_defs = adapter._get_temp_table_col_defs(columns)
        
        self.assertIn("CLOB", col_defs)
        self.assertNotIn("VARCHAR2(4000)", col_defs)
        self.assertNotIn("VARCHAR2", col_defs)
        
        for col in columns:
            self.assertIn(f'"{col}" CLOB', col_defs)

    def test_db2_temp_table_col_defs_use_clob(self):
        """测试 DB2 临时表列定义使用 CLOB"""
        cfg = {"host": "localhost", "port": 50000, "user": "test", 
               "password": "test", "database": "test", "type": "db2"}
        adapter = DB2Adapter(cfg)
        
        columns = ["id", "name", "description"]
        col_defs = adapter._get_temp_table_col_defs(columns)
        
        self.assertIn("CLOB", col_defs)
        self.assertNotIn("VARCHAR(32672)", col_defs)


class TestQuoteIdentifier(unittest.TestCase):
    """测试标识符引用"""

    def test_mysql_quote_identifier(self):
        cfg = {"host": "localhost", "port": 3306, "user": "test", 
               "password": "test", "database": "test", "type": "mysql"}
        adapter = MySQLAdapter(cfg)
        self.assertEqual(adapter.quote_identifier("table_name"), "`table_name`")

    def test_db2_quote_identifier(self):
        cfg = {"host": "localhost", "port": 50000, "user": "test", 
               "password": "test", "database": "test", "type": "db2"}
        adapter = DB2Adapter(cfg)
        self.assertEqual(adapter.quote_identifier("table_name"), '"table_name"')

    def test_oracle_quote_identifier(self):
        cfg = {"host": "localhost", "port": 1521, "user": "test", 
               "password": "test", "database": "test", "type": "oracle"}
        adapter = OracleAdapter(cfg)
        self.assertEqual(adapter.quote_identifier("table_name"), '"table_name"')


if __name__ == "__main__":
    unittest.main(verbosity=2)
