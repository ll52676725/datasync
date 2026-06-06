"""
test_ddl_adapter.py — DDL 适配器单元测试

测试跨数据库类型的 DDL 转换功能：
  - MySQL → DB2
  - MySQL → Oracle
  - DB2 → MySQL
  - DB2 → Oracle
  - Oracle → MySQL
  - Oracle → DB2
"""

import sys
import logging
from ddl_adapter import (
    TableSchema, ColumnSchema, StandardDataType,
    DataTypeMapper, convert_ddl, get_ddl_generator
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def create_test_schema() -> TableSchema:
    """创建一个包含各种常见数据类型的测试表结构。"""
    schema = TableSchema(
        table_name="test_users",
        source_db_type="mysql",
        comment="用户信息表"
    )
    
    schema.columns = [
        ColumnSchema(
            name="id",
            data_type=StandardDataType.BIGINT,
            nullable=False,
            is_primary_key=True,
            auto_increment=True,
            comment="用户ID"
        ),
        ColumnSchema(
            name="username",
            data_type=StandardDataType.VARCHAR,
            length=64,
            nullable=False,
            comment="用户名"
        ),
        ColumnSchema(
            name="email",
            data_type=StandardDataType.VARCHAR,
            length=128,
            nullable=True,
            comment="邮箱"
        ),
        ColumnSchema(
            name="age",
            data_type=StandardDataType.INTEGER,
            nullable=True,
            comment="年龄"
        ),
        ColumnSchema(
            name="balance",
            data_type=StandardDataType.DECIMAL,
            precision=10,
            scale=2,
            nullable=False,
            default_value="0.00",
            comment="账户余额"
        ),
        ColumnSchema(
            name="is_active",
            data_type=StandardDataType.BOOLEAN,
            nullable=False,
            default_value="1",
            comment="是否激活"
        ),
        ColumnSchema(
            name="created_at",
            data_type=StandardDataType.DATETIME,
            nullable=False,
            comment="创建时间"
        ),
        ColumnSchema(
            name="updated_at",
            data_type=StandardDataType.TIMESTAMP,
            nullable=True,
            comment="更新时间"
        ),
        ColumnSchema(
            name="bio",
            data_type=StandardDataType.TEXT,
            nullable=True,
            comment="个人简介"
        ),
    ]
    
    schema.primary_keys = ["id"]
    
    return schema


def test_data_type_mapping():
    """测试数据类型映射。"""
    print("\n" + "="*80)
    print("测试 1: 数据类型映射测试")
    print("="*80)
    
    test_cases = [
        ("mysql", "INT", None, None, None, StandardDataType.INTEGER),
        ("mysql", "BIGINT", None, None, None, StandardDataType.BIGINT),
        ("mysql", "VARCHAR", 255, None, None, StandardDataType.VARCHAR),
        ("mysql", "DECIMAL", None, 10, 2, StandardDataType.DECIMAL),
        ("mysql", "DATETIME", None, None, None, StandardDataType.DATETIME),
        ("mysql", "TINYINT(1)", None, None, None, StandardDataType.TINYINT),
        ("db2", "INTEGER", None, None, None, StandardDataType.INTEGER),
        ("db2", "VARCHAR", 255, None, None, StandardDataType.VARCHAR),
        ("db2", "TIMESTAMP", None, None, None, StandardDataType.TIMESTAMP),
        ("oracle", "NUMBER", None, 10, 2, StandardDataType.DECIMAL),
        ("oracle", "VARCHAR2", 255, None, None, StandardDataType.VARCHAR),
        ("oracle", "DATE", None, None, None, StandardDataType.DATETIME),
    ]
    
    all_passed = True
    for db_type, original_type, length, precision, scale, expected_std in test_cases:
        std_type, params = DataTypeMapper.to_standard(
            db_type, original_type, length=length, precision=precision, scale=scale
        )
        passed = std_type == expected_std
        status = "✓" if passed else "✗"
        print(f"  {status} {db_type:6s} {original_type:15s} → {std_type} (预期: {expected_std})")
        if not passed:
            all_passed = False
    
    print(f"\n数据类型映射测试: {'全部通过' if all_passed else '存在失败'}")
    return all_passed


def test_ddl_generation():
    """测试各数据库 DDL 生成。"""
    print("\n" + "="*80)
    print("测试 2: 各数据库 DDL 生成测试")
    print("="*80)
    
    schema = create_test_schema()
    
    for db_type in ["mysql", "db2", "oracle"]:
        print(f"\n--- 生成 {db_type.upper()} DDL ---")
        ddl = convert_ddl(schema, db_type)
        print(ddl)
        print()
    
    return True


def test_cross_db_conversion():
    """测试跨数据库 DDL 转换。"""
    print("\n" + "="*80)
    print("测试 3: 跨数据库 DDL 转换测试")
    print("="*80)
    
    schema = create_test_schema()
    
    conversions = [
        ("mysql", "db2"),
        ("mysql", "oracle"),
        ("db2", "mysql"),
        ("db2", "oracle"),
        ("oracle", "mysql"),
        ("oracle", "db2"),
    ]
    
    all_passed = True
    for src_db, tgt_db in conversions:
        try:
            schema.source_db_type = src_db
            ddl = convert_ddl(schema, tgt_db)
            print(f"  ✓ {src_db:6s} → {tgt_db:6s}: DDL 生成成功 (长度: {len(ddl)} 字符)")
        except Exception as e:
            print(f"  ✗ {src_db:6s} → {tgt_db:6s}: 失败 - {e}")
            all_passed = False
    
    print(f"\n跨库 DDL 转换测试: {'全部通过' if all_passed else '存在失败'}")
    return all_passed


def test_auto_increment():
    """测试自增主键在各数据库中的转换。"""
    print("\n" + "="*80)
    print("测试 4: 自增主键 DDL 转换测试")
    print("="*80)
    
    schema = TableSchema(
        table_name="test_auto_inc",
        source_db_type="mysql",
        columns=[
            ColumnSchema(
                name="id",
                data_type=StandardDataType.BIGINT,
                nullable=False,
                is_primary_key=True,
                auto_increment=True,
                comment="自增主键"
            ),
            ColumnSchema(
                name="name",
                data_type=StandardDataType.VARCHAR,
                length=100,
                nullable=False,
                comment="名称"
            ),
        ],
        primary_keys=["id"]
    )
    
    expected_patterns = {
        "mysql": "AUTO_INCREMENT",
        "db2": "GENERATED ALWAYS AS IDENTITY",
        "oracle": "GENERATED ALWAYS AS IDENTITY",
    }
    
    all_passed = True
    for db_type in ["mysql", "db2", "oracle"]:
        ddl = convert_ddl(schema, db_type)
        pattern = expected_patterns[db_type]
        passed = pattern in ddl
        status = "✓" if passed else "✗"
        print(f"\n  {status} {db_type.upper()} 自增主键:")
        print(f"    期望包含: {pattern}")
        print(f"    DDL 片段: ...{ddl[ddl.lower().find('id'):ddl.lower().find('id')+120]}...")
        if not passed:
            all_passed = False
            print(f"    ✗ 未找到期望的自增语法!")
    
    print(f"\n自增主键测试: {'全部通过' if all_passed else '存在失败'}")
    return all_passed


def test_standard_type_roundtrip():
    """测试标准类型往返转换。"""
    print("\n" + "="*80)
    print("测试 5: 标准类型往返转换测试")
    print("="*80)
    
    test_types = [
        (StandardDataType.INTEGER, None, None, None),
        (StandardDataType.BIGINT, None, None, None),
        (StandardDataType.VARCHAR, 255, None, None),
        (StandardDataType.DECIMAL, None, 10, 2),
        (StandardDataType.DATETIME, None, None, None),
        (StandardDataType.TIMESTAMP, None, None, None),
        (StandardDataType.BOOLEAN, None, None, None),
        (StandardDataType.TEXT, None, None, None),
    ]
    
    all_passed = True
    for std_type, length, precision, scale in test_types:
        print(f"\n  标准类型: {std_type}")
        for db_type in ["mysql", "db2", "oracle"]:
            db_type_name, type_def = DataTypeMapper.from_standard(
                db_type, std_type, length=length, precision=precision, scale=scale
            )
            print(f"    → {db_type:6s}: {type_def}")
    
    print(f"\n标准类型往返转换测试完成")
    return True


def main():
    """运行所有测试。"""
    print("\n" + "="*80)
    print("DDL 适配器单元测试")
    print("="*80)
    
    results = []
    
    results.append(("数据类型映射", test_data_type_mapping()))
    results.append(("DDL 生成", test_ddl_generation()))
    results.append(("跨库转换", test_cross_db_conversion()))
    results.append(("自增主键", test_auto_increment()))
    results.append(("类型往返", test_standard_type_roundtrip()))
    
    print("\n" + "="*80)
    print("测试汇总")
    print("="*80)
    
    all_passed = True
    for name, passed in results:
        status = "✓ 通过" if passed else "✗ 失败"
        print(f"  {name:15s}: {status}")
        if not passed:
            all_passed = False
    
    print()
    if all_passed:
        print("✓ 所有测试通过!")
        return 0
    else:
        print("✗ 部分测试失败，请检查上面的输出")
        return 1


if __name__ == "__main__":
    sys.exit(main())
