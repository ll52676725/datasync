# -*- coding: utf-8 -*-
"""数据转换引擎核心模块单元测试"""

from data_transformer import (
    ConditionFilterEngine,
    RowScriptEngine,
    AggregationEngine,
    TransformPipeline,
    validate_transform_rules,
    build_pipeline_for_table
)

test_rows = [
    {"id": 1, "name": "张三", "age": 25, "status": "active", "amount": 1200.5, "region": "CN", "category": "A"},
    {"id": 2, "name": "李四", "age": 17, "status": "inactive", "amount": 800.0, "region": "CN", "category": "A"},
    {"id": 3, "name": "王五", "age": 30, "status": "active", "amount": 2500.0, "region": "US", "category": "B"},
    {"id": 4, "name": "赵六", "age": 45, "status": "active", "amount": 560.0, "region": "US", "category": "B"},
    {"id": 5, "name": "张小明", "age": 22, "status": "active", "amount": 3200.0, "region": "JP", "category": "A"},
    {"id": 6, "name": "Smith", "age": 60, "status": None, "amount": 150.0, "region": "US", "category": "C"},
]

def test_condition_basic():
    cond = "age >= 18 AND status = 'active'"
    eng = ConditionFilterEngine(cond)
    passed = [r for r in test_rows if eng.evaluate(r)]
    assert len(passed) == 4, f"期望 4 条通过，实际 {len(passed)}"
    assert all(r["age"] >= 18 for r in passed)
    print("✓ 基础条件测试通过 (AND + 比较)")

def test_condition_in_and_like():
    cond = "region IN ('CN', 'US') AND name LIKE '张%'"
    eng = ConditionFilterEngine(cond)
    passed = [r["name"] for r in test_rows if eng.evaluate(r)]
    # 张三 region=CN ✓; 张小明 region=JP ✗; 其他都不姓张
    assert "张三" in passed
    assert "张小明" not in passed
    assert len(passed) == 1
    print("✓ IN + LIKE 测试通过")

def test_condition_is_null():
    cond = "status IS NULL OR amount < 600"
    eng = ConditionFilterEngine(cond)
    passed = [r["name"] for r in test_rows if eng.evaluate(r)]
    assert "赵六" in passed and "Smith" in passed
    print("✓ IS NULL + OR 测试通过")

def test_script_filter_and_transform():
    script = """
def filter_row(row):
    return row.get('status') == 'active' and row.get('amount', 0) > 1000

def transform_row(row):
    row['display_name'] = f\"{row['name']}({row['age']})\"
    row['amount_double'] = round(row['amount'] * 2, 2)
    return row
"""
    eng = RowScriptEngine(script)
    results = []
    for r in test_rows:
        out = eng.apply(dict(r))
        if out is not None:
            results.append(out)
    assert len(results) == 3, f"期望 3 条，实际 {len(results)}"
    assert results[0]["display_name"].startswith("张三")
    assert results[0]["amount_double"] == 2401.0
    print("✓ 脚本过滤+转换测试通过")

def test_aggregation_basic():
    agg = AggregationEngine(
        group_by=["region", "category"],
        metrics=[
            {"source": "amount", "target": "total_amount", "func": "SUM"},
            {"source": "id", "target": "cnt", "func": "COUNT"},
            {"source": "amount", "target": "avg_amount", "func": "AVG"},
        ]
    )
    for r in test_rows:
        agg.add_row(r)
    results = agg.finalize()
    assert len(results) == 4, f"期望 4 个分组 (CN/A, US/B, JP/A, US/C)，实际 {len(results)}"
    # 查找 CN + A 分组
    cn_a = [r for r in results if r["region"] == "CN" and r["category"] == "A"][0]
    assert cn_a["cnt"] == 2
    assert abs(cn_a["total_amount"] - 2000.5) < 0.001
    print("✓ 聚合引擎测试通过 (SUM/COUNT/AVG)")
    return results

def test_pipeline_streaming():
    rules = {
        "condition": "status = 'active'",
        "rowScript": "def transform_row(row):\n    row['amount_x2'] = row['amount'] * 2\n    return row"
    }
    pipeline = TransformPipeline(rules, source_columns=["id", "name", "age", "status", "amount", "region", "category"])
    assert pipeline.streaming_mode is True
    assert pipeline.has_any_transform is True
    count = 0
    for r in test_rows:
        out = pipeline.process_row(dict(r))
        if out is not None:
            count += 1
            assert "amount_x2" in out
    assert count == 4
    final = pipeline.finalize()
    assert final == []
    print("✓ 管道 (流式 条件+脚本) 测试通过")

def test_pipeline_aggregation():
    rules = {
        "condition": "age >= 18",
        "aggregation": {
            "groupBy": ["region"],
            "metrics": [
                {"source": "amount", "target": "total_amt", "func": "SUM"},
                {"source": "id", "target": "cnt", "func": "COUNT"},
            ]
        }
    }
    pipeline = TransformPipeline(rules)
    assert pipeline.streaming_mode is False
    assert hasattr(pipeline, 'aggregation_engine') or not pipeline.streaming_mode
    for r in test_rows:
        pipeline.process_row(dict(r))
    results = pipeline.finalize()
    print(f"  聚合结果: {results}")
    assert len(results) >= 3
    print("✓ 管道 (条件+聚合两阶段) 测试通过")

def test_validate_rules():
    # 好规则
    ok = validate_transform_rules({"condition": "amount > 0"})
    assert ok["success"], f"条件应通过: {ok['errors']}"

    # 坏条件
    bad = validate_transform_rules({"condition": "amount > AND"})
    assert not bad["success"], "语法错误应被捕获"
    assert len(bad["errors"]) > 0

    # 好脚本
    ok_script = validate_transform_rules({
        "rowScript": "def filter_row(row):\n    return True"
    })
    assert ok_script["success"]

    # 坏脚本 (调用 eval)
    bad_script = validate_transform_rules({
        "rowScript": "def transform_row(row):\n    eval('1+1')\n    return row"
    })
    assert not bad_script["success"]

    # 好聚合
    ok_agg = validate_transform_rules({
        "aggregation": {
            "groupBy": ["region"],
            "metrics": [{"source": "amount", "target": "total", "func": "SUM"}]
        }
    })
    assert ok_agg["success"]

    # 坏聚合 (未知函数)
    bad_agg = validate_transform_rules({
        "aggregation": {
            "groupBy": ["region"],
            "metrics": [{"source": "amount", "target": "total", "func": "FOO"}]
        }
    })
    assert not bad_agg["success"]

    print("✓ 规则校验 (validate_transform_rules) 测试通过")

def test_factory_and_config_chain():
    """模拟从 sync_section 配置构建管道的工厂函数"""
    sync_section = {
        "fieldMappings": {
            "orders": {
                "transformRules": {
                    "condition": "amount > 100",
                    "rowScript": "def transform_row(row):\n    row['flag'] = 1\n    return row"
                }
            }
        }
    }
    pipeline = build_pipeline_for_table(sync_section, "orders")
    assert pipeline.has_any_transform
    assert pipeline.condition_engine is not None
    assert pipeline.row_script_engine is not None
    print("✓ 工厂函数 (build_pipeline_for_table) 测试通过")

if __name__ == "__main__":
    print("=" * 60)
    print("开始运行数据转换引擎单元测试...")
    print("=" * 60)
    test_condition_basic()
    test_condition_in_and_like()
    test_condition_is_null()
    test_script_filter_and_transform()
    test_aggregation_basic()
    test_pipeline_streaming()
    test_pipeline_aggregation()
    test_validate_rules()
    test_factory_and_config_chain()
    print("=" * 60)
    print("✅ 全部测试通过！")
    print("=" * 60)
