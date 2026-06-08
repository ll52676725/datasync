# -*- coding: utf-8 -*-
"""测试新增的转换规则 API"""
import requests
import json

BASE = 'http://127.0.0.1:5000'
s = requests.Session()

print('=' * 70)
print('数据转换规则 API 功能测试')
print('=' * 70)

# 1. 登录
print('\n[1] 登录...')
r = s.post(f'{BASE}/login', json={'username': 'admin', 'password': 'admin123'})
print('   → status:', r.status_code)
print('   → response[:200]:', r.text[:200])
try:
    j = r.json()
    print('   → JSON:', j.get('success'), j.get('message'))
    assert j.get('success'), '登录失败: ' + str(j)
except Exception as e:
    print('   ⚠️ 无法解析 JSON，尝试 GET 登录页获取 session')
    # 可能需要先访问登录页获取 csrf session
    s.get(f'{BASE}/login')
    r = s.post(f'{BASE}/login', json={'username': 'admin', 'password': 'admin123'})
    print('   → retry status:', r.status_code, 'text[:200]:', r.text[:200])
    j = r.json()
    assert j.get('success')

# 2. examples
print('\n[2] GET /api/transform/examples')
r = s.get(f'{BASE}/api/transform/examples')
d = r.json()
ex = d.get('examples', {})
print('   → success:', d.get('success'))
print('   → 条件示例:', len(ex.get('condition', [])), '条')
print('   → 脚本示例:', len(ex.get('rowScript', [])), '条')
print('   → 聚合示例:', len(ex.get('aggregation', [])), '条')
assert d.get('success')

# 3. validate - 正确条件
print('\n[3] POST /api/transform/validate (正确条件)')
rules = {'condition': "age >= 18 AND status = 'active' AND region IN ('CN','US')"}
r = s.post(f'{BASE}/api/transform/validate', json=rules)
d = r.json()
print('   → success:', d.get('success'), 'mode:', d.get('mode'))
if d.get('errors'): print('   → 错误:', d['errors'])
assert d.get('success')

# 4. validate - 坏条件
print('\n[4] POST /api/transform/validate (语法错误条件)')
rules = {'condition': 'amount > AND'}
r = s.post(f'{BASE}/api/transform/validate', json=rules)
d = r.json()
print('   → success:', d.get('success'), '错误数:', len(d.get('errors', [])))
assert not d.get('success') and len(d.get('errors', [])) > 0

# 5. validate - 正确脚本
print('\n[5] POST /api/transform/validate (正确脚本)')
rules = {'rowScript': '''
def filter_row(row):
    return row.get("status") == "active"

def transform_row(row):
    row["x"] = round(float(row.get("amount", 0)) * 1.1, 2)
    return row
'''.strip()}
r = s.post(f'{BASE}/api/transform/validate', json=rules)
d = r.json()
print('   → success:', d.get('success'))
if d.get('errors'): print('   → 错误:', d['errors'])
assert d.get('success')

# 6. validate - 脚本含 eval (必须失败)
print('\n[6] POST /api/transform/validate (脚本含 eval - 应拒绝)')
rules = {'rowScript': '''
def transform_row(row):
    eval("1+1")
    return row
'''.strip()}
r = s.post(f'{BASE}/api/transform/validate', json=rules)
d = r.json()
print('   → success:', d.get('success'), '错误数:', len(d.get('errors', [])))
print('   → 错误:', d.get('errors', [])[:1])
assert not d.get('success')

# 7. validate - 脚本含 import (必须拒绝)
print('\n[7] POST /api/transform/validate (脚本含 import - 应拒绝)')
rules = {'rowScript': '''
import os
def filter_row(row):
    return True
'''.strip()}
r = s.post(f'{BASE}/api/transform/validate', json=rules)
d = r.json()
print('   → success:', d.get('success'), '错误数:', len(d.get('errors', [])))
print('   → 错误:', d.get('errors', [])[:1])
assert not d.get('success')

# 8. validate - 正确聚合
print('\n[8] POST /api/transform/validate (正确聚合)')
rules = {'aggregation': {
    'groupBy': ['region', 'category'],
    'metrics': [
        {'source': 'amount', 'target': 'total_amt', 'func': 'SUM'},
        {'source': 'id', 'target': 'cnt', 'func': 'COUNT'},
        {'source': 'amount', 'target': 'avg_amt', 'func': 'AVG'},
    ]
}}
r = s.post(f'{BASE}/api/transform/validate', json=rules)
d = r.json()
print('   → success:', d.get('success'), 'mode:', d.get('mode'))
if d.get('errors'): print('   → 错误:', d['errors'])
assert d.get('success') and d.get('mode') == 'aggregation'

# 9. validate - 聚合未知函数
print('\n[9] POST /api/transform/validate (聚合含未知函数 - 应拒绝)')
rules = {'aggregation': {
    'groupBy': ['region'],
    'metrics': [{'source': 'amount', 'target': 'x', 'func': 'FOOBAR'}]
}}
r = s.post(f'{BASE}/api/transform/validate', json=rules)
d = r.json()
print('   → success:', d.get('success'), '错误数:', len(d.get('errors', [])))
assert not d.get('success')

# 10. validate - 三种规则混合
print('\n[10] POST /api/transform/validate (条件+脚本+聚合 混合)')
rules = {
    'condition': "amount > 0",
    'rowScript': 'def transform_row(row):\n    row["flag"]=1\n    return row',
    'aggregation': {
        'groupBy': ['region'],
        'metrics': [{'source': 'amount', 'target': 'total', 'func': 'SUM'}]
    }
}
r = s.post(f'{BASE}/api/transform/validate', json=rules)
d = r.json()
print('   → success:', d.get('success'), 'mode:', d.get('mode'))
if d.get('errors'): print('   → 错误:', d['errors'])
assert d.get('success') and d.get('mode') == 'aggregation'

# 11. validate - 空规则
print('\n[11] POST /api/transform/validate (空规则)')
r = s.post(f'{BASE}/api/transform/validate', json={})
d = r.json()
print('   → success:', d.get('success'), 'mode:', d.get('mode'))
assert d.get('success')

print('\n' + '=' * 70)
print('✅ 全部 11 项 API 测试通过！')
print('=' * 70)
