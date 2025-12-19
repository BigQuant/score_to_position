"""score_to_position package.

仓位分配
"""
from datetime import datetime, timedelta
import json
import re
import uuid

import structlog

from bigmodule import I

# 需要安装的第三方依赖包
# from bigmodule import R
# R.require("requests>=2.0", "isort==5.13.2")

# metadata
# 模块作者
author = "BigQuant"
# 模块分类
category = "策略"
# 模块显示名
friendly_name = "仓位分配"
# 文档地址, optional
doc_url = "https://bigquant.com/wiki/doc/aistudio-HVwrgP4J1A#h-仓位分配"
# 是否自动缓存结果
cacheable = True

logger = structlog.get_logger()

DEFAULT_POSITION_EXPR = """-- DAI SQL 算子/函数: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-%E5%87%BD%E6%95%B0
-- 在这里输入表达式, 每行一个表达式, 输出仓位字段必须命名为 position, 模块会进一步做归一化
-- 排序倒数: 1 / score_rank AS position
-- 对数下降: 1 / log2(score_rank + 1) AS position
-- TODO 拟合、最优化 ..
 
-- 等权重分配
1 AS position
"""

SQL_TEMPLATE = '''
WITH {t_hold_table_id} AS (
SELECT
    {base_table_id}.* EXCLUDE(position),
    ROW_NUMBER() OVER (PARTITION BY date ORDER BY {score_field}, instrument) AS score_rank
FROM {base_table_id}
{hold_count_filter}
),
{t_position_table_id} AS (
SELECT
    {t_hold_table_id}.* EXCLUDE(position),
    {expr}
FROM {tables}
ORDER BY date, position
)
SELECT
    * EXCLUDE(position),
    {position_norm}
FROM {t_position_table_id}
QUALIFY
    position IS NOT NULL
ORDER BY date, position, instrument
'''

REMOVE_STRING_RE = re.compile(r"'[^']*'")
TABLE_NAME_RE = re.compile(r"(?<!\.)\b[a-zA-Z_]\w*\b(?=\.[a-zA-Z_*])")


def _ds_to_table(ds) -> dict:
    if isinstance(ds, str):
        sql = ds
    else:
        type_ = ds.type
        if type_ == "json":
            sql = ds.read()["sql"]
        elif type == "text":
            sql = ds.read()
        else:
            # bdb
            return {"sql": "", "table_id": ds.id}

    import bigdb

    table_id = f"_t_{uuid.uuid4().hex}"
    parts = [x.strip().strip(";") for x in bigdb.connect().parse_query(sql)]
    parts[-1] = f"CREATE TABLE {table_id} AS {parts[-1]}"
    sql = ";\n".join(parts)
    if sql:
        sql += ";\n"

    return {
        "sql": sql,
        "table_id": table_id,
    }


def _ds_to_tables(inputs) -> dict:
    sql = ""
    tables = []
    input_tables = []
    for i, x in enumerate(inputs):
        if x is None:
            continue
        table = _ds_to_table(x)
        table["name"] = f"input_{i+1}"
        tables.append(table)

    return {
        "items": tables,
        "map": {x["name"]: x for x in tables},
        "sql": "".join([x["sql"] for x in tables]),
    }


def _split_expr(expr):
    lines = []
    for line in expr.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("--") or line.startswith("#"):
            continue
        lines.append(line)

    return lines


def _build_sql_from_expr(expr: str, hold_count, total_position, score_field, input_tables):
    expr_lines = _split_expr(expr)

    # collect all tables, join them
    tables = [x["table_id"] for x in input_tables["items"]]
    # 第一个输入数据源的 score 用于生成 score_rank，并只保留 score_rank <= hold_count 的行，得到一个新表 t_hold_table_id
    base_table_id = tables[0]
    t_hold_table_id = f"_t_{uuid.uuid4().hex}"
    tables.append(t_hold_table_id)

    for line in expr_lines:
        tables += TABLE_NAME_RE.findall(REMOVE_STRING_RE.sub('', line))
    # de-dup and add using primary key
    table_set = set()
    table_list = []
    for x in tables:
        if x in input_tables["map"]:
            x = input_tables["map"][x]["table_id"]
        if x not in table_set:
            table_list.append(x)
        table_set.add(x)
    for i in range(1, len(table_list)):
        table_list[i] += " USING(date, instrument)"

    # 持仓数量
    hold_count_filter = ""
    if hold_count != 0:
        hold_count_filter = f"QUALIFY\n    score_rank <= {hold_count}"

    # 总仓位
    if total_position == 0:
        position_norm = "position"
    else:
        position_norm = f"c_sum(position) AS _position_sum,\n    position * {total_position} / CASE WHEN _position_sum = 0 THEN 1 ELSE _position_sum END AS position"

    return SQL_TEMPLATE.format(
        base_table_id=base_table_id,
        t_hold_table_id=t_hold_table_id,
        tables="\n    JOIN ".join(table_list),
        hold_count_filter=hold_count_filter,
        expr=",\n    ".join(expr_lines),
        position_norm=position_norm,
        score_field=score_field,
        t_position_table_id=f"_t_{uuid.uuid4().hex}",
    )

def _create_ds_from_sql(sql: str, extract_data: bool, base_ds=None):
    import dai

    if extract_data:
        logger.info("extract data ..")
        filters = {}
        try:
            if base_ds:
                extra = base_ds._extra()
                extra = json.loads(extra)
                # TODO: 支持向前去数据天数
                if "start_date" in extra and "end_date" in extra:
                    query_start_date = extra["start_date"]
                    if "before_start_days" in extra:
                        before_start_days = extra["before_start_days"]
                        date_format = "%Y-%m-%d"
                        query_start_date = (datetime.strptime(query_start_date, date_format) - timedelta(days=before_start_days)).strftime(date_format)

                    filters["date"] = [query_start_date, extra["end_date"]]
                    logger.info(f"filters {filters}")
        except:
            pass

        try:
            df = dai.query(sql, filters=filters).df()
        except:
            logger.error(f"dai query failed: {sql}")
            raise
        logger.info(f"extracted {df.shape}.")
        ds = dai.DataSource.write_bdb(df, base_ds=base_ds)
    else:
        ds = dai.DataSource.write_json({"sql": sql}, base_ds=base_ds)

    return ds


def run(
    input_1: I.port("数据输入1，score数据输出，如果有metadata extra，会传递给输出 data", specific_type_name="DataSource", optional=True) = None,
    input_2: I.port("数据输入2，辅助数据，可在仓位表达式中使用，可选", specific_type_name="DataSource", optional=True) = None,
    input_3: I.port("数据输入3，辅助数据，可在仓位表达式中使用，可选", specific_type_name="DataSource", optional=True) = None,
    score_field: I.str("评分score字段排序，来自输入1的score字段，score ASC 表示 按score字段从小到大排序, score DESC 表示从大到小. 如果不需要按评分排序, 输入0") = "score ASC",
    hold_count: I.int("持仓股票数量，按分数score从低到高排序，保留分数低的, 0表示不限制数量, 传入数据已经做了过滤") = 10,
    position_expr: I.code(
        "仓位公式，仓位表达式公式，e.g. 1 / rank_score",
        default=DEFAULT_POSITION_EXPR,
        auto_complete_type="sql",
        optional=False,
    ) = None,
    total_position: I.float("总仓位，总仓位为1 则归一化到1，为0.5 则总仓位scale到0.5，如果为0 则不做归一化") = 1,
    extract_data: I.bool("抽取数据，是否抽取数据，如果抽取数据，将返回一个BDB DataSource，包含数据DataFrame") = True,
) -> [I.port("输出(SQL文件)", "data")]:
    """输入特征（因子）数据"""

    input_tables = _ds_to_tables([input_1, input_2, input_3])

    sql = _build_sql_from_expr(position_expr, hold_count, total_position, score_field, input_tables=input_tables)

    # 替换 input_*
    for x in input_tables["items"]:
        sql = re.sub(rf'\b{x["name"]}\b', x["table_id"], sql)
    sql = input_tables["sql"] + sql

    # follow 第一个 input ds 的 extra
    return I.Outputs(data=_create_ds_from_sql(sql, extract_data, input_1))


def post_run(outputs):
    """后置运行函数"""
    return outputs
