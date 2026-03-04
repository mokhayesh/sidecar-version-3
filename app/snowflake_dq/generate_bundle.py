from __future__ import annotations
import os, json
from datetime import datetime

def _qident(name: str) -> str:
    return '"' + name.replace('"','""') + '"'

def generate_snowflake_dq_bundle(bundle: dict, out_dir: str) -> str:
    """Generate Snowflake DQ framework SQL files driven by bundle (datasets/columns/rules). Returns output folder."""
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_prefix = os.path.join(out_dir, f"snowflake_dq_{ts}")
    os.makedirs(out_prefix, exist_ok=True)

    # save input
    with open(os.path.join(out_prefix, "00_input_bundle.json"), "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)

    target = bundle.get("target", {}) or {}
    dq_db = target.get("database") or ""
    dq_schema = target.get("schema") or "DQ"
    db_prefix = f"{_qident(dq_db)}." if dq_db else ""
    schema_fqn = f"{db_prefix}{_qident(dq_schema)}"

    core_tables = f"""
CREATE SCHEMA IF NOT EXISTS {schema_fqn};

CREATE TABLE IF NOT EXISTS {schema_fqn}.DQ_RULES_CONFIG (
  RULE_ID STRING NOT NULL,
  RULE_TYPE STRING NOT NULL,
  RULE_PARAMS VARIANT,
  SEVERITY STRING DEFAULT 'ERROR',
  IS_ACTIVE BOOLEAN DEFAULT TRUE,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS {schema_fqn}.DQ_DATASETS (
  DATASET_ID STRING NOT NULL,
  TABLE_FQN STRING NOT NULL,
  PRIMARY_KEYS ARRAY,
  IS_ACTIVE BOOLEAN DEFAULT TRUE,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_DQ_DATASETS PRIMARY KEY (DATASET_ID)
);

CREATE TABLE IF NOT EXISTS {schema_fqn}.DQ_EXPECTATION_BINDINGS (
  BINDING_ID STRING NOT NULL,
  DATASET_ID STRING NOT NULL,
  COLUMN_NAME STRING NOT NULL,
  RULE_ID STRING NOT NULL,
  APPLY_WHERE STRING,
  THRESHOLD FLOAT,
  IS_ACTIVE BOOLEAN DEFAULT TRUE,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_DQ_BINDINGS PRIMARY KEY (BINDING_ID)
);

CREATE TABLE IF NOT EXISTS {schema_fqn}.DQ_RUN_AUDIT (
  RUN_ID STRING NOT NULL,
  STARTED_AT TIMESTAMP_NTZ NOT NULL,
  FINISHED_AT TIMESTAMP_NTZ,
  STATUS STRING NOT NULL,
  TRIGGERED_BY STRING,
  CONTEXT VARIANT,
  ERROR_MESSAGE STRING,
  CONSTRAINT PK_DQ_RUN_AUDIT PRIMARY KEY (RUN_ID)
);

CREATE TABLE IF NOT EXISTS {schema_fqn}.DQ_CHECK_RESULTS (
  RUN_ID STRING NOT NULL,
  DATASET_ID STRING NOT NULL,
  TABLE_FQN STRING NOT NULL,
  COLUMN_NAME STRING NOT NULL,
  RULE_ID STRING NOT NULL,
  RULE_TYPE STRING NOT NULL,
  SEVERITY STRING NOT NULL,
  CHECK_TS TIMESTAMP_NTZ NOT NULL,
  ROWS_EVALUATED NUMBER,
  FAIL_COUNT NUMBER,
  FAIL_RATE FLOAT,
  PASS_FLAG BOOLEAN,
  SAMPLE_FAILS VARIANT,
  DETAILS VARIANT
);
""".strip()

    metric_udfs = f"""
CREATE OR REPLACE FUNCTION {schema_fqn}.DQ_FAIL_RATE(fail_count NUMBER, rows_evaluated NUMBER)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
  IFF(rows_evaluated IS NULL OR rows_evaluated = 0, NULL, fail_count / rows_evaluated)
$$;

CREATE OR REPLACE FUNCTION {schema_fqn}.DQ_PASS_FLAG(fail_count NUMBER, threshold FLOAT)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
  IFF(threshold IS NULL,
      IFF(fail_count = 0, TRUE, FALSE),
      IFF(fail_count <= threshold, TRUE, FALSE))
$$;
""".strip()

    # seed config from bundle
    stmts = ["BEGIN;"]
    for ds in (bundle.get("datasets") or []):
        dsid = ds.get("dataset_id") or "dataset"
        table = ds.get("table_fqn") or ""
        pks = ds.get("primary_keys") or []
        stmts.append(f"""MERGE INTO {schema_fqn}.DQ_DATASETS t
USING (SELECT '{dsid}' AS DATASET_ID, '{table}' AS TABLE_FQN, PARSE_JSON('{json.dumps(pks)}') AS PRIMARY_KEYS) s
ON t.DATASET_ID = s.DATASET_ID
WHEN MATCHED THEN UPDATE SET TABLE_FQN=s.TABLE_FQN, PRIMARY_KEYS=s.PRIMARY_KEYS, UPDATED_AT=CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (DATASET_ID, TABLE_FQN, PRIMARY_KEYS) VALUES (s.DATASET_ID, s.TABLE_FQN, s.PRIMARY_KEYS);""")
        for col in (ds.get("columns") or []):
            cname = col.get("name")
            for i, rule in enumerate(col.get("rules") or [], start=1):
                rtype = rule.get("rule_type")
                rid = f"{dsid}__{cname}__{rtype}__{i}".lower()
                bid = f"b__{rid}"
                severity = rule.get("severity","ERROR")
                params = {k:v for k,v in rule.items() if k not in ("rule_type","severity")}
                stmts.append(f"""MERGE INTO {schema_fqn}.DQ_RULES_CONFIG t
USING (SELECT '{rid}' AS RULE_ID, '{rtype}' AS RULE_TYPE, PARSE_JSON('{json.dumps(params)}') AS RULE_PARAMS, '{severity}' AS SEVERITY) s
ON t.RULE_ID = s.RULE_ID
WHEN MATCHED THEN UPDATE SET RULE_TYPE=s.RULE_TYPE, RULE_PARAMS=s.RULE_PARAMS, SEVERITY=s.SEVERITY, UPDATED_AT=CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (RULE_ID, RULE_TYPE, RULE_PARAMS, SEVERITY) VALUES (s.RULE_ID, s.RULE_TYPE, s.RULE_PARAMS, s.SEVERITY);""")
                stmts.append(f"""MERGE INTO {schema_fqn}.DQ_EXPECTATION_BINDINGS t
USING (SELECT '{bid}' AS BINDING_ID, '{dsid}' AS DATASET_ID, '{cname}' AS COLUMN_NAME, '{rid}' AS RULE_ID, NULL AS APPLY_WHERE, NULL AS THRESHOLD) s
ON t.BINDING_ID = s.BINDING_ID
WHEN MATCHED THEN UPDATE SET DATASET_ID=s.DATASET_ID, COLUMN_NAME=s.COLUMN_NAME, RULE_ID=s.RULE_ID, UPDATED_AT=CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (BINDING_ID, DATASET_ID, COLUMN_NAME, RULE_ID, APPLY_WHERE, THRESHOLD)
VALUES (s.BINDING_ID, s.DATASET_ID, s.COLUMN_NAME, s.RULE_ID, s.APPLY_WHERE, s.THRESHOLD);""")
    stmts.append("COMMIT;")
    config_seed = "\n\n".join(stmts)

    stored_procs = f"""
CREATE OR REPLACE PROCEDURE {schema_fqn}.SP_DQ_RUN(run_id STRING, triggered_by STRING, context VARIANT)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var runId = RUN_ID;
try {{
  snowflake.execute({{
    sqlText: `INSERT INTO {schema_fqn}.DQ_RUN_AUDIT (RUN_ID, STARTED_AT, STATUS, TRIGGERED_BY, CONTEXT)
             VALUES (?, CURRENT_TIMESTAMP(), 'STARTED', ?, ?)`,
    binds: [runId, TRIGGERED_BY, CONTEXT]
  }});

  var rs = snowflake.execute({{
    sqlText: `
      SELECT b.DATASET_ID, d.TABLE_FQN, b.COLUMN_NAME,
             r.RULE_ID, r.RULE_TYPE, r.RULE_PARAMS, r.SEVERITY
      FROM {schema_fqn}.DQ_EXPECTATION_BINDINGS b
      JOIN {schema_fqn}.DQ_RULES_CONFIG r ON b.RULE_ID = r.RULE_ID
      JOIN {schema_fqn}.DQ_DATASETS d ON b.DATASET_ID = d.DATASET_ID
      WHERE b.IS_ACTIVE=TRUE AND r.IS_ACTIVE=TRUE AND d.IS_ACTIVE=TRUE
    `
  }});

  while (rs.next()) {{
    var datasetId = rs.getColumnValue(1);
    var tableFqn  = rs.getColumnValue(2);
    var colName   = rs.getColumnValue(3);
    var ruleId    = rs.getColumnValue(4);
    var ruleType  = rs.getColumnValue(5);
    var ruleParams= rs.getColumnValue(6);
    var severity  = rs.getColumnValue(7);

    var failPredicate = null;
    if (ruleType === 'NOT_NULL') {{
      failPredicate = `${colName} IS NULL`;
    }} else if (ruleType === 'REGEX') {{
      var pattern = ruleParams["pattern"];
      failPredicate = `NOT REGEXP_LIKE(${colName}, '${pattern}')`;
    }} else {{
      continue;
    }}

    var sqlCounts = `
      SELECT COUNT(*) AS ROWS_EVALUATED,
             SUM(IFF(${failPredicate}, 1, 0)) AS FAIL_COUNT
      FROM ${tableFqn}
    `;
    var rs2 = snowflake.execute({{ sqlText: sqlCounts }});
    rs2.next();
    var rowsEval = rs2.getColumnValue(1);
    var failCnt  = rs2.getColumnValue(2);

    snowflake.execute({{
      sqlText: `
        INSERT INTO {schema_fqn}.DQ_CHECK_RESULTS
        (RUN_ID, DATASET_ID, TABLE_FQN, COLUMN_NAME, RULE_ID, RULE_TYPE, SEVERITY, CHECK_TS,
         ROWS_EVALUATED, FAIL_COUNT, FAIL_RATE, PASS_FLAG, DETAILS)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(),
                ?, ?, {schema_fqn}.DQ_FAIL_RATE(?, ?), {schema_fqn}.DQ_PASS_FLAG(?, NULL),
                PARSE_JSON(?))
      `,
      binds: [runId, datasetId, tableFqn, colName, ruleId, ruleType, severity,
              rowsEval, failCnt, failCnt, rowsEval, JSON.stringify({{"rule_params": ruleParams}})]
    }});
  }}

  snowflake.execute({{
    sqlText: `UPDATE {schema_fqn}.DQ_RUN_AUDIT
             SET FINISHED_AT=CURRENT_TIMESTAMP(), STATUS='SUCCEEDED'
             WHERE RUN_ID=?`,
    binds: [runId]
  }});
  return runId;
}} catch (err) {{
  snowflake.execute({{
    sqlText: `UPDATE {schema_fqn}.DQ_RUN_AUDIT
             SET FINISHED_AT=CURRENT_TIMESTAMP(), STATUS='FAILED', ERROR_MESSAGE=?
             WHERE RUN_ID=?`,
    binds: [err.message, runId]
  }});
  throw err;
}}
$$;
""".strip()

    runbook = f"""
-- Run order:
-- 1) 01_core_tables.sql
-- 2) 02_metric_udfs.sql
-- 3) 03_config_seed.sql
-- 4) 04_stored_procs.sql

CALL {schema_fqn}.SP_DQ_RUN(UUID_STRING(), CURRENT_USER(), PARSE_JSON('{{"source":"sidecar-ui"}}'));

SELECT * FROM {schema_fqn}.DQ_RUN_AUDIT ORDER BY STARTED_AT DESC;
SELECT * FROM {schema_fqn}.DQ_CHECK_RESULTS ORDER BY CHECK_TS DESC;
""".strip()

    files = [
        ("01_core_tables.sql", core_tables),
        ("02_metric_udfs.sql", metric_udfs),
        ("03_config_seed.sql", config_seed),
        ("04_stored_procs.sql", stored_procs),
        ("05_runbook.sql", runbook),
    ]
    for fn, content in files:
        with open(os.path.join(out_prefix, fn), "w", encoding="utf-8") as f:
            f.write(content + "\n")

    return out_prefix
