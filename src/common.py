"""Shared helpers: parameters, timing harness and synthetic data generators.

Every benchmark notebook imports this module (it sits next to the notebooks, so it is
importable from the notebook's working directory on both serverless and classic compute).
"""
import time
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta

from pyspark.sql import functions as F, types as T

DEFAULTS = {
    "catalog": "dante_classic_stable_catalog",
    "schema": "sm_bench",
    "market": "anz",                  # anz | usa
    "variant": "serverless",          # compute label recorded with every timing row
    "n_interval_meters": "100000",    # meters that report 5-minute interval data
    "n_platform_meters": "600000",    # all meters on the platform (conversations, register)
    "n_channels": "16",               # channels per interval record (ANZ 16, USA 21)
    "n_tenants": "12",
    "history_days": "7",
    "bench_run_id": "",
    "job_id": "",
    "job_run_id": "",
}
INT_KEYS = ("n_interval_meters", "n_platform_meters", "n_channels", "n_tenants", "history_days")

BUSINESS_SERVICES = ["Residential", "Commercial", "Industrial", "Solar", "Prepay", "Street Lighting", "Agriculture", "EV Charging"]
METER_TYPES = ["Single-phase", "Three-phase", "CT-connected", "Prepay", "Bi-directional"]
FIRMWARE = ["1.4.2", "1.4.3", "2.0.0", "2.0.1", "2.1.0", "2.2.0"]
COMMS = ["Cellular", "RF mesh", "PLC", "Ethernet"]
NOMINAL_V, BAND = 230.0, 0.10      # voltage band for excursion detection (+/-10%)
SLOTS_PER_DAY = 288                # 5-minute intervals


def params(dbutils, extra=None):
    """Declare notebook widgets (so they can be set as job parameters) and return typed values."""
    d = dict(DEFAULTS)
    d.update({k: str(v) for k, v in (extra or {}).items()})
    for k, v in d.items():
        dbutils.widgets.text(k, v)
    p = {k: dbutils.widgets.get(k) for k in d}
    for k in INT_KEYS + tuple(k for k in (extra or {}) if str(d[k]).lstrip("-").isdigit()):
        p[k] = int(p[k])
    if not p["bench_run_id"]:
        p["bench_run_id"] = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return p


def t(p, name):
    return f"{p['catalog']}.{p['schema']}.{name}"


def vol(p, *parts):
    return f"/Volumes/{p['catalog']}/{p['schema']}/landing/" + "/".join(parts)


def today():
    return datetime.now(timezone.utc).date()


def day_list(p):
    """The history_days most recent complete days, oldest first (as ISO strings)."""
    end = today()
    return [(end - timedelta(days=i)).isoformat() for i in range(p["history_days"], 0, -1)]


# ----------------------------------------------------------------------------- timing harness
_RESULT_SCHEMA = T.StructType([
    T.StructField("bench_run_id", T.StringType()), T.StructField("recorded_at", T.TimestampType()),
    T.StructField("market", T.StringType()), T.StructField("use_case", T.StringType()),
    T.StructField("variant", T.StringType()), T.StructField("step", T.StringType()),
    T.StructField("seconds", T.DoubleType()), T.StructField("rows_in", T.LongType()),
    T.StructField("rows_out", T.LongType()), T.StructField("n_interval_meters", T.LongType()),
    T.StructField("n_platform_meters", T.LongType()), T.StructField("job_id", T.StringType()),
    T.StructField("job_run_id", T.StringType()), T.StructField("note", T.StringType()),
])


class Bench:
    """Records wall-clock seconds per step into <catalog>.<schema>.bench_results."""

    def __init__(self, spark, p, use_case):
        self.spark, self.p, self.use_case = spark, p, use_case
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {p['catalog']}.{p['schema']}")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {p['catalog']}.{p['schema']}.landing")
        cols = ", ".join(f"{f.name} {f.dataType.simpleString().upper()}" for f in _RESULT_SCHEMA.fields)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {t(p, 'bench_results')} ({cols})")

    def record(self, step, seconds, rows_in=None, rows_out=None, note=""):
        p = self.p
        row = (p["bench_run_id"], datetime.now(timezone.utc), p["market"], self.use_case, p["variant"], step,
               float(seconds), rows_in, rows_out, p["n_interval_meters"], p["n_platform_meters"],
               p["job_id"], p["job_run_id"], note)
        self.spark.createDataFrame([row], _RESULT_SCHEMA).write.mode("append").saveAsTable(t(p, "bench_results"))
        print(f"[bench] {self.use_case}/{step}: {seconds:.1f}s rows_in={rows_in} rows_out={rows_out} {note}")

    @contextmanager
    def step(self, name, rows_in=None, note=""):
        ctx = {"rows_in": rows_in, "rows_out": None, "note": note}
        t0 = time.time()
        yield ctx
        self.record(name, time.time() - t0, ctx["rows_in"], ctx["rows_out"], ctx["note"])


def count_table(spark, table):
    return spark.table(table).count()


# ----------------------------------------------------------------------------- generators
def pick(seed_col, values):
    """Deterministically pick one of `values` from a hash of `seed_col`."""
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (F.pmod(F.hash(seed_col), F.lit(len(values))) + 1).cast("int"))


def transformer_of(meter_col):
    return (F.ceil(meter_col / F.lit(2.45))).cast("long")        # ~2.45 meters per transformer (USA assumption)


def meters_frame(spark, p):
    n, ni = p["n_platform_meters"], p["n_interval_meters"]
    n_feeders = max(1, round(ni / 780))
    n_subs = max(1, round(ni / 2270))
    m = spark.range(1, n + 1).withColumnRenamed("id", "meter_id")
    m = (m.withColumn("tenant_id", F.concat(F.lit("T"), F.lpad((F.col("meter_id") % p["n_tenants"]).cast("string"), 2, "0")))
          .withColumn("business_service", pick(F.col("meter_id") * 7, BUSINESS_SERVICES))
          .withColumn("meter_type", pick(F.col("meter_id") * 11, METER_TYPES))
          .withColumn("firmware", pick(F.col("meter_id") * 13, FIRMWARE))
          .withColumn("comms_type", pick(F.col("meter_id") * 17, COMMS))
          .withColumn("status", F.when(F.pmod(F.hash("meter_id"), F.lit(100)) < 97, "ACTIVE")
                                 .when(F.pmod(F.hash("meter_id"), F.lit(100)) < 98, "FAULT").otherwise("DISCONNECTED"))
          .withColumn("has_interval_data", F.col("meter_id") <= ni)
          .withColumn("transformer_id", transformer_of(F.col("meter_id")))
          .withColumn("feeder_id", F.pmod(F.col("transformer_id"), F.lit(n_feeders)))
          .withColumn("substation_id", F.pmod(F.col("transformer_id"), F.lit(n_subs))))
    return m


def interval_frame(spark, p, day, slot_lo=0, slot_hi=SLOTS_PER_DAY):
    """Interval readings for one day (slots [slot_lo, slot_hi)): one row per interval meter per slot.

    Built-in signals the use cases look for:
      * ~2% of reads FAILED (null channels); meters with id % 200 == 0 never report (poor comms)
      * meters with id % 50 == 0 run +12% over nominal 17:00-20:00 (voltage excursion)
      * transformers with id % 500 == 0 lose all meters during slots 100-110 on the last day (outage)
      * meters with id % 25 == 0 draw a +7 kW step 18:00-22:00 (EV-like); id % 20 == 0 export midday (PV-like)
    c01 = voltage, c02 = current, c03 = active power (kW), c04 = reactive power, c05.. = other channels.
    """
    ni, nc = p["n_interval_meters"], p["n_channels"]
    mi = spark.range(1, ni + 1).withColumnRenamed("id", "meter_id")
    slots = spark.range(slot_lo, slot_hi).withColumnRenamed("id", "slot")
    df = mi.crossJoin(slots)
    df = df.withColumn("reading_date", F.lit(day).cast("date"))
    df = df.withColumn("ts", F.timestamp_seconds(F.unix_timestamp(F.lit(day).cast("timestamp")) + F.col("slot") * 300))
    hour = (F.col("slot") / 12.0)
    noise = F.rand(11)
    failed = (noise < 0.02) | (F.col("meter_id") % 200 == 0)
    last_day = (day == day_list(p)[-1])
    if last_day:
        failed = failed | ((transformer_of(F.col("meter_id")) % 500 == 0) & F.col("slot").between(100, 110))
    volt = F.lit(NOMINAL_V) * (1 + (F.rand(13) - 0.5) * 0.04)
    volt = F.when((F.col("meter_id") % 50 == 0) & hour.between(17, 20), F.lit(NOMINAL_V * 1.12) * (1 + (F.rand(14) - 0.5) * 0.01)).otherwise(volt)
    load = 1.2 + 0.8 * F.sin((hour - 6) * 3.14159 / 12) ** 2 + F.rand(17) * 0.3
    load = load + F.when((F.col("meter_id") % 25 == 0) & hour.between(18, 22), 7.0).otherwise(0.0)
    load = load - F.when((F.col("meter_id") % 20 == 0) & hour.between(10, 14), 3.0).otherwise(0.0)
    df = df.withColumn("status", F.when(failed, "FAILED").otherwise("OK"))
    chans = {"c01": volt, "c02": load * 4.3, "c03": load, "c04": load * 0.2}
    for i in range(5, nc + 1):
        chans[f"c{i:02d}"] = (F.rand(100 + i) * 50) + F.col("meter_id") % 7
    for name, expr in chans.items():
        df = df.withColumn(name, F.when(failed, F.lit(None).cast("double")).otherwise(expr.cast("double")))
    return df.drop("slot")


def conversation_frame(spark, p, day, n_rows=None, slot_lo=0, slot_hi=SLOTS_PER_DAY):
    """Conversation log rows (one per conversation) for platform meters on `day`.

    Meters with id % 200 == 0 always fail (consistent with the interval generator). ~4% random failures.
    """
    n = p["n_platform_meters"]
    rows = n_rows if n_rows is not None else n * 6
    df = spark.range(rows).withColumnRenamed("id", "conversation_id")
    df = df.withColumn("meter_id", (F.col("conversation_id") % n) + 1)
    df = df.withColumn("tenant_id", F.concat(F.lit("T"), F.lpad((F.col("meter_id") % p["n_tenants"]).cast("string"), 2, "0")))
    slot = (F.rand(21) * (slot_hi - slot_lo) + slot_lo).cast("int")
    start = F.timestamp_seconds(F.unix_timestamp(F.lit(day).cast("timestamp")) + slot * 300 + (F.rand(22) * 299).cast("int"))
    df = df.withColumn("start_ts", start)
    df = df.withColumn("end_ts", F.col("start_ts") + F.expr("make_interval(0,0,0,0,0,0,1 + 20 * rand(23))"))
    fail = (F.rand(24) < 0.04) | (F.col("meter_id") % 200 == 0)
    df = df.withColumn("outcome", F.when(fail, "FAILED").otherwise("SUCCESS"))
    df = df.withColumn("failure_reason", F.when(fail, pick(F.col("conversation_id"), ["TIMEOUT", "NO_SIGNAL", "AUTH_ERROR", "BUSY", "PROTOCOL_ERROR"])))
    return df.withColumn("reading_date", F.lit(day).cast("date")).drop("conversation_id")


# ----------------------------------------------------------------------------- SQL warehouse helper
def run_on_warehouse(warehouse_id, statement, catalog=None, schema=None, timeout_s=600):
    """Execute one statement on a SQL warehouse via the Statement Execution API; returns (seconds, row_count).

    Wall-clock includes queueing and (for the first call after idle) warehouse start - callers record cold and warm separately.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
    w = WorkspaceClient()
    t0 = time.time()
    r = w.statement_execution.execute_statement(warehouse_id=warehouse_id, statement=statement, catalog=catalog,
                                                schema=schema, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() - t0 > timeout_s:
            raise TimeoutError(statement[:80])
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"{r.status.state}: {r.status.error.message if r.status.error else ''}\n{statement[:200]}")
    n = r.manifest.total_row_count if r.manifest else None
    return time.time() - t0, n
