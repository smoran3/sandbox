import csv
import hashlib
import io
import os
import zipfile

import psycopg2 as psql

# GTFS Specification has NULL constraints, which SEPTA doesn't give a flying fuck about
# So any field that is SUPPOSED to be NOT NULL has a trailing comment

GTFS_SCHEMA = {
    "agency": [
        ("agency_id",               str,    "TEXT"), # NN
        ("agency_name",             str,    "TEXT"), # NN
        ("agency_url",              str,    "TEXT"), # NN
        ("agency_timezone",         str,    "TEXT"), # NN
        ("agency_lang",             str,    "TEXT"),
        ("agency_phone",            str,    "TEXT"),
        ("agency_fare_url",         str,    "TEXT"),
        ("agency_email",            str,    "TEXT"),
    ],
    "stops": [
        ("stop_id",                 str,    "TEXT"), # NN
        ("stop_code",               str,    "TEXT"),
        ("stop_name",               str,    "TEXT"), # NN
        ("stop_desc",               str,    "TEXT"),
        ("stop_lat",                float,  "REAL"), # NN
        ("stop_lon",                float,  "REAL"), # NN
        ("zone_id",                 str,    "TEXT"),
        ("stop_url",                str,    "TEXT"),
        ("location_type",           int,    "SMALLINT"),
        ("parent_station",          str,    "TEXT"),
        ("stop_timezone",           str,    "TEXT"),
        ("wheelchair_boarding",     int,    "SMALLINT"),
    ],
    "routes": [
        ("route_id",                str,    "TEXT"), # NN
        ("agency_id",               str,    "TEXT"),
        ("route_short_name",        str,    "TEXT"), # NN
        ("route_long_name",         str,    "TEXT"), # NN
        ("route_desc",              str,    "TEXT"),
        ("route_type",              int,    "SMALLINT"), # NN
        ("route_url",               str,    "TEXT"),
        ("route_color",             str,    "TEXT"),
        ("route_text_color",        str,    "TEXT"),
        ("route_sort_order",        str,    "TEXT"),
    ],
    "trips": [
        ("route_id",                str,    "TEXT"), # NN
        ("service_id",              str,    "TEXT"), # NN
        ("trip_id",                 str,    "TEXT"), # NN
        ("trip_headsign",           str,    "TEXT"),
        ("trip_short_name",         str,    "TEXT"),
        ("direction_id",            int,    "SMALLINT"),
        ("block_id",                str,    "TEXT"),
        ("shape_id",                str,    "TEXT"),
        ("wheelchair_accessible",   int,    "SMALLINT"),
        ("bikes_allowed",           int,    "SMALLINT"),
    ],
    "stop_times": [
        ("trip_id",                 str,    "TEXT"), # NN
        ("arrival_time",            str,    "INTERVAL"), # NN
        ("departure_time",          str,    "INTERVAL"), # NN
        ("stop_id",                 str,    "TEXT"), # NN
        ("stop_sequence",           int,    "SMALLINT"), # NN
        ("stop_headsign",           str,    "TEXT"),
        ("pickup_type",             int,    "SMALLINT"),
        ("drop_off_type",           int,    "SMALLINT"),
        ("shape_dist_traveled",     float,  "REAL"),
        ("timepoint",               int,    "SMALLINT"),
    ],
    "calendar": [
        ("service_id",              str,    "TEXT"), # NN
        ("monday",                  int,    "SMALLINT"), # NN
        ("tuesday",                 int,    "SMALLINT"), # NN
        ("wednesday",               int,    "SMALLINT"), # NN
        ("thursday",                int,    "SMALLINT"), # NN
        ("friday",                  int,    "SMALLINT"), # NN
        ("saturday",                int,    "SMALLINT"), # NN
        ("sunday",                  int,    "SMALLINT"), # NN
        ("start_date",              str,    "TIMESTAMP"),
        ("end_date",                str,    "TIMESTAMP"),
    ],
    "calendar_dates": [
        ("service_id",              str,    "TEXT"), # NN
        ("date",                    str,    "TIMESTAMP"), # NN
        ("exception_type",          int,    "SMALLINT"), # NN
    ],
    "shapes": [
        ("shape_id",                str,    "TEXT"), # NN
        ("shape_pt_lat",            float,  "DOUBLE PRECISION"), # NN
        ("shape_pt_lon",            float,  "DOUBLE PRECISION"), # NN
        ("shape_pt_sequence",       int,    "SMALLINT"), # NN
        ("shape_dist_traveled",     float,  "REAL"),
    ],
}


#####UPDATE TABLE NAMES FOR DIFFERENT IMPORTS - RAIL VS BUS#####
SQL_CREATE = "CREATE TABLE IF NOT EXISTS gtfs_%s (gtfs_id SMALLINT NOT NULL, %s)"
SQL_INSERT = "INSERT INTO gtfs_%s (gtfs_id,%s) VALUES (%%s,%s)"

def filter_gtfs_table(header, data):
    return zip(*filter(lambda col:col[0].strip() in header, zip(*data)))

def prepend_gtfs_id(gtfs_id, data):
    for row in data:
        yield [gtfs_id] + list(row)

def _parseZip(path):
    gtfs_data = {}
    with zipfile.ZipFile(path) as zf:
        for fn in zf.namelist():
            gtfs_table, _ = os.path.splitext(fn)
            gtfs_table = gtfs_table.lower().strip()
            if gtfs_table in GTFS_SCHEMA:
                with zf.open(fn) as f:
                    r = csv.reader(f)
                    gtfs_data[gtfs_table] = filter_gtfs_table(zip(*GTFS_SCHEMA[gtfs_table])[0], [row for row in r])
    return gtfs_data

def parseZip(path):
    hash = hashlib.md5()
    with open(path, "rb") as f:
        hash.update(f.read())
    return hash.hexdigest(), _parseZip(path)

def parseNestedZip(path):
    with zipfile.ZipFile(path) as zf:
        for subzip in zf.namelist():
            print subzip,
            szf = io.BytesIO(zf.read(subzip))
            hash = hashlib.sha256()
            hash.update(szf.read())
            szf.seek(0)
            gtfs_data = _parseZip(szf)
            yield hash.hexdigest(), _parseZip(szf)

def parseSEPTAZip(con, path):
    for hash, gtfs_data in parseNestedZip(path):
        gtfs_id = checkGTFSHash(con, hash)
        if gtfs_id is None:
            print "Exists"
            continue
        print gtfs_id
        insertGTFS(con, gtfs_id, gtfs_data)

def checkGTFSHash(con, hash):
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS gtfs_meta (gtfs_id SMALLINT, hash TEXT)")
    cur.execute("SELECT gtfs_id FROM gtfs_meta WHERE hash = %s", (hash,))
    if cur.fetchone() is None:
        cur.execute("SELECT max(gtfs_id) FROM gtfs_meta")
        (max_gtfs_id,) = cur.fetchone()
        if max_gtfs_id is None:
            max_gtfs_id = 0
        gtfs_id = max_gtfs_id + 1
        cur.execute("INSERT INTO gtfs_meta VALUES (%s, %s);", (gtfs_id, hash))
        con.commit()
        return gtfs_id
    else:
        return None

def insertGTFS(con, gtfs_id, gtfs_data):
    cur = con.cursor()
    for gtfs_table, _field_defs in GTFS_SCHEMA.iteritems():
        field_names, python_types, postgres_types = zip(*_field_defs)
        field_defs = zip(field_names, postgres_types)
        cur.execute(SQL_CREATE % (gtfs_table, ",".join("%s %s" % (f, d) for f, d in field_defs)))
    con.commit()
    for gtfs_table, table in gtfs_data.iteritems():
        _insertGTFSTable(con, gtfs_id, gtfs_table, table)

def _tryCasting(value, dtype, default = None):
    retval = default
    try:
        retval = dtype(value)
    except:
        pass
    return retval

def _insertGTFSTable(con, gtfs_id, gtfs_table, table):
    cur = con.cursor()
    header = map(lambda s:s.lower().strip(), table.pop(0))
    # Empty table check after removing header
    if len(table) > 0:
        table = map(lambda row:map(lambda s:s.strip(), row), table)
        field_names, python_types, postgres_types = zip(*GTFS_SCHEMA[gtfs_table])
        casting_dict = dict(zip(field_names, python_types))
        _table = zip(*table)
        # _table = zip(*filter(lambda r:len(r) == len(header), table))
        # print gtfs_table, len(table), len(_table), len(header)
        for i, field in enumerate(header):
            _table[i] = map(lambda v:_tryCasting(v, casting_dict[field]), _table[i])
        table = zip(*_table)
        cur.executemany(
            SQL_INSERT % (gtfs_table, ",".join(header), ",".join("%s" for _ in header)),
            prepend_gtfs_id(gtfs_id, table)
        )
        con.commit()

if __name__ == "__main__":

    con = psql.connect(
        #daisy's ip address = 10.1.1.190
        host = "10.1.1.190",
        port = 5432,
        database = "septagtf_20191030",
        user = "postgres",
        password = "sergt"
    )

    root = r"D:\dvrpc_shared\Sandbox\FY21_UCity\GTFS"
    

    for septafeed in os.listdir(root):
        if septafeed.endswith('.zip'):
            print septafeed
            parseSEPTAZip(con, os.path.join(root, septafeed))