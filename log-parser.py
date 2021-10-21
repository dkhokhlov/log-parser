import os
import re
import sys
import resource
import click
import pygrok
import statsd
import sqlite3
import urllib.parse

APP_NAME = 'log-parser'
__version__ = '0.1'

PATTERN_LINE = ('%{IPORHOST:clientip} - %{USER:auth} \[%{HTTPDATE:timestamp}\] '
                '"%{WORD:method} %{NOTSPACE:path} HTTP/%{NUMBER:httpversion}" '
                '%{NUMBER:status_code:int} %{NUMBER:response_time:int} %{QS:agent}')
PATTERN_DATE = '.+ (\+|\-)\d\d\d\d'
PATTERN_PATH = "[A-Za-z0-9\-._~%!$&'()*+,;=:@/]*"

DB_PATH = f"/tmp/{APP_NAME}.db"

HTTP_METHOD = {"OPTIONS", "GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "TRACE", "PATCH"}
HTTP_VERSION = {"1.0", "1.1"}

MAX_LINE_LENGTH = 1024 * 512  # 512KB


class LineRec:
    IP = 0
    PATH = 1
    TIME = 2


class PathRec:
    PATH = 0
    AVG_TIME = 1


class IpRec:
    IP = 0
    CNT = 1


class Parser:

    def __init__(self, in_file_path, max_client_ips, max_paths):
        self.in_file = open(in_file_path, 'r')
        self.parser = pygrok.Grok(PATTERN_LINE, fullmatch=True)
        self.regex_date = re.compile(PATTERN_DATE)
        self.regex_path = re.compile(PATTERN_PATH)
        self.max_client_ips = max_client_ips
        self.max_paths = max_paths
        self.total_number_of_lines_processed = 0
        self.total_number_of_lines_ok = 0
        self.total_number_of_lines_failed = 0
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        self.db = sqlite3.connect(DB_PATH)
        self.cur = self.db.cursor()
        self.cur.execute("CREATE TABLE top_client_ips (ip TEXT PRIMARY KEY, cnt INT DEFAULT 1)")
        self.cur.execute(
            "CREATE TABLE top_path_avg_seconds (path TEXT PRIMARY KEY, time INT, total_time INT DEFAULT 0, cnt INT DEFAULT 1)")
        self.cur.execute("PRAGMA temp_store = FILE")
        self.cur.execute("PRAGMA locking_mode = EXCLUSIVE")
        self.cur.execute("PRAGMA cache_size = -500")  # 500KB
        #        self.cur.execute("PRAGMA hard_heap_limit = 3000000") # bytes
        #        self.cur.execute("PRAGMA soft_heap_limit = 2000000")
        statsd_server = os.getenv('STATSD_SERVER', '127.0.0.1:8125')
        try:
            host, port = statsd_server.split(':')
            self.statsd = statsd.StatsClient(host, port, APP_NAME)
        except Exception:
            print(f'failed to create client for statsd server: {statsd_server}')
            self.statsd = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        try:
            self.parser = None
            if self.cur:
                self.cur.close()
                self.cur = None
            if self.db:
                self.db.close()
                self.db = None
            if os.path.exists(DB_PATH):
                os.remove(DB_PATH)
        except Exception:
            pass

    def read_next_line_gen(self):
        while True:
            line = self.in_file.readline(MAX_LINE_LENGTH)
            if len(line) == 0:
                break
            if len(line) == MAX_LINE_LENGTH:
                while len(line) == MAX_LINE_LENGTH:
                    line = self.in_file.readline(MAX_LINE_LENGTH)
                yield None  # line is too big
            yield line

    def parse_line(self, line: str):
        """
        Returns: path line record tuple = (clientip, request, response_time)
        """
        line_map = self.parser.match(line.rstrip())
        if line_map and self.is_valid_line(line_map):
            line_rec = (line_map['clientip'], line_map['path'], line_map['response_time'])
        else:
            line_rec = None
        return line_rec

    def is_valid_line(self, line_map):
        #          100 <= line_map['status_code'] < 600 and \
        is_valid = line_map['method'] in HTTP_METHOD and \
                   line_map['httpversion'] in HTTP_VERSION and \
                   self.regex_date.match(line_map['timestamp']) and \
                   self.regex_path.match(line_map['path']) and \
                   len(line_map['agent']) > 0 and \
                   line_map['response_time'] >= 0
#        if not is_valid:
#            print(f'not valid: {line_map}')
        return is_valid

    def update_path_avg_seconds(self, line_rec):
        self.cur.execute(f'''INSERT INTO top_path_avg_seconds(path, time, total_time) 
                             VALUES("{line_rec[LineRec.PATH]}", {line_rec[LineRec.TIME]}, {line_rec[LineRec.TIME]})
                             ON CONFLICT(path) DO UPDATE SET total_time=total_time+excluded.time, cnt=cnt+1''')

    def update_top_client_ips(self, line_rec):
        self.cur.execute(f'''INSERT INTO top_client_ips(ip) VALUES('{line_rec[LineRec.IP]}')
                             ON CONFLICT(ip) DO UPDATE SET cnt=cnt+1''')

    def update_stats(self, line_rec):
        self.total_number_of_lines_processed += 1
        if not line_rec:
            self.total_number_of_lines_failed += 1
            return
        self.total_number_of_lines_ok += 1
        if self.max_paths:
            self.update_path_avg_seconds(line_rec)
        if self.max_client_ips:
            self.update_top_client_ips(line_rec)
        if self.total_number_of_lines_ok > 500:
            self.db.commit()
            self.cur.execute("PRAGMA shrink_memory")

    def write_report(self, file):
        file.write(f'''{{
    "total_number_of_lines_processed": {self.total_number_of_lines_processed},
    "total_number_of_lines_ok": {self.total_number_of_lines_ok},
    "total_number_of_lines_failed": {self.total_number_of_lines_failed},
    "top_client_ips": {{
''')
        # top_client_ips
        self.cur.execute(f'SELECT ip, cnt FROM top_client_ips ORDER BY cnt DESC LIMIT {self.max_client_ips}')
        rows = self.cur.fetchall()
        # rows.reverse()
        for i, rec in enumerate(rows):
            if i != 0: file.write('\n')
            file.write(f'       "{rec[IpRec.IP]}": {rec[IpRec.CNT]}')
            if i < len(rows) - 1: file.write(',')
        file.write(f'''
    }},
    "top_path_avg_seconds": {{
''')
        # top_path_avg_seconds
        self.cur.execute(
            f'SELECT path, CAST(total_time AS REAL)/cnt FROM top_path_avg_seconds ORDER BY CAST(total_time AS REAL)/cnt DESC LIMIT {self.max_paths}')
        rows = self.cur.fetchall()
        # rows.reverse()
        for i, rec in enumerate(rows):
            if i != 0: file.write(f'\n')
            file.write(f'       "{urllib.parse.unquote(rec[PathRec.PATH])}": {rec[PathRec.AVG_TIME] / 1000:.2f}')
            if i < len(rows) - 1: file.write(',')
        self.top_path_avg_seconds = None
        file.write(f'''
    }}
}}
''')


# ############################################################### CLI

@click.command()
@click.version_option(version=__version__, prog_name=APP_NAME)
@click.option('--in', 'in_file_path', required=True, help='Input file.',
              type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True))
@click.option('--out', 'out_file_path', required=True, help='Output JSON file.',
              type=click.Path(file_okay=True, dir_okay=False, writable=True, resolve_path=True))
@click.option('--max-client-ips', default=10, help='Maximum number of results in the <top_client_ips> field.',
              show_default=True, type=click.IntRange(0, 10000))
@click.option('--max-paths', default=10, help='Maximum number of results in the <top_path_avg_seconds> field.',
              show_default=True, type=click.IntRange(0, 10000))
def main(in_file_path, out_file_path, max_client_ips, max_paths):
    """
    """
    print(f"in: {in_file_path}")
    print(f"out: {out_file_path}")
    print(f"max-client-ips: {max_client_ips}")
    print(f"max-paths: {max_paths}")
    if not os.path.exists(os.path.dirname(out_file_path)):
        raise click.FileError(out_file_path, hint="Invalid file path")
    print("processing...")
    with Parser(in_file_path, max_client_ips, max_paths) as parser:
        with open(in_file_path) as f:
            for ln in parser.read_next_line_gen():
                line_rec = parser.parse_line(ln)
                parser.update_stats(line_rec)
        # report
        print("writing report...")
        out_file = open(out_file_path, 'w')
        parser.write_report(out_file)
        out_file.close()
        with open(out_file_path) as f:
            for ln in f:
                print(ln.rstrip())
    ram_usage_MB = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6
    if parser.statsd:
        parser.statsd.gauge('ram_usage_MB', ram_usage_MB)
    print(f"done [ram={ram_usage_MB}MB]")


if __name__ == '__main__':
    try:
        main(standalone_mode=False)
    except click.exceptions.ClickException as err:
        err.show()
        sys.exit(1)
