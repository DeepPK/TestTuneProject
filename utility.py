import math
import sys
import argparse
import numpy as np
import collections

import psycopg2

import psutil



KB = 1024
MB = 1024 * KB
GB = 1024 * MB
KB_PER_MB = MB / KB
KB_PER_GB = GB / KB

class Config_line(object):
    def __init__(self, line):
        self.original_line = line
        self.parameter = ''
        self.comments = ''
        self.name = ''
        self.value = ''


    def process_line(self):
        self.parameter = self.original_line.strip().split('#', 1)[0].strip()
        self.comments = self.original_line.strip().split('#', 1)[-1]

        if self.parameter != '':
            self.name, self.value = self.parameter.split('=', 1)
            self.name = self.name.rstrip()
            self.name = self.name.rstrip()
            self.value = self.value.rstrip("'")
            self.value = self.value.lstrip("'")


class Config(object):
    def __init__(self, filename):
        self.filename = filename
        self.config_lines = []

    def Read(self):
        for line in open(self.filename):
            Line = Config_line(line)
            Line.process_line()
            self.config_lines.append(Line)

    def Write(self, fout, tuning):
        keys_temp = list(tuning.s.keys())
        for line in self.config_lines:
            if line.parameter == '' and not any(substring in line.comments for substring in ["max_connections =", "shared_buffers =", "effective_cache_size =", "work_mem =", "maintenance_work_mem =", "checkpoint_segments =", "checkpoint_completion_target =", "default_statistics_target ="]):
                fout.write(line.original_line)
            else:
                [last] = collections.deque(keys_temp, maxlen=1)
                for key in keys_temp:
                    value = tuning.s[key]
                    if line.name == key or key + " = " in line.comments:
                        if key in ["shared_buffers", "effective_cache_size", "work_mem", "maintenance_work_mem"]:
                            if key in line.comments:
                                fout.write(key + " = " + str(value) + "MB #" + line.comments + "\n")
                                keys_temp.remove(key)
                                break
                            else:
                                fout.write(key + " = " + str(value) + "MB #" + "\n")
                                keys_temp.remove(key)
                                break
                        else:
                            if key in line.comments:
                                fout.write(key + " = " + str(value) + "#" + line.comments + "\n")
                                keys_temp.remove(key)
                                break
                            else:
                                fout.write(key + " = " + str(value) + "#" + "\n")
                                keys_temp.remove(key)
                                break
                    if last == key:
                        fout.write(line.original_line)
                        break


class Tuning:
    def __init__(self):
        self.s = {}
        self.total_memory = None
        self.prev_stats = None
        self.metrics = {}
        self.config ={
            "db_name" : "postgres",
            "db_user": "postgres",
            "db_password": "",
            "db_host": "localhost",
            "db_port": "5432",
        }
        self.scores = {
            'OLTP': 0,
            'OLAP': 0,
            'Mixed': 0,
            'Web': 0,
            'Desktop': 0
        }

    def total_mem(self):
        return psutil.virtual_memory().total >> 20

    def tune_param(self, options):
        db_type = self.get_postgres_load(options)
        try:
            self.s['max_connections'] = {'Web': 200, 'OLTP': 300, 'OLAP': 20, 'Mixed': 100, 'Desktop': 5}[db_type]
        except KeyError:
            print("Error:  unexpected setting for db_type")
            sys.exit(1)

        if self.total_memory is None:
            self.total_memory = self.total_mem()
        if self.total_memory is None:
            print("Error:  total memory not specified and unable to detect")
            sys.exit(1)

        mem = int(self.total_memory)
        con = int(self.s['max_connections'])

        if mem > 1024:
            self.s['shared_buffers'] = math.ceil({'Web': mem / 4, 'OLTP': mem / 4, 'OLAP': mem / 4,
                                   'Mixed': mem / 4, 'Desktop': mem / 16}[db_type])
        else:
            self.s['shared_buffers'] = mem

        self.s['effective_cache_size'] = math.ceil({'Web': mem * 3 / 4, 'OLTP': mem * 3 / 4, 'OLAP': mem * 3 / 4,
                                     'Mixed': mem * 3 / 4, 'Desktop': mem / 4}[db_type])

        self.s['work_mem'] = math.ceil({'Web': mem / con, 'OLTP': mem / con, 'OLAP': mem / con / 2,
                         'Mixed': mem / con / 2, 'Desktop': mem / con / 6}[db_type])

        self.s['maintenance_work_mem'] = math.ceil({'Web': mem / 16, 'OLTP': mem / 16, 'OLAP': mem / 8,
                                     'Mixed': mem / 16, 'Desktop': mem / 16}[db_type])

        if self.s['maintenance_work_mem'] > (2 * GB / KB):
            self.s['maintenance_work_mem'] = 2 * GB / KB

        self.s['checkpoint_segments'] = {'Web': 32, 'OLTP': 64, 'OLAP': 128,
                                    'Mixed': 32, 'Desktop': 3}[db_type]

        self.s['checkpoint_completion_target'] = {'Web': 0.7, 'OLTP': 0.9, 'OLAP': 0.9,
                                             'Mixed': 0.9, 'Desktop': 0.5}[db_type]

        self.s['default_statistics_target'] = {'Web': 100, 'OLTP': 100, 'OLAP': 500,
                                          'Mixed': 100, 'Desktop': 100}[db_type]

    def collect_metrics(self, conn):
        try:
            with conn.cursor() as cursor: #соединение с бд
                cursor.execute("""
                   SELECT 
                       SUM(xact_commit) AS commits,
                       SUM(xact_rollback) AS rollbacks,
                       SUM(tup_inserted) AS inserts,
                       SUM(tup_updated) AS updates,
                       SUM(tup_deleted) AS deletes,
                       SUM(tup_fetched) AS fetched,
                       SUM(tup_returned) AS returned,
                       SUM(temp_files) AS temp_files,
                       SUM(temp_bytes) AS temp_bytes,
                       EXTRACT(EPOCH FROM NOW() - pg_postmaster_start_time()) AS uptime
                   FROM pg_stat_database
                   WHERE datname = current_database()
               """)
                db_stats = cursor.fetchone()

                cursor.execute("""
                    SELECT 
                        COUNT(*) FILTER (WHERE state = 'active') AS active_conn,
                        COUNT(*) AS total_conn,
                        MAX(EXTRACT(EPOCH FROM NOW() - query_start)) FILTER (WHERE state = 'active') AS max_query_time,
                        AVG(EXTRACT(EPOCH FROM NOW() - query_start)) FILTER (WHERE state = 'active') AS avg_query_time,
                        COUNT(*) FILTER (WHERE query ~* '(join|group by|window|with |select.*from|over )' AND state = 'active') AS complex_queries,
                        COUNT(*) FILTER (WHERE query ~* '(insert|update|delete)' AND state = 'active') AS write_queries,
                        COUNT(*) FILTER (WHERE wait_event_type = 'Lock' AND state = 'active') AS lock_wait
                    FROM pg_stat_activity
                    WHERE 
                        pid <> pg_backend_pid()
                        AND backend_type = 'client backend'
                        AND (state = 'active' OR state LIKE 'idle%')
                """)
                activity_stats = cursor.fetchone()

                total_writes = db_stats[2] + db_stats[3] + db_stats[4]
                total_reads = db_stats[5] + db_stats[6]
                total_ops = total_writes + total_reads or 1
                uptime = db_stats[9] or 1

                active_conn = activity_stats[0] or 0
                total_conn = activity_stats[1] or 1
                active_ratio = active_conn / total_conn if total_conn > 0 else 0

                max_query_time = min(activity_stats[2] or 0, 3600)
                avg_query_time = min(activity_stats[3] or 0, 300)

                self.metrics = {
                    'write_ratio': float(total_writes / total_ops),
                    'read_ratio': float(total_reads / total_ops),
                    'active_ratio': float(active_ratio),
                    'conn_longevity': float(avg_query_time),
                    'complexity_score': float(activity_stats[4] or 0),
                    'temp_usage': float((db_stats[8] / (1024 ** 2))),
                    'lock_ratio': float(activity_stats[6] / active_conn) if active_conn > 0 else 0,
                    'tps': float(db_stats[0] / uptime),
                    'cache_hit_ratio': float(db_stats[5] / (db_stats[5] + db_stats[6])) if (db_stats[5] + db_stats[6]) > 0 else 0
                }

                for i in self.metrics.keys():
                    print(i + ": " + str(self.metrics[i]))

        except Exception as e:
            print(f"Ошибка сбора метрик: {e}")

    def calculate_scores(self):
        weights = {
            'OLTP': {
                'write_ratio': 3.0,
                'tps': 3.0,
                'lock_ratio': 1.5,
                'cache_hit_ratio': 1.2,
                'conn_longevity': -1.0,
                'complexity_score': -2.0,
                'temp_usage': -0.5
            },
            'OLAP': {
                'complexity_score': 1.2,
                'temp_usage': 0.8,
                'read_ratio': 0.7,
                'conn_longevity': 0.6,
                'tps': -1.5,
                'write_ratio': -2.0
            },
            'Web': {
                'active_ratio': 0.8,
                'tps': 0.6,
                'cache_hit_ratio': 1.0,
                'conn_longevity': -1.2,
                'complexity_score': -1.0,
                'write_ratio': -0.9
            },
            'Desktop': {
                'conn_longevity': 1.2,
                'complexity_score': 0.7,
                'temp_usage': 0.5,
                'active_ratio': -1.0,
                'tps': -0.8
            }
        }

        NORMALIZATION = {
            'tps': lambda x: min(x / 500, 3.0),
            'temp_usage': lambda x: (x / 1000) if x < 5000 else 2.0,
            'conn_longevity': lambda x: min(x / 1800, 1.5),
            'complexity_score': lambda x: x / 20 if x < 50 else 2.5,
            'active_ratio': lambda x: x * 2 if x < 0.5 else 1.0,
        }

        scores = {k: 0.0 for k in weights}

        for load_type in weights:
            for metric, weight in weights[load_type].items():
                raw_value = self.metrics.get(metric, 0)
                if metric in NORMALIZATION:
                    norm_value = NORMALIZATION[metric](raw_value)
                else:
                    norm_value = raw_value
                scores[load_type] += norm_value * weight

        scores['Mixed'] = (scores['OLTP'] + scores['OLAP']) * 0.3

        #print("\nМетрики:", {k: round(v, 2) for k, v in self.metrics.items()})
        print("Оценки:", {k: round(v, 2) for k, v in scores.items()})

        return max(scores, key=lambda k: scores[k])

    def get_postgres_load(self, options):
        try:
            if options.db != "postgres":
                self.config["db_name"] = options.db
            if options.Username != "postgres":
                self.config["db_user"] = options.Username
            if options.Password != "":
                self.config["db_password"] = options.Password

            conn = psycopg2.connect(
                dbname=self.config["db_name"],
                user=self.config["db_user"],
                password=self.config["db_password"],
                host=self.config["db_host"],
                port=self.config["db_port"]
            )

            self.collect_metrics(conn)

            return self.calculate_scores()

        except Exception as e:
            print(f"Ошибка анализа: {e}")
        finally:
            if hasattr(self, 'conn'):
                conn.close()

def options():
    parser = argparse.ArgumentParser(
        description="Test Utility for tuning PostgreSQL database.",
        epilog="Example: python utility.py --input_config postgresql.conf --output-config postgresql.conf -U postgres -db postgres -w pass"
    )
    parser.add_argument(
        "--input_config", "-i",
        dest="input_config",
        type=str,
        help="Path to the input file"
    )

    #parser.add_argument("-V", "--version",
    #    dest="db_version",
    #    default="8.4",
    #    help="Version of PostgreSQL to configure for. Default is 8.4"
    #)

    #parser.add_argument(
    #    '-L', '--logging',
    #    action="store_true",
    #    dest="logging",
    #    default="False",
    #    help="Logging"
    #)

    parser.add_argument(
        '-U',
        dest="Username",
        default="postgres",
        help="Define username for db. Need to process load and choose type of db")

    parser.add_argument(
        '-d',
        dest="db",
        default="postgres",
        help="Define db. Need to process load and choose type of db")

    parser.add_argument(
        '-w',
        dest="Password",
        default="",
        help="Define password of user in db. Need to process load and choose type of db")

    parser.add_argument(
        '-o',
        '--output-config',
        dest="output_config",
        default=None,
        help="Output configuration file, defaults to standard output")

    options = parser.parse_args()

    return options, parser

def main():
    option, parser = options()

    Settings = option.input_config
    if Settings is None:
        print(sys.stderr, "Can't do anything without an input config file.")
        parser.print_help()
        return 1

    config = Config(Settings)
    config.Read()
    tuning = Tuning()
    tuning.tune_param(option)

    output_file_name = option.output_config
    if output_file_name is None:
        fout = open("postgresql.conf", 'w')
    else:
        fout = open(output_file_name, 'w')

    config.Write(fout, tuning)
    fout.close()

if __name__ == '__main__':
    sys.exit(main())
