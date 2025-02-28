import math
import sys
import argparse
import numpy as np
from sklearn.preprocessing import MinMaxScaler, StandardScaler
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
        self.metric = {}
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
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS(
                        SELECT 1 FROM pg_extension 
                        WHERE extname = 'pg_stat_statements'
                    )
                """)
                if not cursor.fetchone()[0]:
                    raise Exception("pg_stat_statements extension not enabled")

                cursor.execute("""
                    SELECT 
                        SUM(calls) AS total_calls,
                        SUM(total_exec_time) AS total_time,
                        SUM(rows) AS total_rows,
                        SUM(shared_blks_dirtied) AS writes,
                        SUM(shared_blks_read) AS reads
                    FROM pg_stat_statements
                """)
                stats = cursor.fetchone()

                cursor.execute("""
                    SELECT 
                        COUNT(*) FILTER (WHERE state = 'active'),
                        COUNT(*),
                        EXTRACT(EPOCH FROM NOW() - MIN(backend_start)),
                        SUM(temp_files) + SUM(temp_bytes)/1048576
                    FROM pg_stat_activity
                """)
                activity_stats = cursor.fetchone()

                total_ops = stats[0] or 1
                total_time = stats[1] or 1

                self.metric = {
                    'write_ratio': (stats[3] / (stats[3] + stats[4])) if (stats[3] + stats[4]) > 0 else 0,
                    'read_ratio': stats[4] / (stats[3] + stats[4]) if (stats[3] + stats[4]) > 0 else 0,
                    'complexity_score': (stats[1] / total_ops) * (stats[2] / total_ops),
                    'transaction_rate': stats[0] / (activity_stats[2] or 1),
                    'temp_usage': activity_stats[3],
                    'active_connections': activity_stats[0],
                    'cache_hit_ratio': (1 - (stats[4] / (stats[3] + stats[4]))) if (stats[3] + stats[4]) > 0 else 1
                }

        except Exception as e:
            print(f"Ошибка сбора метрик: {e}")

    def calculate_scores(self):
        weights = {
            'OLAP': {
                'complexity_score': 0.7,
                'temp_usage': 0.8,
                'read_ratio': 0.6,
                'write_ratio': -0.5,
                'transaction_rate': -0.3
            },
            'OLTP': {
                'write_ratio': 0.9,
                'transaction_rate': 0.8,
                'complexity_score': -0.7,
                'cache_hit_ratio': 0.6
            },
            'Web': {
                'transaction_rate': 0.7,
                'active_connections': 0.9,
                'cache_hit_ratio': 0.8,
                'complexity_score': -0.4
            },
            'Desktop': {
                'temp_usage': 0.6,
                'complexity_score': 0.5,
                'active_connections': -0.7,
                'transaction_rate': 0.4
            }
        }

        scores = {}
        for load_type, coeffs in weights.items():
            scores[load_type] = sum(
                self.metric[metric] * weight
                for metric, weight in coeffs.items()
                if metric in self.metric
            )

        scores['Mixed'] = (scores['OLAP'] + scores['OLTP']) / 2

        for i in self.scores.keys():
            print(i + " score: " + str(self.scores[i]))

        return max(scores, key=scores.get)

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