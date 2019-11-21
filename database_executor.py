import concurrent.futures
import importlib.resources as pkg_resources
import json
import os
from enum import Enum

from sqlalchemy import create_engine

BATCH_SIZE = 1000


class Environment(Enum):
    STAGING_1 = 1
    STAGING_2 = 2
    PROD_CR = 3
    PROD = 4


class DatabaseExecutor:
    def __init__(self, environment: Environment):
        with open("./credentials.json") as file:
            settings = json.loads(file.read())

        prod_combined_replica_user_name = settings["prod_combined_replica_user_name"]
        prod_combined_replica_password = settings["prod_combined_replica_password"]
        prod_read_replica_user_name = settings["prod_read_replica_user_name"]
        prod_read_replica_password = settings["prod_read_replica_password"]
        staging_user_name = settings["staging_user_name"]
        staging_password = settings["staging_password"]

        prod_cr_connecting_string = f'mysql+mysqldb://{prod_combined_replica_user_name}:{prod_combined_replica_password}@combinedreplica.fbdatateam.aws.fbks.in:3306/'
        prod_connecting_string = f'mysql+mysqldb://{prod_read_replica_user_name}:{prod_read_replica_password}@127.0.0.1:3307/'
        staging_connecting_string_1 = f'mysql+mysqldb://{staging_user_name}:{staging_password}@127.0.0.1:3308/'
        staging_connecting_string_2 = f'mysql+mysqldb://{staging_user_name}:{staging_password}@127.0.0.1:3309/'

        if environment == Environment.STAGING_1:
            self.connection_string = staging_connecting_string_1
        elif environment == Environment.STAGING_2:
            self.connection_string = staging_connecting_string_2
        elif environment == Environment.PROD_CR:
            self.connection_string = prod_cr_connecting_string
        else:
            self.connection_string = prod_connecting_string

        self._environment = environment

    def execute_query(self, database, sql_file, output_file):
        with open(f"queries/{sql_file}") as file:
            query = file.read()

        engine = create_engine(self.connection_string + database)
        engine.connect()

        csv_report = []
        headers = ''

        cursor = engine.execute(query)

        result = cursor.fetchmany(BATCH_SIZE)

        processed_count = result

        while result:
            for record in result:
                csv_record = ','.join(str(row_value) for row_value in record)
                csv_record = csv_record.replace("None", "")
                csv_report.append(csv_record + os.linesep)

                if not headers:
                    headers = ','.join(record._keymap.keys())

            result = cursor.fetchmany(BATCH_SIZE)

            processed_count += len(result)

            self._report_progress("Processing batches: ", processed_count, 1000000, 100)

        print()
        print(f'Total: {len(csv_report)}')

        with open(output_file, "w") as file:
            file.write(headers + os.linesep)
            file.writelines(csv_report)

    def execute_sharded_query(self, sql_file, output_file):
        shards = self._get_shards()
        shards = [shard for shard in shards if shard == "live" or int(shard.replace("live", "")) < 670]

        with open(f"queries/{sql_file}") as file:
            query = file.read()

        query_results = {}
        processed_shards_count = 0

        headers = ''

        max_workers = 1

        # We can query Combined Replica in parallel (no workers limit is set)
        if self._environment == Environment.PROD_CR:
            max_workers = None

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = [executor.submit(self.execute_query_for_shard, shard, query) for shard in shards]

            for result in concurrent.futures.as_completed(results):
                processed_shards_count = processed_shards_count + 1
                self._report_progress('Processing shards: ', processed_shards_count, len(shards), len(shards))
                future_result = result.result()
                query_results[future_result[0]] = future_result[1]

                if future_result[2]:
                    headers = future_result[2]

        print()
        print(f'Total: {sum([len(report_line) for report_line in query_results.values()])}')

        report = []

        if headers:
            report.append(headers)

        for shard, results in query_results.items():
            if results:
                report.append(os.linesep)
                report.append(str.join(os.linesep, results))

        with open(output_file, "w") as file:
            file.writelines(report)

    def execute_query_for_shard(self, shard, query):
        engine = create_engine(self.connection_string)
        engine.connect()

        shard_report = []
        headers = ''

        engine.execute(f"use {shard}")
        cursor = engine.execute(query)

        result = cursor.fetchmany(BATCH_SIZE)

        while result:
            for record in result:
                csv_record = ','.join(self._escape_value(row_value) for row_value in record)
                csv_record += f',{shard}'
                csv_record = csv_record.replace("None", "")
                shard_report.append(csv_record)

                if not headers:
                    headers = ','.join(record._keymap.keys())
                    headers += ',shard'

            result = cursor.fetchmany(BATCH_SIZE)

        return shard, shard_report, headers

    def _get_shards(self):
        engine = create_engine(self.connection_string)
        engine.connect()

        print("Connected to the database!")
        print("Getting the list of databases...", end=' ')

        with open('queries/show_databases.sql') as file:
            show_databases_query = file.read()

        engine_result = engine.execute(show_databases_query)
        print('Done')

        live_shards = [database[0] for database in engine_result if database[0].startswith('live')]

        print(f'{len(live_shards)} live shards detected')

        return live_shards

    @staticmethod
    def _escape_value(value):
        if not isinstance(value, str):
            return str(value)

        escape_chars = ["\n", "\r", "\"", ","]

        for char in escape_chars:
            escaped = value.replace(char, "_")

        return bytes(escaped, 'utf-8').decode('ascii', 'ignore')

    @staticmethod
    def _report_progress(title, curr, total, full_progbar):
        frac = curr / total
        filled_progbar = round(frac * full_progbar)
        print('\r', title + '#' * filled_progbar + '-' * (full_progbar - filled_progbar), '[{:>7.2%}]'.format(frac),
              end='')
