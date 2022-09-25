"""Sample Parquet target stream class, which handles writing streams."""

from datetime import datetime
import gzip
import os
import shutil
from typing import List
import tempfile
import hashlib
import pendulum

from singer_sdk.sinks import BatchSink
from singer_sdk.helpers._simpleeval import simple_eval

from target_athena import athena
from target_athena import s3
from target_athena import utils
from target_athena import formats

def eval_partition_key_value(row, expr) -> str:
    return simple_eval(expr,
    functions={
        'date': lambda d: pendulum.instance(d).to_date_string(),
        'md5': lambda s: hashlib.md5(s.encode("utf-8")).hexdigest(),
    },
    names={'row': row, 'pendulum': pendulum},
)


class AthenaSink(BatchSink):
    """Athena target sink class."""

    DEFAULT_BATCH_SIZE_ROWS = 10000

    def __init__(
        self,
        target,
        stream_name,
        schema,
        key_properties,
    ):
        super().__init__(target=target,
                         stream_name=stream_name,
                         schema=schema,
                         key_properties=key_properties)
        self._s3_client = None
        self._athena_client = None

        ddl = athena.generate_create_database_ddl(
            self.config["athena_database"])
        athena.execute_sql(ddl, self.athena_client)

    @property
    def s3_client(self):
        if not self._s3_client:
            self._s3_client = s3.create_client(self.config)
        return self._s3_client

    @property
    def athena_client(self):
        if not self._athena_client:
            self._athena_client = athena.create_client(self.config, self.logger)
        return self._athena_client

    @staticmethod
    def _clean_table_name(stream_name):
        return stream_name.replace("-", "_")

    def process_batch(self, context: dict) -> None:
        """Write any prepped records out and return only once fully written."""
        # The SDK populates `context["records"]` automatically
        # since we do not override `process_record()`.
        records_to_drain = context["records"]
        state = None

        object_format = self.config.get("object_format")
        delimiter = self.config.get("delimiter", ",")
        quotechar = self.config.get("quotechar", '"')
        headers = self.schema["properties"].keys(
        ) if object_format == 'csv' else None

        # Use the system specific temp directory if no custom temp_dir provided
        temp_dir = os.path.expanduser(
            self.config.get("temp_dir", tempfile.gettempdir()))

        # Create temp_dir if not exists
        if temp_dir:
            os.makedirs(temp_dir, exist_ok=True)

        partition_keys = self.config.get("partition_keys", {})
        self.logger.info(
            f"Partition Keys from config: '{partition_keys}'"
        )
        if type(partition_keys) == list:
            partition_keys = {k: k for k in partition_keys}
        elif type(partition_keys) != dict:
            partition_keys = {}
        partition_dirs = ['']

        filenames = []
        now = datetime.now().strftime("%Y%m%dT%H%M%S")

        # Serialize records to local files
        for record in records_to_drain:
            partition_path = ''.join(f"{k}={eval_partition_key_value(record, v)}/"
                                     for k, v in partition_keys.items())
            s3_prefix = "{prefix}{database}/".format(
                prefix=self.config.get("s3_key_prefix", ""),
                database=self.config.get("athena_database", ""))
            target_key = utils.get_target_key(
                self.stream_name,
                object_format,
                prefix=s3_prefix,
                timestamp=now,
                # naming_convention=self.config.get("naming_convention"),
                partition_path=partition_path,
            )
            partition_dir = '' if len(partition_keys) == 0 else hashlib.sha256(
                (self.stream_name + now + partition_path).encode()).hexdigest()
            if partition_dir not in partition_dirs:
                full_partition_dir = f"{temp_dir}/{partition_dir}"
                try:
                    os.mkdir(full_partition_dir)
                except FileExistsError:
                    self.logger.warn(
                        f"Existing temp partition directory, please clean, skipping record: '{full_partition_dir}'"
                    )
                    continue
                partition_dirs.append(partition_dir)
            filename = self.stream_name + "-" + now + "." + object_format
            filename = os.path.expanduser(
                os.path.join(temp_dir, partition_dir, filename))
            if not (filename, target_key) in filenames:
                filenames.append((filename, target_key))

            file_is_empty = (
                not os.path.isfile(filename)) or os.stat(filename).st_size == 0

            if self.config.get("flatten_records"):
                flattened_record = utils.flatten_record(record)
            else:
                flattened_record = record

            if object_format == 'csv':
                formats.write_csv(filename=filename,
                                  record=flattened_record,
                                  header=headers,
                                  delimiter=delimiter,
                                  quotechar=quotechar)
            elif object_format == 'jsonl':
                formats.write_jsonl(filename=filename,
                                    record=flattened_record,
                                    partition_keys=partition_keys)
            else:
                self.logger.warn(f"Unrecognized format: '{object_format}'")

        # Create schemas in Athena
        self.logger.info("headers: {}".format(headers))
        data_location = "s3://{s3_bucket}/{key_prefix}{database}/{stream}/".format(
            s3_bucket=self.config.get("s3_bucket"),
            key_prefix=self.config.get("s3_key_prefix", ""),
            database=self.config.get("athena_database", ""),
            stream=self.stream_name,
        )  # TODO: double check this
        if object_format == 'csv':
            ddl = athena.generate_create_table_ddl(
                self._clean_table_name(self.stream_name),
                self.schema,
                headers=headers,
                database=self.config.get("athena_database"),
                data_location=data_location,
                row_format="org.apache.hadoop.hive.serde2.OpenCSVSerde",
                partition_keys=partition_keys,
            )
        elif object_format == 'jsonl':
            ddl = athena.generate_create_table_ddl(
                self._clean_table_name(self.stream_name),
                self.schema,
                headers=headers,
                database=self.config.get("athena_database"),
                data_location=data_location,
                skip_header=False,
                row_format="org.openx.data.jsonserde.JsonSerDe",
                serdeproperties="'ignore.malformed.json'='true', 'case.insensitive'='true'",
                partition_keys=partition_keys,
            )
        else:
            self.logger.warn(f"Unrecognized format: '{object_format}'")
        self.logger.info(ddl)
        self.logger.info(data_location)
        athena.execute_sql(ddl, self.athena_client)

        # Upload created files to S3
        for filename, target_key in filenames:
            compressed_file = None
            if (self.config.get("compression") is None
                    or self.config["compression"].lower() == "none"):
                pass  # no compression
            else:
                if self.config["compression"] == "gzip":
                    compressed_file = f"{filename}.gz"
                    target_key = target_key + ".gz"
                    with open(filename, "rb") as f_in:
                        with gzip.open(compressed_file, "wb") as f_out:
                            self.logger.info(
                                f"Compressing file as '{compressed_file}'")
                            shutil.copyfileobj(f_in, f_out)
                else:
                    raise NotImplementedError(
                        "Compression type '{}' is not supported. "
                        "Expected: 'none' or 'gzip'".format(
                            self.config["compression"]))
            s3.upload_file(
                compressed_file or filename,
                self.s3_client,
                self.config.get("s3_bucket"),
                target_key,
                encryption_type=self.config.get("encryption_type"),
                encryption_key=self.config.get("encryption_key"),
            )

            # Remove the local file(s)
            os.remove(filename)
            if compressed_file:
                os.remove(compressed_file)

        # Repair table, cataloging new partitions if any
        athena.execute_sql(
            f"MSCK REPAIR TABLE {self.config.get('athena_database')}.{self.stream_name};",
            self.athena_client)

        # Remove directories created for partitions
        for partition_dir in partition_dirs:
            if partition_dir != '':
                os.rmdir(os.path.join(temp_dir, partition_dir))
        return state
