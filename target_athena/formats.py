"""Methods for writinig different object formats."""

import os
import csv
import json


def write_csv(filename, record, header=None, delimiter=",", quotechar='"'):

    file_is_empty = (not os.path.isfile(filename)) or os.stat(filename).st_size == 0

    if not header and not file_is_empty:
        with open(filename, "r") as csv_file:
            reader = csv.reader(csv_file, delimiter=delimiter, quotechar=quotechar)
            first_line = next(reader)
            header = (first_line if first_line else record.keys())
    else:
        header = record.keys()

    # Athena does not support newline characters in CSV format.
    # Remove `\n` and replace with escaped text `\\n` ('\n')
    for k, v in record.items():
        if isinstance(v, str) and "\n" in v:
            record[k] = v.replace("\n", "\\n")

    with open(filename, "a") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            header,
            extrasaction="ignore",
            delimiter=delimiter,
            quotechar=quotechar,
        )
        if file_is_empty:
            writer.writeheader()

        writer.writerow(record)


def write_jsonl(filename, record, partition_keys=None):
    # If partition keys delete and re-add them in order so they are at the end of record
    if partition_keys is not None:
        temp_dict = {k: record[k] for k in partition_keys.keys() if k in record}
        for k in partition_keys.keys():
            if k in record:
                del record[k]
        for k in partition_keys.keys():
            if k in temp_dict:
                record[k] = temp_dict[k]
    with open(filename, 'a', encoding='utf-8') as json_file:
        json_file.write(json.dumps(record, default=str) + '\n')