"""
This process will merge all files with matching keyword and write them into a new file.
"""

import argparse
import csv
import datetime
import tqdm
import sys
sys.path.insert(0, '../core_utils')
from core_utils.tabular import tsv_io


def reader(file_1, file_2):
    """
    reader
    """
    print("reader")
    fp_1 = open(file_1)
    fp_2 = open(file_2)
    obj1 = tsv_io.read_file(file_1)
    obj2 = tsv_io.read_file(file_2)
    return obj1, obj2


def writer(filename, header):
    """
    writer
    """
    print("output write")
    cols = prefix_list_pq(header) + prefix_list_cq(header)
    file = tsv_io.iterative_writer(filename, cols)
    return file


def prefix_dict_pq(row):

    return {k + "_PQ": v for k, v in row.items()}


def prefix_dict_cq(row):

    return {k + "_CQ": v for k, v in row.items()}


def prefix_list_pq(row):

    return [k + "_PQ" for k in row]


def prefix_list_cq(row):

    return [k + "_CQ" for k in row]

def merge():
    """
    Merging previous quarter and current quarter ail's
    """
    print("transform")

    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--prev", help="previous quarter file")
    parser.add_argument("-c", "--cur", help="current quarter file")
    parser.add_argument("-o", "--output", help="output filename")

    args = parser.parse_args()

    input_1, input_2 = reader(args.prev, args.cur)
    progress = tqdm.tqdm(mininterval=1, unit=" rows", desc="rows checked ")
    previous_row = next(input_1)
    current_row = next(input_2)

    header = tsv_io.read_header(args.prev)

    write_obj = writer(args.output, header)
    row = {fields:None for fields in header}
    row_pq = prefix_dict_pq(row)
    row_cq = prefix_dict_cq(row)

    while previous_row or current_row:
        try:
            if not current_row:
                row = prefix_dict_pq(previous_row)
                row.update(row_cq)
                write_obj.send(row)
                try:
                    previous_row = next(input_1)
                except:
                    previous_row = None
            elif not previous_row:
                row = row_pq
                row.update(prefix_dict_cq(current_row))
                write_obj.send(row)
                try:
                    current_row = next(input_2)
                except:
                    current_row = None
            elif current_row["PolNo"] + current_row["Company"] < previous_row["PolNo"] + previous_row["Company"]:
                row = row_pq
                row.update(prefix_dict_cq(current_row))
                write_obj.send(row)
                try:
                    current_row = next(input_2)
                except:
                    current_row = None

            elif current_row["PolNo"] + current_row["Company"] > previous_row["PolNo"] + previous_row["Company"]:
                row = prefix_dict_pq(previous_row)
                row.update(row_cq)
                write_obj.send(row)
                try:
                    previous_row = next(input_1)
                except:
                    previous_row = None

            else:
                row = prefix_dict_pq(previous_row)
                row.update(prefix_dict_cq(current_row))
                write_obj.send(row)
                try:
                    previous_row = next(input_1)
                except:
                    previous_row = None
                try:
                    current_row = next(input_2)
                except:
                    current_row = None
        except StopIteration:
            break
        progress.update()


if __name__ == '__main__':
    st = datetime.datetime.now()
    merge()
    print("total time-taken = ", datetime.datetime.now() - st)
