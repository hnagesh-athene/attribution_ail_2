"""
This process will merge all files with matching keyword and write them into a new file.
"""
import csv
import argparse
import pandas as pd
import datetime
from collections import OrderedDict


def merge():
    """
    Merging previous quarter and current quarter ail's
    """
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-p', '--previous-quarter',
                         help='first file path')
    parser.add_argument('-c', '--current-quarter',
                         help='second file path')
    parser.add_argument('-o', '--output',
                         help='output filename')
    
    args = parser.parse_args()
    
    
    print("Files merging started")
    df1 = pd.read_csv(args.previous_quarter, delimiter = '\t', low_memory=False)
    df2 = pd.read_csv(args.current_quarter, delimiter = '\t', low_memory=False)
    out = df1.merge(df2, how='outer', on=['PolNo', 'Company'],suffixes=('_PQ', '_CQ'))
    out.to_csv(args.output,sep = '\t', index=False)
    print("Files merged successfully")
    


if __name__ == '__main__':
    st = datetime.datetime.now()
    merge()
    print('total time-taken = ', datetime.datetime.now()-st)
        

