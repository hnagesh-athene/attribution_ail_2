"""
This process will merge all files with matching keyword and write them into a new file.
"""

import argparse
from core_utils.tabular import FastDictReader
from collections import OrderedDict
from Columns import AIL_Columns as order
from core_utils.tabular import CSVDataIO
import datetime
import tqdm

def reader(file_1, file_2):
        '''
        reader
        '''
        print('reader')
        fp_1 = open(file_1)
        fp_2 = open(file_2)
        obj1 = FastDictReader(fp_1, delimiter='\t')
        fieldnames = order
        obj2 = FastDictReader(fp_2, delimiter='\t')
        return obj1, obj2
    
def writer(filename):
        '''
        writer
        '''
        print('output write')
        write = CSVDataIO(delimiter='\t')
        cols = prefix_list_pq(order) + prefix_list_cq(order)
        file = write.iterative_writer(filename, cols)
        return file
    
def row_builder():
        
        
        row = OrderedDict()
        row = {fields:None for fields in order}
        
        return row


def prefix_dict_pq(row):
    
    return {k+'_PQ':v for k,v in row.items()}

def prefix_dict_cq(row):
    
    return {k+'_CQ':v for k,v in row.items()}

def prefix_list_pq(row):
    
    return [k+'_PQ' for k in row]

def prefix_list_cq(row):
    
    return [k+'_CQ' for k in row]

def Merge():
    """
    Merging previous quarter and current quarter ail's
    """
    print('transform')
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-p', '--prev',
                         help='previous quarter file')
    parser.add_argument('-c', '--cur',
                         help='current quarter file')
    parser.add_argument('-o', '--output',
                         help='output filename')
    
    args = parser.parse_args()
    
    input_1, input_2 = reader(args.prev,args.cur)
    progress = tqdm.tqdm(mininterval=1, unit=' rows', desc='rows checked ')
    previous_row = next(input_1)
    current_row = next(input_2)
    write_obj = writer(args.output)
    row = row_builder()
    row_pq = prefix_dict_pq(row)
    row_cq = prefix_dict_cq(row)
    
    while previous_row or current_row:
        try:
            if not current_row:
                x = prefix_dict_pq(previous_row)
                x.update(row_cq)
                write_obj.send(x)
                try:
                    previous_row = next(input_1)
                except:
                    previous_row = None
            elif not previous_row:
                x = row_pq
                x.update(prefix_dict_cq(current_row))
                write_obj.send(x)
                try:
                    current_row = next(input_2)
                except:
                    current_row = None
            elif current_row['PolNo']+current_row['Company'] < previous_row['PolNo']+previous_row['Company']:
                x = row_pq
                x.update(prefix_dict_cq(current_row))
                write_obj.send(x)
                try:
                    current_row = next(input_2)
                except:
                    current_row = None
                
            elif current_row['PolNo']+current_row['Company'] > previous_row['PolNo']+previous_row['Company']:
                x = prefix_dict_pq(previous_row)
                x.update(row_cq)
                write_obj.send(x)
                try:
                    previous_row = next(input_1)
                except:
                    previous_row = None
                
            else:
                x = prefix_dict_pq(previous_row)
                x.update(prefix_dict_cq(current_row))
                write_obj.send(x)
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
            

st = datetime.datetime.now()            
Merge()
print('total time-taken = ', datetime.datetime.now()-st)
