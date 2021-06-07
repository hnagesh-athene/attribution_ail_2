import argparse
import tqdm
import csv
import datetime
from collections import OrderedDict, defaultdict
from Columns import Columns_ail
import pandas as pd
import xlrd
from core_utils.dates import shift_date, shift_quarters


class INTOutput():
    
    def __init__(self, irecord, avrf_reader_dict, EOR_Assumptions_reader_dict, args):
        """
        In this method,
        Initialization of required instance variables is done,
        These variable are instantiated and used repeatedly throughout the class.
        """
        self.orecord = OrderedDict()
        field_list = Columns_ail
        for i in field_list:
            self.orecord[i] = ''
        self.irecord = {**irecord,**self.orecord}
        self.avrf = avrf_reader_dict
        self.EOR_Assumptions = EOR_Assumptions_reader_dict
        self.args = args
        
    
    def policy_number(self):
        
        self.irecord['PolNo'] = self.irecord['PolNo_PQ'] if self.irecord['PolNo_PQ'] not in ('', None) else self.irecord['PolNo_CQ']
        
        return self
    
    def company(self):
        
        self.irecord['Company'] = self.irecord['Company_PQ'] if self.irecord['Company_PQ'] not in ('', None)\
        else self.irecord['Company_CQ']
        
        return self
    
    def days_ann(self):
        
        a = datetime.datetime.strptime(self.args.valuation,'%Y%m%d')
        issue_date = '19991231' if self.irecord['IssueDate_CQ'] in ('', None) else self.irecord['IssueDate_CQ']
        b = datetime.datetime.strptime(issue_date,'%Y%m%d')
        final_date = a - b
        days = int(final_date.days)
        self.irecord['days_ann'] = days
        
        return self
    
    def joint_indicator(self):
        '''
        joint indicator
        '''
        
        if self.irecord['LegalEntity_PQ'] in (None, '') and self.irecord['LegalEntity_CQ']:
            self.irecord['join_indicator'] = 'A'
        elif self.irecord['LegalEntity_PQ'] and self.irecord['LegalEntity_CQ'] in (None, ''):
            self.irecord['join_indicator'] = 'B'
        elif self.irecord['LegalEntity_PQ'] and self.irecord['LegalEntity_CQ']:
            self.irecord['join_indicator'] = 'AB'
        
        return self
    
    def idxordersync_pq(self):
        
        idx = defaultdict(int)
        if self.irecord['join_indicator'] in ('AB','B'):
            for i in range(1,6):
                key = self.irecord[f'Idx{i}Index_PQ'] + self.irecord[f'Idx{i}RecLinkID_PQ'] + self.irecord[f'Idx{i}ANXStrat_PQ']
                idx[key] = i
            self.irecord['__idxordersync_pq'] = dict(idx)
        
        return self
    
    def idxordersync_cq(self):
        
        idx = defaultdict(int)
        if self.irecord['join_indicator'] in ('AB','A'):
            for i in range(1,6):
                key = self.irecord[f'Idx{i}Index_CQ'] + self.irecord[f'Idx{i}RecLinkID_CQ'] + self.irecord[f'Idx{i}ANXStrat_CQ']
                idx[key] = i
            self.irecord['__idxordersync_cq'] = dict(idx)
        
        return self
    
    def get_eor(self,index):
        
        if self.irecord['Idx{}Term_PQ'.format(index)] == '1' and self.irecord['Idx{}CapRate_PQ'.format(index)] >= '0':
            if (self.irecord['Company'] != "AMP" and self.irecord['Idx{}CapRate_PQ'.format(index)] > '1') or \
                (self.irecord['Company'] == "AMP" and self.irecord['Idx{}CapRate_PQ'.format(index)] >= '0.15'):
                
                return self.EOR_Assumptions['eor1']
            elif self.irecord['Idx{}CapRate_PQ'.format(index)] <= '0.02':
                return self.EOR_Assumptions['eor2']
            elif self.irecord['Idx{}CapRate_PQ'.format(index)] <= '0.04':
                return self.EOR_Assumptions['eor3']
            else:
                return self.EOR_Assumptions['eor4']
            
        elif self.irecord['Idx{}Term_PQ'.format(index)] == '2':
            if self.irecord['Idx{}SpreadRate_PQ'.format(index)] == 0:
                return self.EOR_Assumptions['eor5']
            else:
                return self.EOR_Assumptions['eor6']
        else:
            return self.EOR_Assumptions['eor7']
    
     
    def idx_eor(self):
        
        for index in range(1,6):
            self.irecord['_int_idx{}_eor'.format(index)] = self.get_eor(index)
            
        return self
    
    def index_credit(self):
        
        if self.irecord['PolNo'] in self.avrf:
            if self.avrf[self.irecord['PolNo']] == '' or float(self.avrf[self.irecord['PolNo']]) <= 0.0:
                self.irecord['index_credit'] = 0
            else:
                self.irecord['index_credit'] = self.avrf[self.irecord['PolNo']]
        else:
            self.irecord['index_credit'] = 0
                
        return self
    
    def anniv(self):
        
        if self.irecord['join_indicator'] in ('A','B'):
            for idx in range(1,6):
                self.irecord[f'_int_idx{idx}_anniv'] = self.get_anniv(idx)
            return self
        elif self.irecord['join_indicator'] == 'AB':
            for idx in range(1,6):
                key = self.irecord[f'Idx{idx}Index_CQ'] + self.irecord[f'Idx{idx}RecLinkID_CQ'] + self.irecord[f'Idx{idx}ANXStrat_CQ']
                if key != '__':
                    self.irecord[f'_int_idx{idx}_anniv'] = self.get_anniv(self.irecord['__idxordersync_pq'].get(key,idx))
                else:
                    self.irecord[f'_int_idx{idx}_anniv'] = self.get_anniv(idx)
            return self
    
    def get_anniv(self,idx):
        
        valdate = datetime.datetime.strptime(self.args.valuation,'%Y%m%d').date()
        if self.irecord['join_indicator'] == 'A':
            return 'N'
        elif self.irecord['Company'] == 'CU':
            issue_date_pq = datetime.datetime.strptime(self.irecord['IssueDate_PQ'],'%Y%m%d').date()
            if issue_date_pq.month == valdate.month or issue_date_pq.month == valdate.month-1 or issue_date_pq.month == valdate.month-2:
                return 'Y'
            else:
                return 'N'
        elif self.irecord['join_indicator'] in ('AB','B'):
            issue_date_pq = datetime.datetime.strptime(self.irecord['IssueDate_PQ'],'%Y%m%d').date()
            maturity_date = shift_date(issue_date_pq, 0, int(self.irecord[f'Idx{idx}TermStart_PQ'])+\
                                       int(self.irecord[f'Idx{idx}Term_PQ'])*12-1, 0)
            if self.irecord['PolNo'] == '000087':
                print(maturity_date,valdate,shift_date(valdate,0,-3,0))
            if valdate > maturity_date > shift_date(valdate,0,-3,0):
                #print('y',valdate,maturity_date,shift_date(valdate,0,-3,0),self.irecord['join_indicator'])
                return 'Y'
            else:
                return 'N'
                         
    
def execute_attribute(row, avrf_reader_dict, EOR_Assumptions_reader_dict, args):
    '''
    In this method,
    Transformation class object is created,
    Using method chaining concept,
    transformation methods are called.
    '''
    afdm = INTOutput(row, avrf_reader_dict, EOR_Assumptions_reader_dict, args)

    afdm.policy_number()\
        .company()\
        .days_ann()\
        .joint_indicator()\
        .idxordersync_pq()\
        .idxordersync_cq()\
        .idx_eor()\
        .anniv()\
        .index_credit()
        
    return afdm.irecord
    

def ail_row_processor(avrf_reader_dict, EOR_Assumptions_reader_dict, args):
        '''
        '''
        with open(args.merge_file, 'r') as f:
            reader1 = csv.DictReader(f,delimiter='\t')
            for x in reader1:
                y = execute_attribute(x, avrf_reader_dict, EOR_Assumptions_reader_dict, args)
                yield y

def generate_ail():
    
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-m', '--merge-file',
                         help='merge filename')
    parser.add_argument('-o', '--output',
                         help='output filename')
    parser.add_argument('-e', '--eor-file',
                        help='eor input filename')
    parser.add_argument('-a1', '--avrf-file1',
                         help='avrf input filename for as400 and opas')
    parser.add_argument('-a2', '--avrf-file2',
                         help='avrf input filename for alip')
    parser.add_argument('-v', '--valuation',
                         help='date for file generation')
    
    args = parser.parse_args()
    wb = xlrd.open_workbook(args.eor_file)
    
    sheet = wb.sheet_by_index(1)
    EOR_Assumptions_reader_dict = dict()
    EOR_Assumptions_reader_dict['eor1'] = sheet.cell_value(4,2)
    EOR_Assumptions_reader_dict['eor2'] = sheet.cell_value(5,2)
    EOR_Assumptions_reader_dict['eor3'] = sheet.cell_value(6,2)
    EOR_Assumptions_reader_dict['eor4'] = sheet.cell_value(7,2)
    EOR_Assumptions_reader_dict['eor5'] = sheet.cell_value(8,2)
    EOR_Assumptions_reader_dict['eor6'] = sheet.cell_value(9,2)
    EOR_Assumptions_reader_dict['eor7'] = sheet.cell_value(10,2)
    
    
    avrf_reader_dict = dict()
    
    
    with open(args.avrf_file1, 'r') as f:
        reader = csv.DictReader(f,delimiter='\t')
        for row in reader:
            if row['SourceSystem'] == 'AS400':
                PolNo = row['PolicyNumber'][:-3]
                avrf_reader_dict[PolNo] = row['IndexCredit']
            else:
                avrf_reader_dict[row['PolicyNumber']] = row['IndexCredit']
    
    with open(args.avrf_file2, 'r') as f:
        reader = csv.DictReader(f,delimiter=',')
        for row in reader:
            avrf_reader_dict[row['PolicyNumber']] = row['IndexCredit']
            
    
    with open(args.merge_file, 'r') as file:
        reader_obj = csv.reader(file,delimiter = '\t')
        for field in reader_obj:
            header = field
            break
    header.remove('PolNo_PQ')
    header.remove('PolNo_CQ')
    header.remove('Company_PQ')
    header.remove('Company_CQ')
    
        
    rows = tqdm.tqdm(ail_row_processor(avrf_reader_dict,EOR_Assumptions_reader_dict,args))
    with open(args.output,'w',newline='') as f:
        writer = csv.DictWriter(f,fieldnames=header+Columns_ail,delimiter='\t',extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    generate_ail()
    