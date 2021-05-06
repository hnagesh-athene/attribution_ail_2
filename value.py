import argparse
import tqdm
import csv
import datetime
from collections import OrderedDict
from csv_helper import csv_io
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
        self.irecord = irecord
        self.orecord = OrderedDict()
        field_list = Columns_ail
        self.avrf = avrf_reader_dict
        self.EOR_Assumptions = EOR_Assumptions_reader_dict
        for i in field_list:
            self.orecord[i] = ''
        self.args = args
        
            
    def policy_number(self):
        
        self.orecord['PolNo'] = self.irecord['PolNo']
        return self
    
    def company(self):
        
        self.orecord['Company'] = self.irecord['Company']
        return self
            
    def days_ann(self):
        
        a = datetime.datetime.strptime(self.args.valuation,'%Y%m%d')
        issue_date = '19991231' if self.irecord['IssueDate_CQ'] in ('', None) else self.irecord['IssueDate_CQ'][:-2]
        b = datetime.datetime.strptime(issue_date,'%Y%m%d')
        final_date = a - b
        days = int(final_date.days)
        self.orecord['days_ann'] = days
        
        return self
    
    def joint_indicator(self):
        '''
        joint indicator
        '''
        
        if self.irecord['LegalEntity_PQ'] in (None, '') and self.irecord['LegalEntity_CQ']:
            self.orecord['join_indicator'] = 'A'
        elif self.irecord['LegalEntity_PQ'] and self.irecord['LegalEntity_CQ'] in (None, ''):
            self.orecord['join_indicator'] = 'B'
        elif self.irecord['LegalEntity_PQ'] and self.irecord['LegalEntity_CQ']:
            self.orecord['join_indicator'] = 'AB'
        
        return self
    
    def __idxordersync__(self):
        
        self.orecord['__idxordersync__'] = index1
        return self
    
    def get_eor(self,index):
        
        if self.irecord['Idx{}Term_PQ'.format(index)] == '1' and self.irecord['Idx{}CapRate_PQ'.format(index)] >= 0:
            if (self.irecord['Company'] != "AMP" and self.irecord['Idx{}CapRate_PQ'.format(index)] > 1) or \
                (self.irecord['Company'] == "AMP" and self.irecord['Idx{}CapRate_PQ'.format(index)] >= 0.15):
                
                return self.EOR_Assumptions['eor1']
            elif self.irecord['Idx{}CapRate_PQ'.format(index)] <= 0.02:
                return self.EOR_Assumptions['eor2']
            elif self.irecord['Idx{}CapRate_PQ'.format(index)] <= 0.04:
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
        
    def get_anniver(self,index):
        '''
        Anniv logic
        '''
        issue_date_PQ = '19991231' if self.irecord['IssueDate_PQ'] in ('', None) else self.irecord['IssueDate_PQ'][:-2]
        issuedate_PQ = datetime.datetime.strptime(issue_date_PQ,'%Y%m%d').date()
        issue_date_CQ = '19991231' if self.irecord['IssueDate_CQ'] in ('', None) else self.irecord['IssueDate_CQ'][:-2]
        issuedate_CQ = datetime.datetime.strptime(issue_date_CQ,'%Y%m%d').date()
        valdate = datetime.datetime.strptime(self.args.valuation,'%Y%m%d').date()
        valday = valdate.replace(day=1)
        IssMo_PQ =  issuedate_PQ.month
        IssMo_CQ =  issuedate_CQ.month
        valmonth_2 = shift_date(valdate,0,-2,0).month
        PQ_Start = 0
        d = 0 if self.irecord['Idx{}Term_PQ'.format(index)] in ('', None) else int(float(self.irecord['Idx{}Term_PQ'.format(index)]))
        d = 1 if d<=0 else d
        
        #if self.irecord['Idx{}TermStart_PQ'.format(index)] == 1 and issuedate_PQ < shift_date(valday,-d,-2,0):
        if self.orecord['join_indicator'] in ('AB','B'):
            if (abs(valdate.year - issuedate_PQ.year)% d) == 0 and (IssMo_PQ == valmonth_2  or IssMo_PQ==shift_date(valdate,0,-1,0).month or IssMo_PQ==valdate.month):
                return "Y"
            else:
                return "N"
        else:
            if self.orecord['join_indicator'] in ('AB', 'A'):
                f = 0 if self.irecord['Idx{}Term_CQ'.format(index)] in ('', None) else int(float(self.irecord['Idx{}Term_CQ'.format(index)]))
                if shift_date(issuedate_CQ,f,0,0) > shift_date(valday,0,-2,0):
                    PQ_Start = 1
                else:
                    PQ_Start = f * 12 + 1
            
                a = PQ_Start + (f * 12) - 1
                IdxDate = shift_date(issuedate_CQ,0,a,0)
                if IdxDate > shift_quarters(valdate, -1) and IdxDate < valdate:
                    return "Y";
                elif PQ_Start > 1 and IdxDate.year < valdate.year and (IssMo_CQ == valmonth_2  or IssMo_CQ==shift_date(valdate,0,-1,0).month or IssMo_CQ==valdate.month):
                    return "Y"
                else:
                    return "N"
        
    def idx_eor(self):
        
        for index in range(1,6):
            self.orecord['_int_idx{}_eor'.format(index)] = self.get_eor(index)
            self.orecord['_int_idx{}_anniv'.format(index)] = self.get_anniver(index)
            
        return self
    
    def credrate(self):
        
        sum_idx_avif = 0
        for i in range(1,6):
            if self.irecord['Idx{}AVIF_PQ'.format(i)]:
                sum_idx_avif += float(self.irecord['Idx{}AVIF_PQ'.format(i)])
        
        if sum_idx_avif == 0:
            self.orecord['credrate'] = 0
        else:
            if self.orecord['PolNo'] in self.avrf and self.avrf[self.orecord['PolNo']]:
                print(self.orecord['PolNo'],self.avrf[self.orecord['PolNo']])
                self.orecord['credrate'] = float(self.avrf[self.orecord['PolNo']])/sum_idx_avif
                
            else:
                self.orecord['credrate'] = 0
        
        return self
    
    def index_credit(self):
        
        if self.orecord['PolNo'] in self.avrf:
            if self.avrf[self.orecord['PolNo']] == '' or float(self.avrf[self.orecord['PolNo']]) <= 0:
                self.orecord['index_credit'] = 0
            else:
                self.orecord['index_credit'] = self.avrf[self.orecord['PolNo']]
        else:
            self.orecord['index_credit'] = 0
                
        return self
                         
    
def execute_attribute(row, avrf_reader_dict, EOR_Assumptions_reader_dict,args):
    '''
    In this method,
    Transformation class object is created,
    Using method chaining concept,
    transformation methods are called.
    '''
    afdm = INTOutput(row, avrf_reader_dict, EOR_Assumptions_reader_dict,args)

    afdm.policy_number()\
        .company()\
        .days_ann()\
        .joint_indicator()\
        .__idxordersync__()\
        .idx_eor()\
        .index_credit()
        
    return afdm.orecord
    

def ail_row_processor(avrf_reader_dict, EOR_Assumptions_reader_dict,args):
        '''
        '''
        with open(args.merge_file, 'r') as f:
            reader1 = csv.DictReader(f,delimiter='\t')
            for x in reader1:
                y = execute_attribute(x, avrf_reader_dict, EOR_Assumptions_reader_dict,args)
                yield y

def generate_ail():
    
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-m', '--merge-file',
                         help='merge filename')
    parser.add_argument('-i', '--intermediate-file',
                         help='intermediate filename')
    parser.add_argument('-o', '--output',
                         help='output filename')
    parser.add_argument('-e', '--eor-file',
                        help='eor input filename')
    parser.add_argument('-a', '--avrf-file',
                         help='avrf input filename')
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
    
    
    with open(args.avrf_file, 'r') as f:
        reader = csv.DictReader(f,delimiter='\t')
        for row in reader:
            avrf_reader_dict[row['PolicyNumber']] = row['IndexCredit']
    
    output = tqdm.tqdm(ail_row_processor(avrf_reader_dict,EOR_Assumptions_reader_dict,args))
    with open(args.intermediate_file,'w',newline='') as f:
        writer = csv.DictWriter(f,fieldnames=Columns_ail,delimiter='\t')
        writer.writeheader()
        writer.writerows(output)
    
    merger_ail(args)
        
def merger_ail(args):
    df1 = pd.read_csv(args.merge_file, delimiter = '\t', low_memory=False)
    df2 = pd.read_csv(args.intermediate_file, delimiter = '\t', low_memory=False)
    out = df1.merge(df2, how='outer', on=['PolNo', 'Company'])
    out.to_csv(args.output,sep = '\t', index=False)
    print("Files merged successfully")

try:
    Idx = {'Idx1AVIF_PQ':1, 'Idx2AVIF_PQ':2,'Idx3AVIF_PQ':3,'Idx4AVIF_PQ':4,'Idx5AVIF_PQ':5}
    Idx1 = {'Idx1AVIF_PQ':1, 'Idx2AVIF_PQ':2,'Idx3AVIF_PQ':3,'Idx4AVIF_PQ':4,'Idx5AVIF_PQ':5}
    index1 = {}
    
    with open(args.merge_file, 'r') as file:
        reader_obj = csv.reader(file,delimiter = '\t')
        for field in reader_obj:
            header = field
            break
        for k,v in Idx.items():
            Idx[k] = header.index(k)
    
    Idx_sorted = sorted(Idx.items(),key = lambda kv:(kv[1],kv[0]))
    count = 1

    for i in Idx_sorted:
        index1[Idx1[i[0]]] = count
        count += 1   
except:
    pass

if __name__ == '__main__':
    generate_ail()
    