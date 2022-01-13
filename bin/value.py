import argparse
import csv
import datetime
import sys

import tqdm
import xlrd
from columns import Columns_ail

sys.path.insert(0, '../core_utils')
from core_utils.dates import shift_date
from core_utils.tabular import tsv_io


class INTOutput():
    def __init__(self, irecord, avrf_reader_dict, EOR_Assumptions_reader_dict,
                 field_names, args):
        """
        In this method,
        Initialization of required instance variables is done,
        These variable are instantiated and used repeatedly throughout the class.
        """
        self.args = args
        if self.args.block == 'amp':
            field_list = Columns_ail + ['Idx1TermStart_PQ', 'Idx2TermStart_PQ', 'Idx3TermStart_PQ',
                                        'Idx4TermStart_PQ', 'Idx5TermStart_PQ', 'Idx1TermStart_CQ',
                                        'Idx2TermStart_CQ', 'Idx3TermStart_CQ', 'Idx4TermStart_CQ',
                                        'Idx5TermStart_CQ']
        else:
            field_list = Columns_ail
        self.orecord = {fields: None for fields in field_list}
        self.irecord = {**irecord, **self.orecord}
        self.avrf = avrf_reader_dict
        self.EOR_Assumptions = EOR_Assumptions_reader_dict
        self.suffix = {
            'ILA': 'N01',
            'DLA': 'ADA',
            'AMH': 'I10',
            'ANX': 'ANX',
            'AIL': 'AAI',
            'FBL': 'AFB'
        }

    def policy_number(self):
        '''
        maps policy number
        '''
        self.irecord['PolNo'] = self.irecord['PolNo_PQ'] if self.irecord['PolNo_PQ'] \
                                                            not in ('', None) else self.irecord['PolNo_CQ']

        return self

    def company(self):
        '''
        maps company name
        '''
        self.irecord['Company'] = self.irecord['Company_PQ'] if self.irecord['Company_PQ'] \
                                                                not in ('', None) else self.irecord['Company_CQ']

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
        '''
        dictionary to keep track of strategies of previous quarter
        '''
        idx = {}
        if self.irecord['join_indicator'] in ('AB', 'B'):
            for i in range(1, 6):
                if self.args.block == 'amp':
                    key = self.irecord[f'Idx{i}Index_PQ'] + self.irecord[f'Idx{i}RecLinkID_PQ']
                else:
                    key = self.irecord[f'Idx{i}Index_PQ'] + self.irecord[f'Idx{i}RecLinkID_PQ'] + \
                          self.irecord[f'Idx{i}ANXStrat_PQ']
                idx[key] = i
            self.irecord['__idxordersync_pq'] = idx

        return self

    def idxordersync_cq(self):
        '''
        dictionary to keep track of strategies of current quarter
        '''
        idx = {}
        if self.irecord['join_indicator'] in ('AB', 'A'):
            for i in range(1, 6):
                if self.args.block == 'amp':
                    key = self.irecord[f'Idx{i}Index_CQ'] + self.irecord[f'Idx{i}RecLinkID_CQ']
                else:
                    key = self.irecord[f'Idx{i}Index_CQ'] + self.irecord[f'Idx{i}RecLinkID_CQ'] + \
                          self.irecord[f'Idx{i}ANXStrat_CQ']
                idx[key] = i
            self.irecord['__idxordersync_cq'] = idx

        return self

    def get_eor(self, index):
        '''
        gets eor value for all index
        '''
        if self.irecord['join_indicator'] in ('AB', 'B') and float(self.irecord['Idx{}Term_PQ'.format(index)]) == 1 \
                and float(self.irecord['Idx{}CapRate_PQ'.format(index)]) >= 0:
            if (self.irecord['Company'] != "AMP" and float(self.irecord['Idx{}CapRate_PQ'.format(index)]) > 1) or \
                    (self.irecord['Company'] == "AMP" and float(self.irecord['Idx{}CapRate_PQ'.format(index)]) >= 0.15):
                return self.EOR_Assumptions['eor1']
            elif float(self.irecord['Idx{}CapRate_PQ'.format(index)]) <= 0.02:
                return self.EOR_Assumptions['eor2']
            elif float(self.irecord['Idx{}CapRate_PQ'.format(index)]) <= 0.04:
                return self.EOR_Assumptions['eor3']
            else:
                return self.EOR_Assumptions['eor4']

        elif self.irecord['join_indicator'] in ('AB', 'B') and float(
                self.irecord['Idx{}Term_PQ'.format(index)]) == 2:
            if self.args.block == 'amp' or float(
                    self.irecord['Idx{}SpreadRate_PQ'.format(index)]) == 0:
                return self.EOR_Assumptions['eor5']
            else:
                return self.EOR_Assumptions['eor6']

        else:
            return self.EOR_Assumptions['eor7']

    def idx_eor(self):
        '''
        calculates eor for each index
        '''
        for index in range(1, 6):
            self.irecord['_int_idx{}_eor'.format(index)] = self.get_eor(index)

        return self

    def index_credit(self):
        '''
        maps index credit from avrf
        '''
        if self.irecord['AdminSystem_PQ'] == 'AS400' or self.irecord['AdminSystem_CQ'] == 'AS400':
            PolNo = self.irecord['PolNo'] + self.suffix.get(self.irecord['Company'])
            if PolNo in self.avrf:
                if self.avrf[PolNo] == '' or self.avrf[PolNo] <= 0:
                    self.irecord['index_credit'] = 0
                else:
                    self.irecord['index_credit'] = self.avrf[PolNo]
            else:
                self.irecord['index_credit'] = 0
        else:
            if self.irecord['PolNo'] in self.avrf:
                if self.avrf[self.irecord['PolNo']] == '' or self.avrf[self.irecord['PolNo']] <= 0:
                    self.irecord['index_credit'] = 0
                else:
                    self.irecord['index_credit'] = self.avrf[self.irecord['PolNo']]
            else:
                self.irecord['index_credit'] = 0
        return self

    def anniv(self):
        '''
        calculates maturity status of strategies
        '''
        for idx in range(1, 6):
            self.irecord[f'_int_idx{idx}_anniv'] = self.get_anniv(idx)
        return self

    def get_anniv(self, idx):
        '''
        calculates maturity status of strategy
        '''
        valdate = datetime.datetime.strptime(self.args.valuation, '%Y%m%d').date()
        if self.irecord['join_indicator'] == 'A':
            return 'N'
        elif self.irecord['Company'] == 'CU':
            issue_date_pq = datetime.datetime.strptime(
                self.irecord['IssueDate_PQ'], '%Y%m%d').date()
            if issue_date_pq.month == valdate.month or issue_date_pq.month == valdate.month - 1 \
                    or issue_date_pq.month == valdate.month - 2:
                return 'Y'
            else:
                return 'N'
        elif self.irecord['join_indicator'] in ('AB', 'B'):
            issue_date_pq = datetime.datetime.strptime(
                self.irecord['IssueDate_PQ'], '%Y%m%d').date()
            maturity_date = shift_date(issue_date_pq, 0, int(self.irecord[f'Idx{idx}TermStart_PQ']) + \
                                       int(self.irecord[f'Idx{idx}Term_PQ']) * 12 - 1, 0)
            if valdate >= maturity_date > shift_date(valdate, 0, -3, 0):
                return 'Y'
            else:
                return 'N'

    def idxTermStart(self):
        '''
        calculates idxtermstart for amp ail
        '''
        valdate = datetime.datetime.strptime(self.args.valuation, '%Y%m%d').date()
        if self.args.block == 'amp':
            for idx in range(1, 6):
                if self.irecord['join_indicator'] == 'A':
                    issue_date_cq = datetime.datetime.strptime(self.irecord['IssueDate_CQ'], '%Y%m%d').date()
                    term = 0
                    if 31 >= issue_date_cq.day > 22:
                        term = 1
                    self.irecord[f'Idx{idx}TermStart_CQ'] = 1 + term
                if self.irecord['join_indicator'] in ('AB', 'B'):
                    issue_date_pq = datetime.datetime.strptime(self.irecord['IssueDate_PQ'], '%Y%m%d').date()
                    maturity_date = shift_date(issue_date_pq, 0, int(self.irecord[f'Idx{idx}Term_PQ']) * 12, 0)
                    term = 0
                    if 31 >= issue_date_pq.day > 22:
                        term = 1
                    if maturity_date > shift_date(valdate, 0, -3, 0):
                        self.irecord[f'Idx{idx}TermStart_PQ'] = 1 + term
                    else:
                        self.irecord[f'Idx{idx}TermStart_PQ'] = int(self.irecord[f'Idx{idx}Term_PQ']) * 12 + 1 + term
                if self.irecord['join_indicator'] == 'AB':
                    if self.args.block == 'amp':
                        key = self.irecord[f'Idx{idx}Index_CQ'] + self.irecord[f'Idx{idx}RecLinkID_CQ']
                    else:
                        key = self.irecord[f'Idx{idx}Index_CQ'] + self.irecord[f'Idx{idx}RecLinkID_CQ'] + \
                              self.irecord[f'Idx{idx}ANXStrat_CQ']

                    issue_date_pq = datetime.datetime.strptime(
                        self.irecord['IssueDate_PQ'], '%Y%m%d').date()
                    term = 0
                    if 31 >= issue_date_pq.day > 22:
                        term = 1
                    index = self.irecord['__idxordersync_pq'].get(key, idx)
                    maturity_date = shift_date(issue_date_pq, 0, int(self.irecord[f'Idx{index}Term_PQ']) * 12, 0)
                    if maturity_date > shift_date(valdate, 0, -3, 0):
                        self.irecord[f'Idx{idx}TermStart_CQ'] = 1 + term
                    else:
                        self.irecord[f'Idx{idx}TermStart_CQ'] = int(self.irecord[f'Idx{index}Term_PQ']) * 12 + 1 + term
        return self

    def days_ann(self):
        '''
        days between maturity and valuation
        '''
        valdate = datetime.datetime.strptime(self.args.valuation, '%Y%m%d').date()
        for i in range(1, 6):
            if self.irecord[f'_int_idx{i}_anniv'] == 'Y' and self.irecord['join_indicator'] in ('AB', 'B'):
                issue_date_pq = datetime.datetime.strptime(self.irecord['IssueDate_PQ'], '%Y%m%d').date()
                maturity_date = shift_date(issue_date_pq, 0, int(self.irecord[f'Idx{i}TermStart_PQ']) + \
                                           int(self.irecord[f'Idx{i}Term_PQ']) * 12 - 1, 0)
                self.irecord[f'_int_idx{i}_days'] = (valdate - maturity_date).days
            else:
                self.irecord[f'_int_idx{i}_days'] = 1
        return self


def execute_attribute(row, avrf_reader_dict, EOR_Assumptions_reader_dict,
                      field_names, args):
    '''
    In this method,
    Transformation class object is created,
    Using method chaining concept,
    transformation methods are called.
    '''
    afdm = INTOutput(row, avrf_reader_dict, EOR_Assumptions_reader_dict,
                     field_names, args)
    afdm.policy_number() \
        .company() \
        .joint_indicator() \
        .idxordersync_pq() \
        .idxordersync_cq() \
        .idxTermStart() \
        .idx_eor() \
        .anniv() \
        .days_ann() \
        .index_credit()

    return afdm.irecord


def ail_row_processor(avrf_reader_dict, EOR_Assumptions_reader_dict,
                      field_names, args):
    '''
    process each row
    '''
    with open(args.merge_file, 'r') as file:
        reader1 = csv.DictReader(file, delimiter='\t')
        for row in reader1:
            res = execute_attribute(row, avrf_reader_dict,
                                    EOR_Assumptions_reader_dict, field_names,
                                    args)
            yield res


def generate_ail():
    '''
    main function
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument('-m', '--merge-file', help='merge filename')
    parser.add_argument('-o', '--output', help='output filename')
    parser.add_argument('-e', '--eor-file', help='eor input filename')
    parser.add_argument('-a1',
                        '--avrf-file1',
                        help='avrf input filename for as400 and opas')
    parser.add_argument('-a2',
                        '--avrf-file2',
                        help='avrf input filename')
    parser.add_argument('-p', '--prev', help='previous quarter file')
    parser.add_argument('-v', '--valuation', help='date for file generation')
    parser.add_argument('-b', '--block', help='name of block')

    args = parser.parse_args()
    obj = xlrd.open_workbook(args.eor_file)

    sheet = obj.sheet_by_index(1)
    EOR_Assumptions_reader_dict = dict()
    EOR_Assumptions_reader_dict['eor1'] = sheet.cell_value(4, 2)
    EOR_Assumptions_reader_dict['eor2'] = sheet.cell_value(5, 2)
    EOR_Assumptions_reader_dict['eor3'] = sheet.cell_value(6, 2)
    EOR_Assumptions_reader_dict['eor4'] = sheet.cell_value(7, 2)
    EOR_Assumptions_reader_dict['eor5'] = sheet.cell_value(8, 2)
    EOR_Assumptions_reader_dict['eor6'] = sheet.cell_value(9, 2)
    EOR_Assumptions_reader_dict['eor7'] = sheet.cell_value(10, 2)

    avrf_reader_dict = dict()

    with open(args.avrf_file1, 'r') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            avrf_reader_dict[row['PolicyNumber']] = float(
                row['IndexCredit'] if row['IndexCredit'] != '' else 0)

    with open(args.avrf_file2, 'r') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            avrf_reader_dict[row['policy_number']] = float(
                row['index_credit'] if row['index_credit'] != '' else 0)
                
    header = tsv_io.read_header(args.merge_file)
    header.remove('PolNo_PQ')
    header.remove('PolNo_CQ')
    header.remove('Company_PQ')
    header.remove('Company_CQ')

    field_names = tsv_io.read_header(args.prev)

    if args.block == 'amp':
        field_list = Columns_ail + ['Idx1TermStart_PQ', 'Idx2TermStart_PQ', 'Idx3TermStart_PQ',
                                    'Idx4TermStart_PQ', 'Idx5TermStart_PQ', 'Idx1TermStart_CQ',
                                    'Idx2TermStart_CQ', 'Idx3TermStart_CQ', 'Idx4TermStart_CQ',
                                    'Idx5TermStart_CQ']
    else:
        field_list = Columns_ail
    rows = tqdm.tqdm(
        ail_row_processor(avrf_reader_dict, EOR_Assumptions_reader_dict,
                          field_names, args))
    with open(args.output, 'w', newline='') as file:
        writer = csv.DictWriter(file,
                                fieldnames=header + field_list,
                                delimiter='\t',
                                extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    generate_ail()
