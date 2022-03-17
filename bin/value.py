import argparse
import csv
import datetime
import sys

import tqdm
import xlrd
from .columns import Columns_ail
import glob
sys.path.insert(0, '../core_utils')
from core_utils.dates import shift_date
from core_utils.tabular import tsv_io


class INTOutput():
    def __init__(self, irecord, avrf_reader_dict, EOR_Assumptions_reader_dict,
                 field_names, args, logger):
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
        self.logger = logger
        self.check = ''


    def rec_linkid_cq(self):

        for i in range(1,6):
            if self.args.block == "amp":
                self.irecord[f'_int_idx{i}_RecLinkID_CQ'] = self.irecord[f'Idx{i}Index_CQ'] + self.irecord[f'Idx{i}RecLinkID_CQ']
            elif self.args.block in ('voya_fia', 'voya_fa', 'Rocky.fia', 'Rocky.tda', 'jackson.fia', 'jackson.tda', 'lsw'):
                self.irecord[f'_int_idx{i}_RecLinkID_CQ'] = self.irecord[f'Idx{i}Index_CQ'] + self.irecord[f'Idx{i}RecLinkID_CQ'] \
                          + self.irecord[f'Idx{i}CredStrategy_CQ']
            else:
                self.irecord[f'_int_idx{i}_RecLinkID_CQ'] = self.irecord[f'Idx{i}Index_CQ'] + self.irecord[f'Idx{i}RecLinkID_CQ'] \
                          + self.irecord[f"Idx{i}ANXStrat_CQ"]

            if self.irecord[f'_int_idx{i}_RecLinkID_CQ'] in ('___', '__'):
                self.irecord[f'_int_idx{i}_RecLinkID_CQ'] = '_'

        return self

    def rec_linkid_pq(self):

        for i in range(1,6):
            if self.args.block == "amp":
                self.irecord[f'_int_idx{i}_RecLinkID_PQ'] = self.irecord[f'Idx{i}Index_PQ'] + self.irecord[f'Idx{i}RecLinkID_PQ']
            elif self.args.block in ('voya_fia', 'voya_fa', 'Rocky.fia', 'Rocky.tda', 'jackson.fia', 'jackson.tda', 'lsw'):
                self.irecord[f'_int_idx{i}_RecLinkID_PQ'] = self.irecord[f'Idx{i}Index_PQ'] + self.irecord[f'Idx{i}RecLinkID_PQ'] \
                          + self.irecord[f'Idx{i}CredStrategy_PQ']
            else:
                self.irecord[f'_int_idx{i}_RecLinkID_PQ'] = self.irecord[f'Idx{i}Index_PQ'] + self.irecord[f'Idx{i}RecLinkID_PQ'] \
                          + self.irecord[f"Idx{i}ANXStrat_PQ"]

            if self.irecord[f'_int_idx{i}_RecLinkID_PQ'] in ('___', '__'):
                self.irecord[f'_int_idx{i}_RecLinkID_PQ'] = '_'

        return self


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
                key = self.irecord[f'_int_idx{i}_RecLinkID_PQ']
                if key != '_' and key in idx:
                    self.check = key
                    idx = {}
                    break
                else:
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
                key = self.irecord[f'_int_idx{i}_RecLinkID_CQ']
                if key != '_' and key in idx:
                    self.check = key
                    idx = {}
                else:
                    idx[key] = i
            self.irecord['__idxordersync_cq'] = idx

        if self.check:
            self.logger.info('PolNo : '+self.irecord['PolNo'] + ' key : '+self.check)

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
    
    def is_leap_year(self, _year):
        return True if (_year - 2000) % 4 == 0 else False

    def adjust_for_leap_year(self, _year, _month, _day):
        if (self.is_leap_year(_year), _month, _day) == (False, 2, 29):
            _day = _day - 1
        return datetime.datetime(_year,_month, _day).date()

    def _new_idx_flag(self):
        for i in range(1,6):
            if self.irecord[f'_int_idx{i}_RecLinkID_CQ'] != self.irecord[f'_int_idx{i}_RecLinkID_PQ']:
                return True
        return False

    def get_maturity_date(self, valdate, issue_date_cq, issue_date_pq, idx): # term form CQ

        if self.args.block in ('voya_fia', 'voya_fa', 'jackson.fia', 'jackson.tda'):
            if self.irecord['join_indicator'] in ('A','AB'):
                term = int(self.irecord[f'Idx{idx}Term_CQ'])
                if float(self.irecord[f'Idx{idx}AVIF_CQ']) <= 0:
                    term = 1
                mat_yr = valdate.year - (valdate.year - issue_date_cq.year) % term
                return self.adjust_for_leap_year(mat_yr, issue_date_cq.month, issue_date_cq.day)
            else:
                term = int(self.irecord[f'Idx{idx}Term_PQ'])
                if float(self.irecord[f'Idx{idx}AVIF_PQ']) <= 0:
                    term = 1
                mat_yr = valdate.year - (valdate.year - issue_date_pq.year) % term
                return self.adjust_for_leap_year(mat_yr, issue_date_pq.month, issue_date_pq.day)
                
        elif self._new_idx_flag() == True and self.irecord['join_indicator'] in ('A','AB'):
            maturity_date = shift_date(issue_date_cq, 0, int(self.irecord[f'Idx{idx}TermStart_CQ']) - 1, 0)

        elif self.irecord['join_indicator'] == 'B': # A = currrent and B = prior
            maturity_date = shift_date(issue_date_pq, 0, int(self.irecord[f'Idx{idx}TermStart_PQ']) - 1, 0)

        else:
            maturity_date = shift_date(issue_date_cq, 0, int(self.irecord[f'Idx{idx}TermStart_CQ']) + \
                                       int(self.irecord[f'Idx{idx}Term_CQ']) * 12 - 1, 0)
        return maturity_date

    def get_anniv(self, idx):
        '''
        calculates maturity status of strategy
        '''
        valdate = datetime.datetime.strptime(self.args.valuation_date, '%Y%m%d').date()
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
            issue_date_cq = datetime.datetime.strptime(self.irecord['IssueDate_CQ'], '%Y%m%d').date() if self.irecord['IssueDate_CQ'] else 0
            maturity_date = self.get_maturity_date(valdate, issue_date_cq, issue_date_pq, idx)
            if valdate >= maturity_date >= shift_date(valdate, 0, -3, 0):
                return 'Y'
            else:
                return 'N'

    def idxTermStart(self):
        '''
        calculates idxtermstart for amp ail
        '''
        valdate = datetime.datetime.strptime(self.args.valuation_date, '%Y%m%d').date()
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
                    issue_date_pq = datetime.datetime.strptime(
                        self.irecord['IssueDate_PQ'], '%Y%m%d').date()
                    term = 0
                    if 31 >= issue_date_pq.day > 22:
                        term = 1
                    index = self.irecord['__idxordersync_pq'].get(self.irecord[f'_int_idx{idx}_RecLinkID_CQ'], idx)
                    maturity_date = shift_date(issue_date_pq, 0, int(self.irecord[f'Idx{index}Term_PQ']) * 12, 0)
                    if maturity_date > shift_date(valdate, 0, -3, 0):
                        self.irecord[f'Idx{idx}TermStart_CQ'] = 1 + term
                    else:
                        self.irecord[f'Idx{idx}TermStart_CQ'] = int(self.irecord[f'Idx{index}Term_PQ']) * 12 + 1 + term
        return self

    def days_ann(self):
        '''
        days between maturity and valuation_date
        '''
        valdate = datetime.datetime.strptime(self.args.valuation_date, '%Y%m%d').date()
        for i in range(1, 6):
            if self.irecord[f'_int_idx{i}_anniv'] == 'Y' and self.irecord['join_indicator'] in ('AB', 'B'):
                issue_date_pq = datetime.datetime.strptime(self.irecord['IssueDate_PQ'], '%Y%m%d').date()
                issue_date_cq = datetime.datetime.strptime(self.irecord['IssueDate_CQ'], '%Y%m%d').date() if self.irecord['IssueDate_CQ'] else 0
                maturity_date = self.get_maturity_date(valdate, issue_date_cq, issue_date_pq, i)
                self.irecord[f'_int_idx{i}_days'] = (valdate - maturity_date).days
            else:
                self.irecord[f'_int_idx{i}_days'] = 1
        return self


def execute_attribute(row, avrf_reader_dict, EOR_Assumptions_reader_dict,
                      field_names, args, logger):
    '''
    In this method,
    Transformation class object is created,
    Using method chaining concept,
    transformation methods are called.
    '''
    afdm = INTOutput(row, avrf_reader_dict, EOR_Assumptions_reader_dict,
                     field_names, args, logger)
    afdm.policy_number() \
        .company() \
        .rec_linkid_cq() \
        .rec_linkid_pq()\
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
                      field_names, args, conf, logger):
    '''
    process each row
    '''
    with open(conf['merge_file'].format(dir = conf['dir'],
                            date = args.valuation_date, block = args.block), 'r') as file:
        reader1 = csv.DictReader(file, delimiter='\t')
        for row in reader1:
            res = execute_attribute(row, avrf_reader_dict,
                                    EOR_Assumptions_reader_dict, field_names,
                                    args, logger)
            yield res


def generate_ail(args, conf, logger):
    '''
    main function
    '''

    obj = xlrd.open_workbook(conf['eor_file'].format(dir = conf['dir'],
                                                     date = args.valuation_date, block = args.block))

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

    avrf_files = conf['avrf_input'].split(',')
    avrf_key = conf['avrf_key'].split(',')

    if args.block in ('amp', 'ila', 'anx', 'amp', 'tda'):
        for file in range(len(avrf_files)):
            file_path = avrf_files[file].format(dir = conf['dir'],
                            date = args.valuation_date, block = args.block)
            file_kv = avrf_key[file].split('|')
            polno = file_kv[0].split(':')[1]
            indexcredit = file_kv[1].split(':')[1]
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    avrf_reader_dict[row[polno]] = float(
                        row[indexcredit] if row[indexcredit] != '' else 0)
    elif args.block in ('voya_fia', 'voya_fa'):
        for file in range(len(avrf_files)):
            file_path = avrf_files[file].format(dir = conf['dir'],
                            date = args.valuation_date, block = args.block, valdate = args.valuation_date)
            file_kv = avrf_key[file].split('|')
            polno = file_kv[0].split(':')[1]
            indexcredit = file_kv[1].split(':')[1]
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f, delimiter=',')
                for row in reader:
                    avrf_reader_dict[row[polno]] = float(
                        row[indexcredit] if row[indexcredit] != '' else 0)
    elif args.block in ('Rocky.fia', 'Rocky.tda'):
        for file in range(len(avrf_files)):
            avrf_obj = xlrd.open_workbook(avrf_files[file].format(dir = conf['dir'],date = args.valuation_date,
                                                                  block = args.block, year=args.valuation_date[:4],
                                                                  month=args.valuation_date[4:6],
                                                                  day = args.valuation_date[6:]))
            sheet = avrf_obj.sheet_by_index(0)
            for index in range(1, sheet.nrows):
                policy_number = sheet.cell_value(index, 0)
                value = sheet.cell_value(index, 15)
                try:
                    if '(' in value:
                        value = value[1:-1]
                except:
                    pass
                temp = avrf_reader_dict.get(policy_number, 0)
                avrf_reader_dict[policy_number] = temp + float(value)

    elif args.block in ['jackson.fia', 'jackson.tda']:
        valdate = datetime.datetime.strptime(args.valuation_date, '%Y%m%d').date()
        sec_month = valdate.replace(day = 1) - datetime.timedelta(days = 1)
        first_month = sec_month.replace(day = 1) - datetime.timedelta(days = 1)
        avrf_dates = [first_month.strftime('%Y%m%d'), sec_month.strftime('%Y%m%d'), valdate.strftime('%Y%m%d')]
        for file in range(len(avrf_files)):
            file_path = avrf_files[file].format(dir=conf['dir'], date=args.valuation_date, block=args.block, yyyy = avrf_dates[file][0:4], mm = avrf_dates[file][4:6], dd = avrf_dates[file][6:8])
            file_kv = avrf_key[file].split('|')
            polno = file_kv[0].split(':')[1]
            indexcredit = file_kv[1].split(':')[1]
            for name in glob.glob(file_path):
                with open(name, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        avrf_reader_dict[row[polno].lstrip('0')] = avrf_reader_dict.get(row[polno].lstrip('0'), 0) + float(row[indexcredit] if row[indexcredit] != '' else 0)

    elif args.block in ('lsw'):
        for file in range(len(avrf_files)):
            file_path = avrf_files[file].format(dir = conf['dir'],date = args.valuation_date, block = args.block
                                                , year=args.valuation_date[:4],month=args.valuation_date[4:6],
                                                day = args.valuation_date[6:])
            file_kv = avrf_key[file].split('|')
            polno = file_kv[0].split(':')[1]
            indexcredit = file_kv[1].split(':')[1]
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f, delimiter=',')
                for row in reader:
                    avrf_reader_dict[row[polno]] = float(row[indexcredit] if row[indexcredit] != '' else 0)

    header = tsv_io.read_header(conf['merge_file'].format(dir = conf['dir'],
                                                          date = args.valuation_date, block = args.block))
    header.remove('PolNo_PQ')
    header.remove('PolNo_CQ')
    header.remove('Company_PQ')
    header.remove('Company_CQ')

    field_names = tsv_io.read_header(conf['dir']+'/input/'+args.valuation_date+'/'+args.block+'/'+args.cur)

    if args.block == 'amp':
        field_list = Columns_ail + ['Idx1TermStart_PQ', 'Idx2TermStart_PQ', 'Idx3TermStart_PQ',
                                    'Idx4TermStart_PQ', 'Idx5TermStart_PQ', 'Idx1TermStart_CQ',
                                    'Idx2TermStart_CQ', 'Idx3TermStart_CQ', 'Idx4TermStart_CQ',
                                    'Idx5TermStart_CQ']
    else:
        field_list = Columns_ail
    rows = tqdm.tqdm(
        ail_row_processor(avrf_reader_dict, EOR_Assumptions_reader_dict,
                          field_names, args, conf, logger))
    with open(conf['intermediate_file'].format(dir = conf['dir'],
                            date = args.valuation_date, block = args.block), 'w', newline='') as file:
        writer = csv.DictWriter(file,
                                fieldnames=header + field_list,
                                delimiter='\t',
                                extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    generate_ail()
