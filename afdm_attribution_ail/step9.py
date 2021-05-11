'''
transformation step-9
'''
#from decimal import Decimal
from prework import Prework
from datetime import date
from collections import OrderedDict
from Columns import AIL_Columns as order
from _operator import index

class Step9:
    '''
    changes to be made in step 9
    '''
    def __init__(self, valuation_date):
        '''
        define fields to be modified in step 9
        '''
        self.valuation_date = date(int(valuation_date[:4]), int(valuation_date[4:6]), int(valuation_date[6:]))
        self.functions = [self.IdxAOptNomMV_formater]
    
    def IdxAOptNomMV(self,merger_row, current_row,index):
        '''
        logic for the field
        '''
        if merger_row[f'Idx{index}RecLinkID_CQ'] and merger_row[f'_int_idx{index}_anniv'] == 'N':
            current_row[f'Idx{index}AOptNomMV'] = float(merger_row[f'Idx{index}AOptNomMV_PQ'] if merger_row[f'Idx{index}AOptNomMV_PQ'] else '0')\
            *((1 +float(merger_row[f'_int_idx{index}_eor'] if merger_row[f'_int_idx{index}_eor'] else '0'))**(1/4))
        if merger_row[f'Idx{index}RecLinkID_CQ'] and merger_row[f'_int_idx{index}_anniv'] == 'Y':
            current_row[f'Idx{index}AOptNomMV'] = float(merger_row[f'Idx{index}BudgetVolAdjOB_PQ'] if merger_row[f'Idx{index}BudgetVolAdjOB_PQ'] else '0')\
            *((1 + float(merger_row[f'_int_idx{index}_eor'] if merger_row[f'_int_idx{index}_eor'] else '0'))\
              **(float(merger_row['days_ann'] if merger_row['days_ann'] else '0')/365))
        
        return current_row

    def IdxAOptNomMV_formater(self,merger_row,current_row):
        
        '''
        Running the process for all index
        '''
        
        for index in range(1,6):
            current_row = self.IdxAOptNomMV(merger_row,current_row,index)
            
        return current_row