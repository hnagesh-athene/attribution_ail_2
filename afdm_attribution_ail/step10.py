'''
transformation step-10
'''
#from decimal import Decimal
from prework import Prework
from datetime import date
from collections import OrderedDict
from Columns import AIL_Columns as order

class Step10:
    '''
    changes to be made in step 10
    '''
    def __init__(self, valuation_date):
        '''
        define fields to be modified in step 10
        '''
        print('Step 10 class')
        self.valuation_date = date(int(valuation_date[:4]), int(valuation_date[4:6]), int(valuation_date[6:]))
        #print(self.valuation_date)
        self.functions = [self.row_builder,
                          self.GenBudgetOBCurr,
                          self.GenBudgetUltOB,
                          self.idx,
                          self.generate]
        
    
    def row_builder(self,merger_row, current_row):
        
        
        self.row = OrderedDict()
        self.row = {fields:None for fields in order}
        
        return self.row

    def GenBudgetOBCurr(self, merger_row, current_row):
        '''
        logic for the field
        '''
        if merger_row['join_indicator'] in ('AB') and merger_row['GenAV_PQ'] and float(merger_row['GenAV_PQ']) > 0:
            current_row['GenBudgetOBCurr'] = merger_row['GenBudgetOBCurr_PQ']
        else:
            current_row['GenBudgetOBCurr'] = merger_row['GenBudgetOBCurr_CQ']
        
        return current_row
    
    def GenBudgetUltOB(self, merger_row, current_row):
        '''
        logic for the field
        '''
        if merger_row['join_indicator'] in ('AB') and merger_row['GenAV_PQ'] and float(merger_row['GenAV_PQ']) > 0:
            current_row['GenBudgetUltOB'] = merger_row['GenBudgetUltOB_PQ']
        else:
            current_row['GenBudgetUltOB'] = merger_row['GenBudgetUltOB_CQ']
            
        return current_row
            
    def IdxBudgetStrategyFee(self, merger_row, current_row, index):
        '''
        logic for the field
        '''
        if merger_row['join_indicator'] == 'AB' and merger_row['Idx1RecLinkID_CQ'] not in ('',None) and\
        merger_row[f'_int_idx{index}_anniv'] == 'N':
            current_row[f'Idx{index}BudgetStrategyFee'] = merger_row[f'Idx{index}BudgetStrategyFee_PQ']
            current_row[f'Idx{index}BudgetVolAdjOB'] = merger_row[f'Idx{index}BudgetVolAdjOB_PQ']
            current_row[f'Idx{index}BudgetOBCurr'] = merger_row[f'Idx{index}BudgetOBCurr_PQ']
            current_row[f'Idx{index}BudgetUltOB'] = merger_row[f'Idx{index}BudgetUltOB_PQ']
        
        elif merger_row['join_indicator'] == 'AB' and merger_row['Idx1RecLinkID_CQ'] not in ('',None) and\
        merger_row[f'_int_idx{index}_anniv'] == 'Y':
            current_row[f'Idx{index}BudgetStrategyFee'] = merger_row[f'Idx{index}BudgetStrategyFee_PQ']
            current_row[f'Idx{index}BudgetVolAdjOB'] = merger_row[f'Idx{index}BudgetOBCurr_PQ']
            current_row[f'Idx{index}BudgetOBCurr'] = merger_row[f'Idx{index}BudgetOBCurr_PQ']
            current_row[f'Idx{index}BudgetUltOB'] = merger_row[f'Idx{index}BudgetUltOB_PQ']
        
        else:
            current_row[f'Idx{index}BudgetStrategyFee'] = merger_row[f'Idx{index}BudgetStrategyFee_CQ']
            current_row[f'Idx{index}BudgetVolAdjOB'] = merger_row[f'Idx{index}BudgetVolAdjOB_CQ']
            current_row[f'Idx{index}BudgetOBCurr'] = merger_row[f'Idx{index}BudgetOBCurr_CQ']
            current_row[f'Idx{index}BudgetUltOB'] = merger_row[f'Idx{index}BudgetUltOB_CQ']
        
        return current_row
        
    def idx(self,merrger_row, current_row):
        
        for i in range(1,6):
            current_row = self.IdxBudgetStrategyFee(merrger_row, current_row, i)
        return current_row
            
    
    def generate(self,merge,current_row):
        '''
        Default fields
        '''
        for fields in order:
            if fields in ('PolNo', 'Company'):
                current_row[fields] = merge[fields]
            elif fields not in ['GenBudgetOBCurr','GenBudgetUltOB','Idx1BudgetStrategyFee','Idx2BudgetStrategyFee',\
                                'Idx3BudgetStrategyFee','Idx4BudgetStrategyFee','Idx15BudgetStrategyFee','Idx1BudgetOBCurr',\
                                'Idx2BudgetOBCurr','Idx3BudgetOBCurr','Idx4BudgetOBCurr','Idx5BudgetOBCurr','Idx1BudgetUltOB',\
                                'Idx2BudgetUltOB','Idx3BudgetUltOB','Idx4BudgetUltOB','Idx5BudgetUltOB','Idx1BudgetVolAdjOB',\
                                'Idx2BudgetVolAdjOB','Idx3BudgetVolAdjOB','Idx4BudgetVolAdjOB','Idx5BudgetVolAdjOB']:
                
                current_row[fields] = merge[fields+'_CQ']
        return current_row
