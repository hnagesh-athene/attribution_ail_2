'''
transformation step-6
'''
#from decimal import Decimal
from prework import Prework
from datetime import date
from collections import OrderedDict
from Columns import AIL_Columns as order

class Step6:
    '''
    changes to be made in step 6
    '''
    def __init__(self, valuation_date):
        '''
        define fields to be modified in step 6
        '''
        self.valuation_date = date(int(valuation_date[:4]), int(valuation_date[4:6]), int(valuation_date[6:]))
        
        self.fields = ['GMWBCharge','GMWBIncType','GMWBParRate','GMWBParRate2','GMWBParRate3','GMWBParRate4',\
                       'GMWBPayment','GMWBRollup','GMWBRollup2','GMWBRollup3','GMWBRollup4','GMWBMaxAccumPeriod',\
                       'GMWBMaxCharge','F133GMWBPayment','IRRestartNew','InitialRestartMonths','AdditionalRestartMonths',\
                       'RestartCharge','Seed']
        
        self.functions = [self.step_6]
                          
    
    
    def Step6_functions(self, merger_row, current_row, field):
        '''
        logic for the field
        '''
        if merger_row['join_indicator'] == 'AB' and merger_row['IRRestartNew_PQ'] not in ( "U", "_") and\
         merger_row['IRRestartNew_CQ'] == 'U':
            current_row[f'{field}'] = merger_row[f'{field}'+'_PQ']
        return current_row
    
    def step_6(self,merger_row,current_row,fieldnames):
        
        for field in self.fields:
            if field in fieldnames:
                current_row = self.Step6_functions(merger_row,current_row,field)
            
        return current_row
    