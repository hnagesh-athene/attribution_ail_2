'''
transformation step-7
'''
#from decimal import Decimal
from prework import Prework
from datetime import date
from collections import OrderedDict
from Columns import AIL_Columns as order

class Step7:
    '''
    changes to be made in step 7
    '''
    def __init__(self, valuation_date):
        '''
        define fields to be modified in step 1
        '''
        print('Step 7 class')
        self.valuation_date = date(int(valuation_date[:4]), int(valuation_date[4:6]), int(valuation_date[6:]))
        #print(self.valuation_date)
        self.functions = [self.Seed]
        
    def Seed(self, merger_row, current_row, fieldnames):
        '''
        logic for the field
        '''
        if 'Seed' in fieldnames:
            if merger_row['IRRestartNew_PQ'] == merger_row['IRRestartNew_CQ'] and merger_row['IRRestartNew_CQ'] not in ('_', 'U'):
                current_row['Seed'] = merger_row['Seed_PQ']
        return current_row

    