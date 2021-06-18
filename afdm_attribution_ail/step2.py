'''
transformation step-2
'''
#from decimal import Decimal
from datetime import date
class Step2:
    '''
    changes to be made in step 2
    '''
    def __init__(self, valuation_date):
        '''
        define fields to be modified in step 1
        '''
        print('Step 2 class')
        self.valuation_date = date(int(valuation_date[:4]), int(valuation_date[4:6]), int(valuation_date[6:]))
        #print(self.valuation_date)
        self.functions = [self.ICOSFlag]
        
    def ICOSFlag(self, merger_row, previous_row, fieldnames):
        '''
        logic for the field
        '''
        if 'ICOSFlag' in fieldnames:
            if merger_row['join_indicator'] == 'AB':
                previous_row['ICOSFlag'] = merger_row['ICOSFlag_CQ']
        return previous_row

    