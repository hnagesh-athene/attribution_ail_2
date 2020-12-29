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
        print('Step 1 class')
        self.valuation_date = date(int(valuation_date[:4]), int(valuation_date[4:6]), int(valuation_date[6:]))
        #print(self.valuation_date)
        self.functions = [self.ICOSFlag]
        
    def ICOSFlag(self, previous_row, current_row):
        '''
        logic for the field
        '''
        previous_row['ICOSFlag'] = current_row['ICOSFlag']
        return previous_row
#         cur['F133InitGuarCSV_Tax'] = round(pre['F133InitGuarCSV_Tax'],2)
#         return cur

    