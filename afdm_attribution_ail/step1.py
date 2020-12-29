'''
transformation step-1
'''
#from decimal import Decimal
from prework import Prework
from datetime import date

class Step1(Prework):
    '''
    changes to be made in step 1
    '''
    def __init__(self, valuation_date):
        '''
        define fields to be modified in step 1
        '''
        print('Step 1 class')
        self.valuation_date = date(int(valuation_date[:4]), int(valuation_date[4:6]), int(valuation_date[6:]))
        #print(self.valuation_date)
        self.functions = [self.F133InitGuarCSV_Tax,
                          self.F133GAVFloorValue,
                          self.F133ROPAmt,
                          self.Idx1AOptNomMV,
                          self.Idx2AOptNomMV,
                          self.Idx3AOptNomMV,
                          self.Idx4AOptNomMV,
                          self.Idx5AOptNomMV,
                          self.Idx5ExcessRecLinkID]
        self.count=0
        self.c=0

    def F133InitGuarCSV_Tax(self, previous_row, current_row):
        '''
        logic for the field
        '''
        return previous_row
#         previous_row['F133InitGuarCSV_Tax'] = round(current_row['F133InitGuarCSV_Tax'],2)
#         return previous_row

    def F133GAVFloorValue(self, previous_row, current_row):
        '''
        logic for the field
        '''
        previous_row['F133GAVFloorValue'] = round(float(current_row['F133GAVFloorValue']), 10)
        return previous_row

    def F133ROPAmt(self, previous_row, current_row):
        '''
        logic for the field
        '''
        previous_row['F133ROPAmt'] = round(float(current_row['F133ROPAmt']), 10)
        return previous_row

    def Idx5ExcessRecLinkID(self, previous_row, current_row):
        '''
        logic for the field
        '''
        previous_row['Idx5ExcessRecLinkID'] = current_row['Idx5ExcessRecLinkID'][:20]
        return previous_row

    def AOptNomMV(self, previous_row, current_row, index):
        '''
        logic for the field
        '''
        if self.effective_date(self.valuation_date, previous_row, index):
            #logic
            self.count+=1
        avif = previous_row['Idx{}AVIF'.format(index)]
        term = previous_row['Idx{}Term'.format(index)]
        return previous_row

    def Idx1AOptNomMV(self, previous_row, current_row):
        '''
        logic for the field
        '''
        self.c+=1
        return self.AOptNomMV(previous_row, current_row, 1)

    def Idx2AOptNomMV(self, previous_row, current_row):
        '''
        logic for the field
        '''
        return self.AOptNomMV(previous_row, current_row, 2)

    def Idx3AOptNomMV(self, previous_row, current_row):
        '''
        logic for the field
        '''
        return self.AOptNomMV(previous_row, current_row, 3)

    def Idx4AOptNomMV(self, previous_row, current_row):
        '''
        logic for the field
        '''
        return self.AOptNomMV(previous_row, current_row, 4)

    def Idx5AOptNomMV(self, previous_row, current_row):
        '''
        logic for the field
        '''
        return self.AOptNomMV(previous_row, current_row, 5)
