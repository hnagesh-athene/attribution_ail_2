'''
transformation step-5
'''
#from decimal import Decimal
from prework import Prework
from datetime import date

class Step5:
    '''
    changes to be made in step 5
    '''
    def __init__(self, valuation_date):
        '''
        define fields to be modified in step 5
        '''
        self.valuation_date = date(int(valuation_date[:4]), int(valuation_date[4:6]), int(valuation_date[6:]))
        self.fields = ['GMWBCharge','GMWBMaxBenTable','GMWBInitAccumPeriod','GMWBMaxAccumPeriod','GMWBMaxCharge',\
                        'AdditionalRestartMonths','RestartCharge','Seed']
        self.functions = [self.step_5,
                          self.IRRestartNew]
                          
    
    def IRRestartNew(self, merger_row, current_row, fieldnames):
        '''
        logic for the field
        '''
        if 'IRRestartNew' in fieldnames:
            if merger_row['IRRestartNew_PQ'] in ("F","X") and merger_row['IRRestartNew_CQ'] in ("Y","N"):
                if merger_row['IRRestartNew_PQ'] == 'F':
                    current_row['IRRestartNew'] = 'Y'
                else:
                    current_row['IRRestartNew'] = 'N'
        return current_row
    
    def Step5_functions(self, merger_row, current_row, field):
        '''
        logic for the field
        '''
        if merger_row['join_indicator'] == 'AB' and merger_row['IRRestartNew_PQ'] in ("F","X") and \
        merger_row['IRRestartNew_CQ'] in ("Y","N"):
            current_row[f'{field}'] = merger_row[f'{field}'+'_PQ']
        
        return current_row
    
    def step_5(self,merger_row,current_row,fieldnames):
        
        for field in self.fields:
            if field in fieldnames:
                current_row = self.Step5_functions(merger_row,current_row,field)
            
        return current_row
    