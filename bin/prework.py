'''
pre process the AIL
'''
from datetime import date
from dateutil import relativedelta

class Prework:
#     def __init__(self,args):
#         '''
#         pre process steps
#         '''
#         self.args=args
#         self.stratagies_work()
#         self.rec_link_id()
        
    def stratagies_work(self):
        '''
        stratagies pre processing
        '''
        pass
    
    def rec_link_id(self):
        '''
        rec link id preprocessing
        '''
        pass
    
    def effective_date(self, val_date, row_data, index):
        '''
        process only if effective date is in this quarter
        '''
        x = relativedelta.relativedelta(months=int(row_data['Idx{}TermStart'.format(index)])-1)
        eff_date = date(int(row_data['ck.IssYear']),int(row_data['ck.IssMon']),1) + x
        
        if val_date.month - 3 < eff_date.month and eff_date.month <= val_date.month and index==1 :#and x:
            #print(row_data['PolNo'], eff_date, date(int(row_data['ck.IssYear']),int(row_data['ck.IssMon']),1),x)
            return True
        else:
            return False
        