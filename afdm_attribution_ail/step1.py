'''
transformation step-1
'''

from prework import Prework
from datetime import date
from collections import OrderedDict
from Columns import AIL_Columns as order


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
        
        self.functions = [self.row_builder,
                          self.IdxAOptNomMV,
                          self.generate]
        

    def row_builder(self,merger_row, current_row):
        
        
        self.row = OrderedDict()
        self.row = {fields:None for fields in order}
        
        return self.row
    
    def IdxAOptNomMV(self, merge, previous_row):
        '''
        logic for the field
        '''
        
        sum_idx_avif = 0
        
        for i in range(1,6):
            if merge['Idx{}AVIF_PQ'.format(i)]:
                sum_idx_avif += float(merge['Idx{}AVIF_PQ'.format(i)])
        
        
        if merge['index_credit'] != '0.0' and sum_idx_avif != 0:
            
            previous_row['Idx1AOptNomMV'] = float(merge['index_credit'])/sum_idx_avif
            previous_row['Idx2AOptNomMV'] = float(merge['index_credit'])/sum_idx_avif
            previous_row['Idx3AOptNomMV'] = float(merge['index_credit'])/sum_idx_avif
            previous_row['Idx4AOptNomMV'] = float(merge['index_credit'])/sum_idx_avif
            previous_row['Idx5AOptNomMV'] = float(merge['index_credit'])/sum_idx_avif
           
        else:
            previous_row['Idx1AOptNomMV'] = merge['Idx1AOptNomMV_PQ']
            previous_row['Idx2AOptNomMV'] = merge['Idx2AOptNomMV_PQ']
            previous_row['Idx3AOptNomMV'] = merge['Idx3AOptNomMV_PQ']
            previous_row['Idx4AOptNomMV'] = merge['Idx4AOptNomMV_PQ']
            previous_row['Idx5AOptNomMV'] = merge['Idx5AOptNomMV_PQ']
            
        
        return previous_row
           
    def generate(self,merge,previous_row):
        '''
        Default fields
        '''
        
        for fields in order:
            if fields in ('PolNo', 'Company'):
                previous_row[fields] = merge[fields]
            elif fields not in ['Idx1AOptNomMV', 'Idx5AOptNomMV', 'Idx2AOptNomMV', 'Idx3AOptNomMV', 'Idx4AOptNomMV']:
                previous_row[fields] = merge[fields+'_PQ']
        
        
        return previous_row
