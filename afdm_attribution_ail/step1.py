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
        

    def row_builder(self,merger_row, current_row, fieldnames):
        
        
        self.row = OrderedDict()
        self.row = {fields:None for fields in fieldnames}
        
        return self.row
    
    def IdxAOptNomMV(self, merge, previous_row, fieldnames):
        '''
        logic for the field
        '''
        if 'Idx1AOptNomMV' in fieldnames:
            
            if merge['Company'] == 'CU':
                return previous_row
            else:
                sum_idx_avif = 0
                
                for i in range(1,6):
                    if merge[f'Idx{i}AVIF_PQ'] and merge[f'_int_idx{i}_anniv'] == 'Y':
                        sum_idx_avif += float(merge[f'Idx{i}AVIF_PQ'])
                 
                for i in range(1,6):
                    if merge[f'_int_idx{i}_anniv'] == 'Y' and float(merge['index_credit']) != 0 and sum_idx_avif != 0:
                        previous_row[f'Idx{i}AOptNomMV'] = float(merge['index_credit'])/sum_idx_avif
                    elif float(merge[f'Idx{i}AVIF_PQ']) != 0:
                        previous_row[f'Idx{i}AOptNomMV'] = merge[f'Idx{i}AOptNomMV_PQ']
                    else:
                        previous_row[f'Idx{i}AOptNomMV'] = 0
                
    
        return previous_row
           
    def generate(self,merge,previous_row,fieldnames):
        '''
        Default fields
        '''
        
        for fields in fieldnames:
            if fields in ('PolNo', 'Company'):
                previous_row[fields] = merge[fields]
            elif merge['Company'] == 'CU':
                previous_row[fields] = merge[fields+'_PQ']
            elif fields not in ['Idx1AOptNomMV', 'Idx5AOptNomMV', 'Idx2AOptNomMV', 'Idx3AOptNomMV', 'Idx4AOptNomMV']:
                previous_row[fields] = merge[fields+'_PQ']
        
        
        return previous_row
