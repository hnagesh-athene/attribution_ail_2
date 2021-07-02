'''
transformation step-10
'''
from collections import OrderedDict

class Step10:
    '''
    changes to be made in step 10
    '''
    def __init__(self):
        '''
        define fields to be modified in step 10
        '''
        print('Step 10 class')
        self.functions = [self.row_builder,
                          self.GenBudgetOBCurr,
                          self.GenBudgetUltOB,
                          self.IdxBudgetStrategyFee,
                          self.IdxBudgetVolAdjOB,
                          self.IdxBudgetOBCurr,
                          self.IdxBudgetUltOB,
                          self.generate]
        
    def row_builder(self,merger_row, current_row, fieldnames, args):
        """
        default row
        """
        self.row = OrderedDict()
        self.row = {fields:None for fields in fieldnames}
        
        return self.row

    def GenBudgetOBCurr(self, merger_row, current_row, fieldnames, args):
        '''
        logic for the field
        '''
        if 'GenBudgetOBCurr' in fieldnames:
            if merger_row['join_indicator'] == 'AB' and( merger_row['GenAV_PQ'] and float(merger_row['GenAV_PQ']) > 0\
            and merger_row['GenAV_CQ'] and float(merger_row['GenAV_CQ']) > 0):
                current_row['GenBudgetOBCurr'] = merger_row['GenBudgetOBCurr_PQ']
            else:
                current_row['GenBudgetOBCurr'] = merger_row['GenBudgetOBCurr_CQ']
        return current_row
    
    def GenBudgetUltOB(self, merger_row, current_row, fieldnames, args):
        '''
        logic for the field
        '''
        if 'GenBudgetUltOB' in fieldnames:
            if merger_row['join_indicator'] == 'AB' and (merger_row['GenAV_PQ'] and float(merger_row['GenAV_PQ']) > 0\
            and merger_row['GenAV_CQ'] and float(merger_row['GenAV_CQ']) > 0):
                current_row['GenBudgetUltOB'] = merger_row['GenBudgetUltOB_PQ']
            else:
                current_row['GenBudgetUltOB'] = merger_row['GenBudgetUltOB_CQ']
        return current_row
            
    def IdxBudgetStrategyFee(self, merger_row, current_row, fieldnames, args):
        '''
        logic for the field
        '''
        if 'Idx1BudgetStrategyFee' in fieldnames:
            for index in range(1,6):
                if args.block == 'amp':
                    key = merger_row[f'Idx{index}Index_CQ'] + merger_row[f'Idx{index}RecLinkID_CQ']
                else:
                    key = merger_row[f'Idx{index}Index_CQ'] + merger_row[f'Idx{index}RecLinkID_CQ']\
                     + merger_row[f'Idx{index}ANXStrat_CQ']
                if key != '__' and merger_row['join_indicator'] == 'AB':
                    idx = eval(merger_row['__idxordersync_pq']).get(key,index)
                else:
                    idx = index
                if merger_row['join_indicator'] == 'AB' and merger_row[f'Idx{index}RecLinkID_CQ'] != '_'\
                and merger_row[f'_int_idx{idx}_anniv'] == 'N':
                    current_row[f'Idx{index}BudgetStrategyFee'] = merger_row[f'Idx{idx}BudgetStrategyFee_PQ']
                elif merger_row['join_indicator'] == 'AB' and merger_row[f'Idx{index}RecLinkID_CQ'] != '_'\
                and  merger_row[f'_int_idx{idx}_anniv'] == 'Y':
                    current_row[f'Idx{index}BudgetStrategyFee'] = merger_row[f'Idx{idx}BudgetStrategyFee_PQ']
                else:
                    current_row[f'Idx{index}BudgetStrategyFee'] = merger_row[f'Idx{index}BudgetStrategyFee_CQ']
        return current_row
    
    def IdxBudgetVolAdjOB(self, merger_row, current_row, fieldnames, args):
        '''
        logic for the field
        '''
        if 'Idx1BudgetVolAdjOB' in fieldnames:
            for index in range(1,6):
                if args.block == 'amp':
                    key = merger_row[f'Idx{index}Index_CQ'] + merger_row[f'Idx{index}RecLinkID_CQ']
                else:
                    key = merger_row[f'Idx{index}Index_CQ'] + merger_row[f'Idx{index}RecLinkID_CQ']\
                     + merger_row[f'Idx{index}ANXStrat_CQ']
                if key != '__' and merger_row['join_indicator'] == 'AB':
                    idx = eval(merger_row['__idxordersync_pq']).get(key,index)
                else:
                    idx = index
                if merger_row['join_indicator'] == 'AB' and merger_row[f'Idx{index}RecLinkID_CQ'] != '_'\
                and merger_row[f'_int_idx{idx}_anniv'] == 'N':
                    current_row[f'Idx{index}BudgetVolAdjOB'] = merger_row[f'Idx{idx}BudgetVolAdjOB_PQ']
                elif merger_row['join_indicator'] == 'AB' and merger_row[f'Idx{index}RecLinkID_CQ'] != '_'\
                and merger_row[f'_int_idx{idx}_anniv'] == 'Y':
                    current_row[f'Idx{index}BudgetVolAdjOB'] = merger_row[f'Idx{idx}BudgetOBCurr_PQ']
                else:
                    current_row[f'Idx{index}BudgetVolAdjOB'] = merger_row[f'Idx{index}BudgetVolAdjOB_CQ']      
        return current_row
    
    def IdxBudgetOBCurr(self, merger_row, current_row, fieldnames, args):
        '''
        logic for the field
        '''
        if 'Idx1BudgetOBCurr' in fieldnames:
            for index in range(1,6):
                if args.block == 'amp':
                    key = merger_row[f'Idx{index}Index_CQ'] + merger_row[f'Idx{index}RecLinkID_CQ']
                else:
                    key = merger_row[f'Idx{index}Index_CQ'] + merger_row[f'Idx{index}RecLinkID_CQ']\
                     + merger_row[f'Idx{index}ANXStrat_CQ']
                if key != '__' and merger_row['join_indicator'] == 'AB':
                    idx = eval(merger_row['__idxordersync_pq']).get(key,index)
                else:
                    idx = index
                if merger_row['join_indicator'] == 'AB' and merger_row[f'Idx{index}RecLinkID_CQ'] != '_'\
                and merger_row[f'_int_idx{idx}_anniv'] == 'N':
                    current_row[f'Idx{index}BudgetOBCurr'] = merger_row[f'Idx{idx}BudgetOBCurr_PQ']
                elif merger_row['join_indicator'] == 'AB' and merger_row[f'Idx{index}RecLinkID_CQ'] != '_'\
                and merger_row[f'_int_idx{idx}_anniv'] == 'Y':
                    current_row[f'Idx{index}BudgetOBCurr'] = merger_row[f'Idx{idx}BudgetOBCurr_PQ']
                else:
                    current_row[f'Idx{index}BudgetOBCurr'] = merger_row[f'Idx{index}BudgetOBCurr_CQ']
        return current_row
    
    def IdxBudgetUltOB(self, merger_row, current_row, fieldnames, args):
        '''
        logic for the field
        '''
        if 'Idx1BudgetUltOB' in fieldnames:
            for index in range(1,6):
                if args.block == 'amp':
                    key = merger_row[f'Idx{index}Index_CQ'] + merger_row[f'Idx{index}RecLinkID_CQ']
                else:
                    key = merger_row[f'Idx{index}Index_CQ'] + merger_row[f'Idx{index}RecLinkID_CQ']\
                     + merger_row[f'Idx{index}ANXStrat_CQ']
                if key != '__' and merger_row['join_indicator'] == 'AB':
                    idx = eval(merger_row['__idxordersync_pq']).get(key,index)
                else:
                    idx = index
                if merger_row['join_indicator'] == 'AB' and merger_row[f'Idx{index}RecLinkID_CQ'] != '_'\
                and merger_row[f'_int_idx{idx}_anniv'] == 'N':
                    current_row[f'Idx{index}BudgetUltOB'] = merger_row[f'Idx{idx}BudgetUltOB_PQ']
                elif merger_row['join_indicator'] == 'AB' and merger_row[f'Idx{index}RecLinkID_CQ'] != '_'\
                and merger_row[f'_int_idx{idx}_anniv'] == 'Y':
                    current_row[f'Idx{index}BudgetUltOB'] = merger_row[f'Idx{idx}BudgetUltOB_PQ']
                else:
                    current_row[f'Idx{index}BudgetUltOB'] = merger_row[f'Idx{index}BudgetUltOB_CQ']
        return current_row
            
    def generate(self,merge,current_row,fieldnames, args):
        '''
        Default fields
        '''
        for fields in fieldnames:
            if fields in ('PolNo', 'Company'):
                current_row[fields] = merge[fields]
            elif fields not in ['GenBudgetOBCurr','GenBudgetUltOB','Idx1BudgetStrategyFee',\
            'Idx2BudgetStrategyFee','Idx3BudgetStrategyFee','Idx4BudgetStrategyFee','Idx15BudgetStrategyFee',\
            'Idx1BudgetOBCurr','Idx2BudgetOBCurr','Idx3BudgetOBCurr','Idx4BudgetOBCurr',\
            'Idx5BudgetOBCurr','Idx1BudgetUltOB','Idx2BudgetUltOB','Idx3BudgetUltOB',\
            'Idx4BudgetUltOB','Idx5BudgetUltOB','Idx1BudgetVolAdjOB','Idx2BudgetVolAdjOB',\
            'Idx3BudgetVolAdjOB','Idx4BudgetVolAdjOB','Idx5BudgetVolAdjOB']:
            
                current_row[fields] = merge[fields+'_CQ']
        return current_row
