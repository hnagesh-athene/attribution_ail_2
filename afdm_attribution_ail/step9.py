'''
transformation step-9
'''


class Step9:
    '''
    changes to be made in step 9
    '''

    def __init__(self):
        '''
        define fields to be modified in step 9
        '''
        self.functions = [self.IdxAOptNomMV_formater]

    def IdxAOptNomMV(self, merger_row, current_row, index, idx):
        '''
        logic for the field
        '''
        if merger_row['join_indicator'] == 'A':
            current_row[f'Idx{index}AOptNomMV'] = merger_row[f'Idx{index}IncepCost_CQ']

        if merger_row['join_indicator'] == 'AB':
            if merger_row[f'Idx{index}RecLinkID_CQ'] != '_' and merger_row[f'_int_idx{idx}_anniv'] == 'N':
                current_row[f'Idx{index}AOptNomMV'] = float(merger_row[f'Idx{idx}AOptNomMV_PQ']) \
                                                      * ((1 + float(merger_row[f'_int_idx{idx}_eor'])) ** (1 / 4))
            if merger_row[f'Idx{index}RecLinkID_CQ'] != '_' and merger_row[f'_int_idx{idx}_anniv'] == 'Y':
                current_row[f'Idx{index}AOptNomMV'] = float(merger_row[f'Idx{idx}BudgetVolAdjOB_PQ']) \
                                                      * ((1 + float(merger_row[f'_int_idx{idx}_eor'])) \
                                                         ** (float(merger_row[f'_int_idx{idx}_days']) / 365))
        return current_row

    def IdxAOptNomMV_formater(self, merger_row, current_row, fieldnames, args):
        '''
        Running the process for all index
        '''
        if 'Idx1AOptNomMV' in fieldnames:
            for index in range(1, 6):
                if args.block == 'amp':
                    key = merger_row[f'Idx{index}Index_CQ'] + merger_row[f'Idx{index}RecLinkID_CQ']
                else:
                    key = merger_row[f'Idx{index}Index_CQ'] + merger_row[f'Idx{index}RecLinkID_CQ'] \
                          + merger_row[f'Idx{index}ANXStrat_CQ']
                if key != '__' and merger_row['join_indicator'] == 'AB':
                    idx = eval(merger_row['__idxordersync_pq']).get(key, index)
                else:
                    idx = index
                current_row = self.IdxAOptNomMV(merger_row, current_row, index, idx)
        return current_row
