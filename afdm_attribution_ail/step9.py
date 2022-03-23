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
        self.functions = [self.generate,
                          self.IdxAOptNomMV_formater]

    def IdxAOptNomMV(self, merger_row, current_row, index, idx):
        '''
        logic for the field
        '''

        if merger_row['join_indicator'] == 'AB':
            if merger_row[f'_int_idx{index}_RecLinkID_CQ'] != '_' and merger_row[f'_int_idx{index}_anniv_cq'] == 'N':
                current_row[f'Idx{index}AOptNomMV'] = float(merger_row[f'Idx{idx}AOptNomMV_PQ']) \
                                                      * ((1 + float(merger_row[f'_int_idx{idx}_eor'])) ** (1 / 4))
            if merger_row[f'_int_idx{index}_RecLinkID_CQ'] != '_' and merger_row[f'_int_idx{index}_anniv_cq'] == 'Y':
                current_row[f'Idx{index}AOptNomMV'] = float(merger_row[f'Idx{index}IncepCost_CQ']) \
                                                      * ((1 + float(merger_row[f'_int_idx{idx}_eor'])) \
                                                         ** (float(merger_row[f'_int_idx{idx}_days']) / 365))
        return current_row

    def IdxAOptNomMV_formater(self, merger_row, current_row, fieldnames, args):
        '''
        Running the process for all index
        '''
        for index in range(1, 6):
            if f'Idx{index}AOptNomMV' in fieldnames:
                key = merger_row[f"_int_idx{index}_RecLinkID_CQ"]
                if key != '_' and merger_row['join_indicator'] == 'AB':
                    idx = eval(merger_row["__idxordersync_pq"]).get(key, index)
                else:
                    idx = index
                current_row = self.IdxAOptNomMV(merger_row, current_row, index, idx)
        return current_row

    def generate(self, merger_row, current_row, fieldnames, args):
        """
        Default fields
        """
        for fields in fieldnames:
            if fields in ("PolNo", "Company"):
                current_row[fields] = merger_row[fields]
            else:
                current_row[fields] = merger_row[fields + "_CQ"]
        return current_row