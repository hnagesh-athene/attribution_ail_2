'''
transformation step-1
'''


class Step1:
    '''
    changes to be made in step 1
    '''

    def __init__(self):
        '''
        define fields to be modified in step 1
        '''
        print('Step 1 class')
        self.functions = [self.IdxAOptNomMV,
                          self.generate]

    def IdxAOptNomMV(self, merge, previous_row, fieldnames, args):
        '''
        logic for the field
        '''
        if 'Idx1AOptNomMV' in fieldnames:
            if merge['Company'] == 'CU':
                return previous_row
            sum_idx_avif = 0
            for i in range(1, 6):
                if args.block in ('Rocky.fia', 'Rocky.tda') and merge[f'Idx{i}AVIF_PQ']:
                    sum_idx_avif += float(merge[f'Idx{i}AVIF_PQ'])
                elif merge[f'Idx{i}AVIF_PQ'] and merge[f'_int_idx{i}_anniv'] == 'Y':
                    sum_idx_avif += float(merge[f'Idx{i}AVIF_PQ'])
            for i in range(1, 6):
                if args.block in ('voya_fia', 'voya_fa', 'Rocky.fia', 'Rocky.tda') and\
                 merge[f'_int_idx{i}_anniv'] == 'Y' and float(merge['index_credit']) == 0:
                    previous_row[f'Idx{i}AOptNomMV'] = 0
                elif args.block in ('jackson.tda', 'jackson.fia') and\
                 merge[f'_int_idx{i}_anniv'] == 'Y' and float(merge['index_credit']) == 0:
                    previous_row[f'Idx{i}AOptNomMV'] = merge[f'Idx{i}AOptNomMV_PQ']
                elif args.block in ('voya_fia', 'voya_fa', 'jackson.tda', 'jackson.fia', 'Rocky.fia', 'Rocky.tda') and\
                 merge[f'_int_idx{i}_anniv'] == 'Y' and float(merge[f'Idx{i}AVIF_PQ']) == 0:
                    previous_row[f'Idx{i}AOptNomMV'] = 0
                elif args.block in ('voya_fia', 'voya_fa', 'Rocky.fia', 'Rocky.tda') and merge[f'_int_idx{i}_anniv'] == 'N':
                    previous_row[f'Idx{i}AOptNomMV'] = merge[f'Idx{i}AOptNomMV_PQ']
                elif merge[f'_int_idx{i}_anniv'] == 'Y' and float(merge['index_credit']) != 0 \
                        and sum_idx_avif != 0:
                    previous_row[f'Idx{i}AOptNomMV'] = float(merge['index_credit']) / sum_idx_avif
                elif float(merge[f'Idx{i}AVIF_PQ']) != 0:
                    previous_row[f'Idx{i}AOptNomMV'] = merge[f'Idx{i}AOptNomMV_PQ']
                else:
                    previous_row[f'Idx{i}AOptNomMV'] = 0
        return previous_row

    def generate(self, merge, previous_row, fieldnames, args):
        '''
        Default fields
        '''
        for fields in fieldnames:
            if fields in ('PolNo', 'Company'):
                previous_row[fields] = merge[fields]
            elif merge['Company'] == 'CU':
                previous_row[fields] = merge[fields + '_PQ']
            elif fields not in ['Idx1AOptNomMV', 'Idx5AOptNomMV', 'Idx2AOptNomMV',
                                'Idx3AOptNomMV', 'Idx4AOptNomMV']:
                previous_row[fields] = merge[fields + '_PQ']
        return previous_row
