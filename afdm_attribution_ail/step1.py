'''
transformation step-1
'''
#from decimal import Decimal

class Step1:
    '''
    changes to be made in step 1
    '''
    def __init__(self):
        '''
        define fields to be modified in step 1
        '''
        print('Step 1 class')
        self.functions = [self.F133InitGuarCSV_Tax,
                          self.F133GAVFloorValue,
                          self.F133ROPAmt,
                          self.Idx1AOptNomMV,
                          self.Idx2AOptNomMV,
                          self.Idx3AOptNomMV,
                          self.Idx4AOptNomMV,
                          self.Idx5AOptNomMV,
                          self.Idx5ExcessRecLinkID]

    def F133InitGuarCSV_Tax(self, cur, pre):
        '''
        logic for the field
        '''
        return cur
#         cur['F133InitGuarCSV_Tax'] = round(pre['F133InitGuarCSV_Tax'],2)
#         return cur

    def F133GAVFloorValue(self, cur, pre):
        '''
        logic for the field
        '''
        cur['F133GAVFloorValue'] = round(float(pre['F133GAVFloorValue']), 10)
        return cur

    def F133ROPAmt(self, cur, pre):
        '''
        logic for the field
        '''
        cur['F133ROPAmt'] = round(float(pre['F133ROPAmt']), 10)
        return cur

    def Idx5ExcessRecLinkID(self, cur, pre):
        '''
        logic for the field
        '''
        cur['Idx5ExcessRecLinkID'] = pre['Idx5ExcessRecLinkID'][:20]
        return cur

    def AOptNomMV(self, cur, pre, index):
        '''
        logic for the field
        '''
        avif = cur['Idx{}AVIF'.format(index)]
        term = cur['Idx{}Term'.format(index)]
        return cur

    def Idx1AOptNomMV(self, cur, pre):
        '''
        logic for the field
        '''
        return self.AOptNomMV(cur, pre, 1)

    def Idx2AOptNomMV(self, cur, pre):
        '''
        logic for the field
        '''
        return self.AOptNomMV(cur, pre, 2)

    def Idx3AOptNomMV(self, cur, pre):
        '''
        logic for the field
        '''
        return self.AOptNomMV(cur, pre, 3)

    def Idx4AOptNomMV(self, cur, pre):
        '''
        logic for the field
        '''
        return self.AOptNomMV(cur, pre, 4)

    def Idx5AOptNomMV(self, cur, pre):
        '''
        logic for the field
        '''
        return self.AOptNomMV(cur, pre, 5)
