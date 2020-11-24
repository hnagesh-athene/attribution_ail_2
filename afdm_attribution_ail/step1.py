'''
transformation step-1
'''
#from decimal import Decimal

class Step1:
    def __init__(self):
        self.functions=[self.F133InitGuarCSV_Tax,
                self.F133GAVFloorValue,
                self.F133ROPAmt,
                self.Idx1AOptNomMV,
                self.Idx2AOptNomMV,
                self.Idx3AOptNomMV,
                self.Idx4AOptNomMV,
                self.Idx5AOptNomMV,
                self.Idx5ExcessRecLinkID]
    
    def F133InitGuarCSV_Tax(self, cur, pre):
        return cur
#         cur['F133InitGuarCSV_Tax']=round(pre['F133InitGuarCSV_Tax'],2)
#         return cur

    def F133GAVFloorValue(self, cur, pre):
        cur['F133GAVFloorValue']=round(float(pre['F133GAVFloorValue']),10)
        return cur
    
    def F133ROPAmt(self,cur,pre):
        cur['F133ROPAmt']=round(float(pre['F133ROPAmt']),10)
        return cur
    
    def Idx5ExcessRecLinkID(self,cur,pre):
        cur['Idx5ExcessRecLinkID']=pre['Idx5ExcessRecLinkID'][:20]
        return cur
    
    def AOptNomMV(self,cur,pre,index):
        avif=cur['Idx{}AVIF'.format(index)]
        term=cur['Idx{}Term'.format(index)]
        return cur
    
    def Idx1AOptNomMV(self,cur,pre):
        return self.AOptNomMV(cur,pre,1)
    
    def Idx2AOptNomMV(self,cur,pre):
        return self.AOptNomMV(cur,pre,2)
    
    def Idx3AOptNomMV(self,cur,pre):
        return self.AOptNomMV(cur,pre,3)
    
    def Idx4AOptNomMV(self,cur,pre):
        return self.AOptNomMV(cur,pre,4)

    def Idx5AOptNomMV(self,cur,pre):
        return self.AOptNomMV(cur,pre,5)
        