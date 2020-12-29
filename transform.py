'''
calculate/transform the required fields depending on admin system
'''
#from profile import Profile
import tqdm
#from collections import OrderedDict
#from core_utils.progress import sane_tqdm
#from core_utils.tabular import FastDictReader
#from core_utils.tabular import CSVDataIO
from core_utils.core_utils.tabular import FastDictReader
from core_utils.core_utils.tabular import CSVDataIO

from afdm_attribution_ail.step1 import Step1
from afdm_attribution_ail.step2 import Step2
from afdm_attribution_ail.step3 import Step3
from afdm_attribution_ail.step4 import Step4
from afdm_attribution_ail.step5 import Step5
from afdm_attribution_ail.step6 import Step6
from afdm_attribution_ail.step7 import Step7
from afdm_attribution_ail.step8 import Step8
from afdm_attribution_ail.step9 import Step9
from afdm_attribution_ail.step10 import Step10
from prework import Prework


class Transform(Prework):
    '''
    generate step 1- 10 atttribution_ails
    '''
    def __init__(self, args):
        '''
        initialize the class object
        '''
        self.args = args
        print(self.args.valuation_date)
        self.cur = args.current_path
        self.prev = args.previous_path
        self.fieldnames = []
        file_name = args.current_path.split('\\')
        self.output_path = 'output/'+args.valuation_date+'/{}_'+file_name[-1]
        #self.profile=Profile(self.args.admin_system)
        #self.profile=self.create_profile(slef.args.admin_system)
        #self.all_steps=[self.step_1,self.step_2,self.step_3,self.step_4,self.step_5,
        #                self.step_6,self.step_7,self.step_8,self.step_9,self.step_10]
        self.steps = self.steps_profile(self.args.admin_system)
        self.changes = self.change_steps()
        self.cur_file, self.prev_file = self.reader(self.cur, self.prev)
        self.writers = self.generate()
        self.transformer(self.prev_file, self.cur_file, self.steps)

    def steps_profile(self, admin_system):
        '''
        define the steps for admin system
        '''
        print('steps_profile')
        if admin_system == 'as400':
            return [self.step_1, self.step_2, self.step_3, self.step_4, self.step_5,
                    self.step_6, self.step_7, self.step_8, self.step_9, self.step_10]
        #return [self.step for step in Profile(self.args.admin_system).steps_required]

    def change_steps(self):
        '''
        get the field names and functions for the respective steps
        '''
        print('steps_change')
        d=self.args.valuation_date
        return [Step1(d), Step2(d), Step3(), Step4(), Step5(),
                Step6(), Step7(), Step8(), Step9(), Step10()]

    def generate(self):
        '''
        read transform and create output files
        '''
        print('generate')
        return [self.writer('step{}'.format(i+1)) if self.steps[i] else None for i in range(10)]

    def transformer(self, input_1, input_2, steps):
        '''
        transform logic
        '''
        print('transform')
        progress = tqdm.tqdm(mininterval=1, unit=' rows', desc='rows checked ')
        previous_row = next(input_1)
        current_row = next(input_2)
        policy = ''
        while previous_row:
            try:
                if current_row['Company']+current_row['PolNo'] < previous_row['Company']+previous_row['PolNo']:
                    current_row = next(input_2)
                elif current_row['Company']+current_row['PolNo'] > previous_row['Company']+previous_row['PolNo']:
                    self.write_all(previous_row)
#                     for func in steps:
#                         previous_row = func(previous_row)
                    policy = previous_row['Company']+previous_row['PolNo']
                    previous_row = next(input_1)
                else:
                    #operations
                    for func in steps:
                        previous_row = func(previous_row, current_row)
                    policy = previous_row['Company']+previous_row['PolNo']
                    previous_row = next(input_1)
                    current_row = next(input_2)
            except StopIteration:
                if policy != previous_row['Company']+previous_row['PolNo']:
                    self.write_all(previous_row)
#                     for func in steps:
#                         previous_row = func(previous_row)
                break
            progress.update()
        try:
            previous_row = next(input_1)
            while previous_row:
#                 for func in steps:
#                     previous_row = func(previous_row)
                self.write_all(previous_row)
                previous_row = next(input_1)
        except StopIteration:
            progress.update()
        progress.close()
        print(self.changes[0].count)
        print(self.changes[0].c)
            ##print('\ndone')

    def reader(self, file_1, file_2):
        '''
        reader
        '''
        ##print('Inputs read')
        #print(file_1,file_2)
        print('reader')
        fp_1 = open(file_1)
        fp_2 = open(file_2)
        obj1 = FastDictReader(fp_1, delimiter='\t')
        self.fieldnames = obj1.fieldnames
        #print(self.fieldnames)
        obj2 = FastDictReader(fp_2, delimiter='\t')
        return obj1, obj2

    def writer(self, step):
        '''
        writer
        '''
        print('output write')
        write = CSVDataIO(delimiter='\t')
        cols = self.fieldnames
        #print(cols)
        file = write.iterative_writer(self.output_path.format(step), cols)
#        write.write_file('test.ail', cols, self.res.values())
        return file

#     def close(self, fp_1, fp_2):
#         '''
#         close the files
#         '''
#         fp_1.close()
#         fp_2.close()

    def step_1(self, previous_row, current_row=None):
        '''
        step 1
        '''
#        #print('step - 1')
#         cur, prev = self.reader(self.cur, self.prev)
#         output = self.writer('step_1')
#         changes = Step1()
        if current_row:
            #print( self.changes[0].functions)
            #print( self.changes[0])
            for change in self.changes[0].functions:
                #print(change)
                previous_row = change(previous_row, current_row)
        self.writers[0].send(previous_row)
        return previous_row
        #self.close(cur,  prev)
#         self.prev = self.cur
#         self.cur = self.output_path.format('step_1')

    def step_2(self, previous_row, current_row=None):
        '''
        step 2
        '''
        if current_row:
            #print( self.changes[1].functions)
            #print( self.changes[1])
            for change in self.changes[1].functions:
                #print(change)
                previous_row = change(previous_row, current_row)
            self.writers[1].send(previous_row)
        return previous_row

    def step_3(self, previous_row, current_row=None):
        '''
        step 3
        '''
        if current_row:
            #print( self.changes[2].functions)
            #print( self.changes[2])
            for change in self.changes[2].functions:
                #print(change)
                previous_row = change(previous_row, current_row)
            self.writers[2].send(previous_row)
        return previous_row

    def step_4(self, previous_row, current_row=None):
        '''
        step 4
        '''
        #print('step - 4')
        if current_row:
            #print( self.changes[3].functions)
            #print( self.changes[3])
            for change in self.changes[3].functions:
                #print(change)
                previous_row = change(previous_row, current_row)
            self.writers[3].send(previous_row)
        return previous_row

    def step_5(self, previous_row, current_row=None):
        '''
        step 5
        '''
        if current_row:
            #print( self.changes[4].functions)
            #print( self.changes[4])
            for change in self.changes[4].functions:
                #print(change)
                previous_row = change(previous_row, current_row)
            self.writers[4].send(previous_row)
        return previous_row

    def step_6(self, previous_row, current_row=None):
        '''
        step 6
        '''
        if current_row:
            #print( self.changes[5].functions)
            #print( self.changes[5])
            for change in self.changes[5].functions:
                #print(change)
                previous_row = change(previous_row, current_row)
            self.writers[5].send(previous_row)
        return previous_row

    def step_7(self, previous_row, current_row=None):
        '''
        step 7
        '''
        if current_row:
            #print( self.changes[6].functions)
            #print( self.changes[6])
            for change in self.changes[6].functions:
                #print(change)
                previous_row = change(previous_row, current_row)
            self.writers[6].send(previous_row)
        return previous_row

    def step_8(self, previous_row, current_row=None):
        '''
        step 8
        '''
        if current_row:
            #print( self.changes[7].functions)
            #print( self.changes[7])
            for change in self.changes[7].functions:
                #print(change)
                previous_row = change(previous_row, current_row)
            self.writers[7].send(previous_row)
        return previous_row

    def step_9(self, previous_row, current_row=None):
        '''
        step 9
        '''
        if current_row:
            #print( self.changes[8].functions)
            #print( self.changes[8])
            for change in self.changes[8].functions:
                #print(change)
                previous_row = change(previous_row, current_row)
            self.writers[8].send(previous_row)
        return previous_row

    def step_10(self, previous_row, current_row=None):
        '''
        step 10
        '''
        if current_row:
            #print( self.changes[9].functions)
            #print( self.changes[9])
            for change in self.changes[9].functions:
                #print(change)
                previous_row = change(previous_row, current_row)
            self.writers[9].send(previous_row)
        return previous_row
        #self.close(cur, prev)
    
    def write_all(self, previous_row):
        '''
        write directly if no logic is involved
        '''
        for step in self.writers:
            if step:
                step.send(previous_row)

# example definition of actual transformation function
#     def AVIF(self, previous_row, current_row):
#         '''
#         this is an example how to transform the data
#         '''
#         previous_row['AVIF'] = current_row['AVIF']
#         return previous_row
    