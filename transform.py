'''
calculate/transform the required fields depending on admin system
'''
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

from profile import Profile

class Transform:
    '''
    generate step 1- 10 atttribution_ails
    '''
    def __init__(self, args):
        '''
        initialize the class object
        '''
        self.args = args
        self.cur = args.current_path
        self.prev = args.previous_path
        self.fieldnames = []
        file_name = args.current_path.split('\\')
        self.output_path = 'output/'+args.valuation_date+'/{}_'+file_name[-1]
        self.profile=Profile(self.args.admin_system)
        self.all_steps=[self.step_1,self.step_2,self.step_3,self.step_4,self.step_5,
                        self.step_6,self.step_7,self.step_8,self.step_9,self.step_10]
        steps=self.steps_profile(self.profile)
        self.generate(steps)
    
    def steps_profile(self, profile):
        return [i[1] for i in zip(profile.steps_required, self.all_steps) if i[0]]

    def generate(self, steps):
        '''
        read transform and create output files
        '''
        [func() for func in steps]

    def transformer(self, input_1, input_2, output, step, changes=None):
        '''
        transform logic
        '''
        progress = tqdm.tqdm(mininterval=1, unit=' rows', desc='rows checked '+step)
        cur_row = next(input_1)
        prev_row = next(input_2)
        policy = ''
        while cur_row:
            try:
                if prev_row['Company']+prev_row['PolNo'] < cur_row['Company']+cur_row['PolNo']:
                    prev_row = next(input_2)
                elif prev_row['Company']+prev_row['PolNo'] > cur_row['Company']+cur_row['PolNo']:
                    output.send(cur_row)
                    policy = cur_row['Company']+cur_row['PolNo']
                    cur_row = next(input_1)
                else:
                    #operations
                    for func in changes.functions:
                        cur_row = func(cur_row, prev_row)
                    output.send(cur_row)
                    policy = cur_row['Company']+cur_row['PolNo']
                    cur_row = next(input_1)
                    prev_row = next(input_2)
            except StopIteration:
                if policy != cur_row['Company']+cur_row['PolNo']:
                    output.send(cur_row)
                break
            progress.update()
        try:
            cur_row = next(input_1)
            while cur_row:
                output.send(cur_row)
                cur_row = next(input_1)
        except StopIteration:
            progress.update()
        progress.close()
            #print('\ndone')

    def reader(self, file_1, file_2):
        '''
        reader
        '''
        #print('Inputs read')
        fp_1 = open(file_1)
        fp_2 = open(file_2)
        obj1 = FastDictReader(fp_1, delimiter='\t')
        self.fieldnames = obj1.fieldnames
        obj2 = FastDictReader(fp_2, delimiter='\t')
        return obj1, obj2

    def writer(self, step):
        '''
        writer
        '''
        #print('output write')
        write = CSVDataIO(delimiter='\t')
        cols = self.fieldnames
        file = write.iterative_writer(self.output_path.format(step), cols)
#        write.write_file('test.ail', cols, self.res.values())
        return file

#     def close(self, fp_1, fp_2):
#         '''
#         close the files
#         '''
#         fp_1.close()
#         fp_2.close()

    def step_1(self):
        '''
        step 1
        '''
        print('step - 1')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_1')
        changes = Step1()
        self.transformer(cur, prev, output, 'step-1', changes)
        #self.close(cur,  prev)
        self.prev = self.cur
        self.cur = self.output_path.format('step_1')

    def step_2(self):
        '''
        step 2
        '''
        print('step - 2')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_2')
        changes = Step2()
        self.transformer(cur, prev, output, 'step-2', changes)
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = self.output_path.format('step_2')

    def step_3(self):
        '''
        step 3
        '''
        print('step - 3')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_3')
        changes = Step3()
        self.transformer(cur, prev, output, 'step-3', changes)
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = self.output_path.format('step_3')

    def step_4(self):
        '''
        step 4
        '''
        print('step - 4')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_4')
        changes = Step4()
        self.transformer(cur, prev, output, 'step-4', changes)
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = self.output_path.format('step_4')

    def step_5(self):
        '''
        step 5
        '''
        print('step - 5')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_5')
        changes = Step5()
        self.transformer(cur, prev, output, 'step-5', changes)
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = self.output_path.format('step_5')

    def step_6(self):
        '''
        step 6
        '''
        print('step - 6')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_6')
        changes = Step6()
        self.transformer(cur, prev, output, 'step-6', changes)
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = self.output_path.format('step_6')

    def step_7(self):
        '''
        step 7
        '''
        print('step - 7')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_7')
        changes = Step7()
        self.transformer(cur, prev, output, 'step-7', changes)
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = self.output_path.format('step_7')

    def step_8(self):
        '''
        step 8
        '''
        print('step - 8')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_8')
        changes = Step8()
        self.transformer(cur, prev, output, 'step-8', changes)
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = self.output_path.format('step_8')

    def step_9(self):
        '''
        step 9
        '''
        print('step - 9')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_9')
        changes = Step9()
        self.transformer(cur, prev, output, 'step-9', changes)
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = self.output_path.format('step_9')

    def step_10(self):
        '''
        step 10
        '''
        print('step - 10')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_10')
        changes = Step10()
        self.transformer(cur, prev, output, 'step-10', changes)
        #self.close(cur, prev)

# example definition of actual transformation function
#     def AVIF(self, cur_row, prev_row):
#         '''
#         this is an example how to transform the data
#         '''
#         cur_row['AVIF'] = prev_row['AVIF']
#         return cur_row
    