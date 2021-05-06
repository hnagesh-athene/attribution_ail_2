'''
calculate/transform the required fields depending on admin system
'''

import tqdm
import datetime
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
from Columns import AIL_Columns
from prework import Prework
from core_utils.dates import shift_date, shift_quarters


class Transform(Prework):
    '''
    generate step 1- 10 atttribution_ails
    '''
    def __init__(self, args):
        '''
        initialize the class object
        '''
        self.args = args
        valdate = datetime.datetime.strptime(self.args.valuation_date,'%Y%m%d').date()
        self.previous = ''.join(str(shift_quarters(valdate,-1)).split('-'))
        self.merger = args.merger_path
        self.fieldnames = AIL_Columns
        self.output_path = 'output/'+args.valuation_date+'/{}_'+r'ail.fia.{}.ail2'

        self.steps = self.steps_profile(self.args.admin_system)
        self.changes = self.change_steps()
        self.merger_file = self.reader(self.merger)
        self.writers = self.generate()
        self.transformer(self.merger_file, self.steps)

    def steps_profile(self, admin_system):
        '''
        define the steps for admin system
        '''
        print('steps_profile')
        if admin_system == 'as400':
            return [self.step_1, self.step_2,self.step_10,self.step_9,self.step_8,
                    self.step_7,self.step_6,self.step_5,self.step_3]

    def change_steps(self):
        '''
        get the field names and functions for the respective steps
        '''
        print('steps_change')
        d=self.args.valuation_date
        return [Step1(d), Step2(d), Step10(d), Step9(d), Step8(d), Step7(d), Step6(d),Step5(d),Step3(d)]


    def generate(self):
        '''
        read transform and create output files
        '''
        print('generate')
        return [self.writer('step1'),self.writer('step2'),self.writer('step10'),self.writer('step9'),
                self.writer('step8'),self.writer('step7'),self.writer('step6'),self.writer('step5'),
                self.writer('step3')]

    def transformer(self, input_1, steps):
        '''
        transform logic
        '''
        print('transform')
        progress = tqdm.tqdm(mininterval=1, unit=' rows', desc='rows checked ')
        merger_row = next(input_1)
        previous_row = None
        
        while merger_row:
            try:
                for idx,func in enumerate(steps):
                    if idx != 3:
                        previous_row = func(merger_row, previous_row)
                    if idx == 2:
                        temp = previous_row
                    if idx == 3:
                        previous_row = self.step_9(merger_row,temp)
                merger_row = next(input_1)
                        
            except StopIteration:
                break
            progress.update()
        progress.close()

    def reader(self, file_1):
        '''
        reader
        '''
        print('reader')
        fp_1 = open(file_1)
        obj1 = FastDictReader(fp_1, delimiter='\t')
        return obj1

    def writer(self, step):
        '''
        writer
        '''
        print('output write')
        write = CSVDataIO(delimiter='\t')
        cols = self.fieldnames
        
        if step == 'step1' or step == 'step2':
            file = write.iterative_writer(self.output_path.format(step,self.previous), cols)
            return file
        else:
            file = write.iterative_writer(self.output_path.format(step,self.args.valuation_date), cols)
            return file

    def step_1(self, merger_row, previous_row=None):
        '''
        step 1
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'B'):
                for change in self.changes[0].functions:
                    previous_row = change(merger_row,previous_row)
                self.writers[0].send(previous_row)
        return previous_row

    def step_2(self, merger_row, previous_row):
        '''
        step 2
        '''
        if merger_row and previous_row:
            if merger_row['join_indicator'] in ('AB', 'B'):
                for change in self.changes[1].functions:
                    previous_row = change(merger_row, previous_row)
                self.writers[1].send(previous_row)
        return previous_row

    def step_3(self, merger_row, current_row=None):
        '''
        step 3
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[8].functions:
                    current_row = change(merger_row, current_row)
                self.writers[8].send(current_row)
        return current_row

    def step_4(self, previous_row, current_row=None):
        '''
        step 4
        '''
        if current_row:
            for change in self.changes[3].functions:
                previous_row = change(previous_row, current_row)
            self.writers[3].send(previous_row)
        return previous_row

    def step_5(self, merger_row, current_row=None):
        '''
        step 5
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[7].functions:
                    current_row = change(merger_row, current_row)
                self.writers[7].send(current_row)
        return current_row

    def step_6(self, merger_row, current_row=None):
        '''
        step 6
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[6].functions:
                    current_row = change(merger_row, current_row)
                self.writers[6].send(current_row)
        return current_row

    def step_7(self, merger_row, current_row=None):
        '''
        step 7
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[5].functions:
                    current_row = change(merger_row, current_row)
                self.writers[5].send(current_row)
        return current_row

    def step_8(self, merger_row, current_row):
        '''
        step 8
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[4].functions:
                    current_row = change(merger_row, current_row)
                self.writers[4].send(current_row)
        return current_row

    def step_9(self, merger_row, current_row):
        '''
        step 9
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[3].functions:
                    current_row = change(merger_row, current_row)
                self.writers[3].send(current_row)
        return current_row

    def step_10(self, merger_row, current_row=None):
        '''
        step 10
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[2].functions:
                    current_row = change(merger_row, current_row)
                self.writers[2].send(current_row)
        return current_row
    
    def write_all(self, previous_row):
        '''
        write directly if no logic is involved
        '''
        for step in self.writers:
            if step:
                step.send(previous_row)

    