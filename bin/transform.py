'''
calculate/transform the required fields depending on admin system
'''
import csv
import datetime
import sys

import tqdm

sys.path.insert(0, './core_utils')
from core_utils.tabular import tsv_io
from core_utils.dates import shift_quarters
from afdm_attribution_ail.step1 import Step1
from afdm_attribution_ail.step2 import Step2
from afdm_attribution_ail.step3 import Step3
from afdm_attribution_ail.step5 import Step5
from afdm_attribution_ail.step6 import Step6
from afdm_attribution_ail.step7 import Step7
from afdm_attribution_ail.step8 import Step8
from afdm_attribution_ail.step9 import Step9
from afdm_attribution_ail.step10 import Step10
from afdm_attribution_ail.step_ob import OB


class Transform:
    '''
    generate step 1- 10 atttribution_ails
    '''

    def __init__(self, args, conf):
        '''
        initialize the class object
        '''
        self.args = args
        valdate = datetime.datetime.strptime(self.args.valuation_date, '%Y%m%d').date()
        self.previous = ''.join(str(shift_quarters(valdate, -1)).split('-'))
        self.merger = conf['intermediate_file'].format(dir = conf['dir'],
                                                       date = args.valuation_date, block = args.block)
        self.fieldnames = tsv_io.read_header(conf['dir']+'/input/'+args.valuation_date+'/'+args.block+'/'+args.cur)
        self.output_path = 'data/output/' + args.valuation_date + '/' + args.block + '/' + r'ail.{}.{}_{}.ail2'
        self.steps = self.steps_profile()
        self.changes = self.change_steps()
        self.merger_file = self.reader(self.merger)
        self.writers = self.generate()
        self.row = {fields: None for fields in self.fieldnames}
        self.transformer(self.merger_file, self.steps)

    def steps_profile(self):
        '''
        define the steps for different blocks
        '''
        print('steps_profile')
        if self.args.block in ('fia', 'voya_fia', 'Rocky.fia', 'jackson.fia'):
            return [self.step_1, self.step_2, self.step_10, self.step_9, self.step_8,
                    self.step_7, self.step_6, self.step_5, self.step_3]
        elif self.args.block == 'anx':
            return [self.step_1, self.step_2, self.step_10, self.step_9, self.step_8,
                    self.step_7, self.step_6, self.step_5, self.step_3]
        elif self.args.block == 'amp':
            return [self.step_1, self.step_2, self.step_10, self.step_9, self.step_8,
                    self.step_8, self.step_8, self.step_8, self.step_8]
        elif self.args.block in ('tda', 'voya_fa', 'Rocky.tda', 'jackson.tda'):
            return [self.step_1, self.step_2, self.OB, self.OB, self.OB, self.step_7,
                    self.step_6, self.step_6, self.step_3]
        elif self.args.block == 'ila':
            return [self.step_1, self.step_2, self.OB, self.OB, self.OB, self.OB,
                    self.OB, self.OB, self.OB]

    def change_steps(self):
        '''
        get the field names and functions for the respective steps
        '''
        print('steps_change')
        return [Step1(), Step2(), Step10(), Step9(), Step8(),
                Step7(), Step6(), Step5(), Step3(), OB()]

    def generate(self):
        '''
        read transform and create output files
        '''
        print('generate')
        return [self.writer('Step1'), self.writer('Step2'), self.writer('Step10'),
                self.writer('Step9'), self.writer('Step8'), self.writer('Step7'),
                self.writer('Step6'), self.writer('Step5'), self.writer('Step3')]

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
                for idx, func in enumerate(steps):
                    if idx != 4:
                        previous_row = func(merger_row, idx, previous_row)
                    if idx == 2:
                        temp = previous_row
                    if idx == 4:
                        previous_row = func(merger_row, idx, temp)
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
        obj1 = tsv_io.read_file(file_1)
        return obj1

    def writer(self, step):
        '''
        writer
        '''
        print('output write')
        cols = self.fieldnames
        if step in ('Step1', 'Step2'):
            file = tsv_io.iterative_writer(self.output_path.format(self.args.block, self.previous, step), cols)
            return file
        else:
            file = tsv_io.iterative_writer(self.output_path.format(self.args.block, self.args.valuation_date, step),
                                           cols)
            return file

    def step_1(self, merger_row, idx, previous_row=None):
        '''
        step 1
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'B'):
                for change in self.changes[0].functions:
                    previous_row = self.row
                    previous_row = change(merger_row, previous_row, self.fieldnames)
                self.writers[idx].send(previous_row)
        return previous_row

    def step_2(self, merger_row, idx, previous_row):
        '''
        step 2
        '''
        if merger_row['join_indicator'] == 'AB':
            for change in self.changes[1].functions:
                previous_row = change(merger_row, previous_row, self.fieldnames)
            self.writers[idx].send(previous_row)
        return previous_row

    def step_3(self, merger_row, idx, current_row=None):
        '''
        step 3
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[8].functions:
                    current_row = change(merger_row, current_row, self.fieldnames)
                self.writers[idx].send(current_row)
        return current_row

    def step_4(self, previous_row, idx, current_row=None):
        '''
        step 4
        '''
        if current_row:
            for change in self.changes[3].functions:
                previous_row = change(previous_row, current_row, self.fieldnames)
            self.writers[idx].send(previous_row)
        return previous_row

    def step_5(self, merger_row, idx, current_row=None):
        '''
        step 5
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[7].functions:
                    current_row = change(merger_row, current_row, self.fieldnames)
                self.writers[idx].send(current_row)
        return current_row

    def step_6(self, merger_row, idx, current_row=None):
        '''
        step 6
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[6].functions:
                    current_row = change(merger_row, current_row, self.fieldnames)
                self.writers[idx].send(current_row)
        return current_row

    def step_7(self, merger_row, idx, current_row=None):
        '''
        step 7
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[5].functions:
                    current_row = change(merger_row, current_row, self.fieldnames)
                self.writers[idx].send(current_row)
        return current_row

    def step_8(self, merger_row, idx, current_row):
        '''
        step 8
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[4].functions:
                    current_row = change(merger_row, current_row, self.fieldnames, self.args)
                self.writers[idx].send(current_row)
        return current_row

    def step_9(self, merger_row, idx, current_row):
        '''
        step 9
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[3].functions:
                    current_row = change(merger_row, current_row, self.fieldnames, self.args)
                self.writers[idx].send(current_row)
        return current_row

    def step_10(self, merger_row, idx, current_row=None):
        '''
        step 10
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[2].functions:
                    current_row = self.row
                    current_row = change(merger_row, current_row, self.fieldnames, self.args)
                self.writers[idx].send(current_row)
        return current_row

    def OB(self, merger_row, idx, current_row=None):
        '''
        step 10
        '''
        if merger_row:
            if merger_row['join_indicator'] in ('AB', 'A'):
                for change in self.changes[9].functions:
                    current_row = self.row
                    current_row = change(merger_row, current_row, self.fieldnames)
                self.writers[idx].send(current_row)
        return current_row

    def write_all(self, previous_row):
        '''
        write directly if no logic is involved
        '''
        for step in self.writers:
            if step:
                step.send(previous_row)
