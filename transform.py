'''
calculate/transform the required fields depending on admin system
'''
import tqdm
#from core_utils.progress import sane_tqdm
#from core_utils.tabular import FastDictReader
#from core_utils.tabular import CSVDataIO
#from collections import OrderedDict
from core_utils.core_utils.tabular import FastDictReader
from core_utils.core_utils.tabular import CSVDataIO

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
        self.generate()

    def generate(self):
        '''
        read transform and create output files
        '''
        self.step_1()
        self.step_2()
        self.step_3()
        self.step_4()
        self.step_5()
        self.step_6()
        self.step_7()
        self.step_8()
        self.step_9()
        self.step_10()

    def transformer(self, input_1, input_2, output, step, *field):
        '''
        transform logic
        '''
        progress = tqdm.tqdm(mininterval=1, unit=' rows', desc='rows checked '+step)
        cur_row = next(input_1)
        prev_row = next(input_2)
        while cur_row:
            try:
                if prev_row['Company']+prev_row['PolNo'] < cur_row['Company']+cur_row['PolNo']:
                    prev_row = next(input_2)
                elif prev_row['Company']+prev_row['PolNo'] > cur_row['Company']+cur_row['PolNo']:
                    output.send(cur_row)
                    cur_row = next(input_1)
                else:
                    #operations
                    output.send(cur_row)
                    cur_row = next(input_1)
                    prev_row = next(input_2)
            except StopIteration:
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
        self.transformer(cur, prev, output, 'step-1')
        #self.close(cur,  prev)
        self.prev = self.cur
        self.cur = 'step_1.ail2'

    def step_2(self):
        '''
        step 2
        '''
        print('step - 2')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_2')
        self.transformer(cur, prev, output, 'step-2')
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = 'step_2.ail2'

    def step_3(self):
        '''
        step 3
        '''
        print('step - 3')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_3')
        self.transformer(cur, prev, output, 'step-3')
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = 'step_3.ail2'

    def step_4(self):
        '''
        step 4
        '''
        print('step - 4')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_4')
        self.transformer(cur, prev, output, 'step-4')
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = 'step_4.ail2'

    def step_5(self):
        '''
        step 5
        '''
        print('step - 5')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_5')
        self.transformer(cur, prev, output, 'step-5')
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = 'step_5.ail2'

    def step_6(self):
        '''
        step 6
        '''
        print('step - 6')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_6')
        self.transformer(cur, prev, output, 'step-6')
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = 'step_6.ail2'

    def step_7(self):
        '''
        step 7
        '''
        print('step - 7')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_7')
        self.transformer(cur, prev, output, 'step-7')
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = 'step_7.ail2'

    def step_8(self):
        '''
        step 8
        '''
        print('step - 8')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_8')
        self.transformer(cur, prev, output, 'step-8')
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = 'step_8.ail2'

    def step_9(self):
        '''
        step 9
        '''
        print('step - 9')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_9')
        self.transformer(cur, prev, output, 'step-9')
        #self.close(cur, prev)
        self.prev = self.cur
        self.cur = 'step_9.ail2'

    def step_10(self):
        '''
        step 10
        '''
        print('step - 10')
        cur, prev = self.reader(self.cur, self.prev)
        output = self.writer('step_10')
        self.transformer(cur, prev, output, 'step-10')
        #self.close(cur, prev)
