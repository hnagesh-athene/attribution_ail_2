'''
calculate/transform the required fields depending on admin system
'''
import tqdm
#from core_utils.progress import sane_tqdm
from core_utils.tabular import FastDictReader
from core_utils.tabular import CSVDataIO
#from collections import OrderedDict

class Transformer:
    '''
    use transformation based on adminsystem
    '''
    def __init__(self, args):
        '''
        initialize the class object
        '''
        self.args = args
        self.generate()

    def generate(self):
        '''
        read transform and create output files
        '''
        self.cur_ail, self.prev_ail = self.reader()
        self.output = self.writer()
        self.transformer()

    def transformer(self):
        '''
        transform logic
        '''
        progress = tqdm.tqdm(mininterval=1, unit=' rows', desc='rows checked')
        cur_row = next(self.cur_ail)
        prev_row = next(self.prev_ail)
        while cur_row:
            try:
                if prev_row['Company']+prev_row['PolNo'] < cur_row['Company']+cur_row['PolNo']:
                    prev_row = next(self.prev_ail)
                    continue
                elif prev_row['Company']+prev_row['PolNo'] > cur_row['Company']+cur_row['PolNo']:
                    self.output.send(cur_row)
                else:
                    self.output.send(cur_row)
                cur_row = next(self.cur_ail)
                prev_row = next(self.prev_ail)
            except StopIteration:
                break
            progress.update()
        try:
            while cur_row:
                self.output.send(cur_row)
                cur_row = next(self.cur_ail)
        except StopIteration:
            print('done')

    def reader(self):
        '''
        reader
        '''
        print('current read')
        new_file = open(self.args.current_path)
        obj1 = FastDictReader(new_file, delimiter='\t')
        self.fieldnames = obj1.fieldnames
        print('previous read')
        old_file = open(self.args.old_path)
        obj2 = FastDictReader(old_file, delimiter='\t')
        return obj1, obj2

    def writer(self):
        '''
        writer
        '''
        print('write')
        write = CSVDataIO(delimiter='\t')
        cols = self.fieldnames
        file = write.iterative_writer('test.ail', cols)
#        write.write_file('test.ail', cols, self.res.values())
        return file
