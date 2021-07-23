"""
main file to generate ails based on admin system
"""
import argparse
import datetime
import os
import traceback

from bin.transform import Transform
from bin.log_utils import logger
from core_utils.log import ConsoleLogOutput, FileLogOutput



def parse_timestamp(timestamp):
    """
    Load a standard representation date string timestamp.
    """
    if timestamp:
        return datetime.datetime.strptime(timestamp, '%Y%m%d').date()


def main():
    """
    Run Extract, Transform, Load, AIL, and CHF generation processes.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('-v', '--valuation-date',
                        help='date for which the AILs should be generated')
    parser.add_argument('-b', '--block',
                        help='name of the block')
    parser.add_argument('-m', '--merger-path',
                        help='path to the old input AILs')
    parser.add_argument('-o', '--output',
                        help='path to the output AILs',
                        default='output/test.ail2')
    parser.add_argument('-p', '--prev',
                        help='previous quarter file')
    args = parser.parse_args()

    if not os.path.exists('data/output/' + args.valuation_date + '/' + args.block):
        os.makedirs('data/output/' + args.valuation_date + '/' + args.block)
    
    if not os.path.exists('logs/'+args.valuation_date+'/'+args.block):
        os.makedirs('logs/'+args.valuation_date+'/'+args.block)
    console_output = ConsoleLogOutput()
    failure_output = FileLogOutput('logs/'+args.valuation_date+'/'+args.block+'/'+'step.error.log')
    logger.add_output(console_output, 'info', 'error', 'critical')
    logger.add_output(failure_output, 'error', 'critical')
    
    try:
        Transform(args)
    except Exception as e:
        logger.error(traceback.format_exc())



if __name__ == '__main__':
    st = datetime.datetime.now()
    main()
    print('total time-taken = ', datetime.datetime.now() - st)
