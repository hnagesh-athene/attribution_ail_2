"""
main file to generate ails based on admin system
"""
import argparse
import datetime
import os
import traceback
import json

from bin.transform import Transform
from bin.merger import merge
from bin.value import generate_ail
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
    parser.add_argument('-p', '--prev',
                        help='previous quarter file')
    parser.add_argument('-c', '--cur',
                        help='current quarter file')
    
    args = parser.parse_args()

    if not os.path.exists('data/output/' + args.valuation_date + '/' + args.block):
        os.makedirs('data/output/' + args.valuation_date + '/' + args.block)
    
    if not os.path.exists('data/intermediate/' + args.valuation_date + '/' + args.block):
        os.makedirs('data/intermediate/' + args.valuation_date + '/' + args.block)
    
    if not os.path.exists('logs/'+args.valuation_date+'/'+args.block):
        os.makedirs('logs/'+args.valuation_date+'/'+args.block)
    console_output = ConsoleLogOutput()
    run_log = FileLogOutput('logs/'+args.valuation_date+'/'+args.block+'/'+'step.run.log')
    failure_output = FileLogOutput('logs/'+args.valuation_date+'/'+args.block+'/'+'step.error.log')
    logger.add_output(console_output, 'error', 'critical')
    logger.add_output(failure_output, 'error', 'critical')
    logger.add_output(run_log, 'info')
    path = 'templates/attribution_ail.json'
    with open(path) as file:
            conf = json.load(file)
            
    try:
        merge(args, conf[args.block][0])
        print("Merging complete")
        generate_ail(args, conf[args.block][0], logger)
        Transform(args, conf[args.block][0])
    except Exception as e:
        logger.error(traceback.format_exc())



if __name__ == '__main__':
    st = datetime.datetime.now()
    main()
    print('total time-taken = ', datetime.datetime.now() - st)
