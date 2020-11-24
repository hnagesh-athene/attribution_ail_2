"""
main file to generate ails based on admin system
"""
#from importlib import import_module
import sys
import argparse
import datetime
import os
from transform import Transform

sys.path.insert(0, './ail_mesh')
# sys.path.insert(0, './core_utils')
#sys.path.insert(0, './afdm_attribution_ail')

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
    parser.add_argument('-s', '--admin-system',
                        help='name of the source system')
    parser.add_argument('-c', '--current-path',
                        help='path to the current input AILs')
    parser.add_argument('-p', '--previous-path',
                        help='path to the old input AILs')
    parser.add_argument('-o', '--output',
                        help='path to the output AILs',
                        default='output/test.ail2')
    args = parser.parse_args()

    #source=import_module(args.admin_system)
    if not os.path.exists('output/'+args.valuation_date):
        os.mkdir('output/'+args.valuation_date)
    Transform(args)

if __name__ == '__main__':
    st = datetime.datetime.now()
    main()
    print('total time-taken = ', datetime.datetime.now()-st)
