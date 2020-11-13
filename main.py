"""
main file to generate ails based on admin system
"""

import sys
import argparse
import datetime

sys.path.insert(0, './ail_mesh')
sys.path.insert(0, './core_utils')
sys.path.insert(0, './afdm_attribution_ail')

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

    parser.add_argument('--valuation-date',
                        type=parse_timestamp,
                        help='date for which the AILs should be generated')
    parser.add_argument('-s', '--admin-system',
                        help='name of the source system')
    parser.add_argument('-c', '--current-path',
                        help='path to the current input AILs')
    parser.add_argument('-o', '--old-path',
                        help='path to the old input AILs')


    args = parser.parse_args()


        
if __name__ == '__main__':
        main()
