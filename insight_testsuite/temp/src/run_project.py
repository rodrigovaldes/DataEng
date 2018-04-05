
from project import *

'''
-------------------------------------------------------------------------------
This file runs the main program. Execute from the commanline like

python3 run_insight.py
-------------------------------------------------------------------------------
'''

fname = "input/log.csv"
char_division = ","
foutput = "output/sessionization.txt"
ipfile = 'input/inactivity_period.txt'

# Open inactivity_period.txt
inactivity_period = read_inactivity_period(ipfile)

process_file(fname, foutput, inactivity_period, char_division)
