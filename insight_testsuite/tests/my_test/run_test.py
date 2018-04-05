
import pandas as pd
from test import *
import os
# os.chdir("/insight_testsuite/tests/my_test")

'''
-------------------------------------------------------------------------------
This file runs the test of the program. Execute from the commanline like

python3 run_test.py
-------------------------------------------------------------------------------
'''

fname = "input/log.csv"
char_division = ","
foutput = "output/sessionization.txt"
ipfile = 'input/inactivity_period.txt'

# Open inactivity_period.txt
inactivity_period = read_inactivity_period(ipfile)

# Original data
df = pd.read_csv(fname)
gb = df.groupby(['ip'], as_index = False).count()

# Result of the process
result = read_result(foutput)
rgb = result.groupby(['ip'], as_index=False).sum()

# Test the number of documents is the same, by ip
test1(rgb, gb)

# Test the number of sessions
test2(df, result, inactivity_period)
