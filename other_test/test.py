
import pandas as pd
import numpy as np

'''
-------------------------------------------------------------------------------
This file contains fucntions to test the execution of the program in
run_insight.py

See the file run_test.py in order to learn examples on how to run functions
in this file.
-------------------------------------------------------------------------------
'''


def read_result(foutput):
    '''
    Reads the outcome of the program "sessionization.txt"

    Input:
        foutput: string
    Output:
        result = dataframe
    '''
    result = pd.read_csv(foutput, header = None)
    result.rename(index=str,
        columns={0: "ip", 1: "first", 2: "last", 3:"duration", 4:"number"},
        inplace=True)

    result['first'] = pd.to_datetime(result['first'])
    result['last'] = pd.to_datetime(result['last'])

    return result


def read_inactivity_period(ipfile):
    '''
    Reads the file inactivitity period and returns an interger.

    Input:
        ipfile = string
    Output:
        inactivity_period = int
    '''
    with open(ipfile, 'r') as f:
      for line in f:
          inactivity_period = int(line)

    return inactivity_period


def test1(rgb, gb):
    '''
    Prints statements about the success or fail to contabilize, correctly,
    the total number of documents by ip.

    Inputs:
        rgb = dataframe
        gb = dataframe
    '''
    outcome_test = "Everything good."

    ips_random = rgb.sample(frac=0.2)
    verification = pd.merge(ips_random[["ip", "number"]], gb[["ip", "date"]],
        on = "ip")
    verificator = sum(verification["number"] == verification["date"])
    if len(verification) == verificator:
        print("Test 1 passed. Same total number of documents.")

    else:
        print("Test 1 fail. The number of total documents is incorrect.")
        outcome_test = "Review the code, please. It is not working."

    return outcome_test



def test2(df, result, inactivity_period):
    '''
    Assess if the number of sessions by ip is the correct.

    Inputs:
        df = dataframe
        result = dataframe
    '''
    outcome_test = "Everything good."

    gb_max = df.groupby(['ip'], as_index = False).max()[["ip", "date", "time"]]
    gb_min = df.groupby(['ip'], as_index = False).min()[["ip", "date", "time"]]
    gb_compare = pd.merge(gb_max, gb_min, on = "ip")
    mask_same_date = gb_compare["date_x"] == gb_compare["date_y"]
    gb_compare = gb_compare[mask_same_date]
    gb_compare["time_x"] = pd.to_datetime(gb_compare['time_x'],
        format='%H:%M:%S')
    gb_compare["time_y"] = pd.to_datetime(gb_compare['time_y'],
        format='%H:%M:%S')
    gb_compare["difference"] = (
        gb_compare["time_x"] - gb_compare["time_y"]).astype(
        'timedelta64[s]') + 1
    gb_compare = gb_compare[["ip", "difference"]]
    gb_compare["cicles"] = np.ceil(
        gb_compare.difference/inactivity_period).astype(int)

    # Filter the result df (only take the same days for easy comparison)
    result["first"] = result["first"].dt.date
    result["last"] = result["last"].dt.date
    mask_same_date_r = result["first"] == result["last"]
    result = result[mask_same_date_r]
    rgb_2 = result.groupby(['ip'], as_index=False).count()[["ip", "number"]]

    # Compare 1) the output of my program with 2) the analysis with groupby
    compare_sessions = pd.merge(gb_compare, rgb_2, on = "ip")
    mask_compare_sessions = sum(
        compare_sessions["cicles"] >= compare_sessions["number"])
    if len(compare_sessions) == mask_compare_sessions:
        print("Test 2 passed. The number of sessions is consistent.")
    else:
        print("Test 2 fail. The number of sessions by ip is incorrect.")
        outcome_test = "Review the code, please. It is not working."

    return outcome_test
