
from itertools import permutations
import numpy as np
import linecache
import datetime
from subprocess import check_output
import os


'''
-------------------------------------------------------------------------------
This file contains the functions to execute the main program in
run_insight.py
-------------------------------------------------------------------------------
'''

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


def get_index_numbers(data_columns, reference_columns):
    '''
    Get the index of the columns needed for the analyis.
    Then, the process do not depends on the order of the columns

    Inputs:
        data_columns = list of the name of the columns in the header of a file
        reference_columns = list of the reference order in the header
    Outpus:
        output_index = list of the indexes where the information we need is
            available.
    '''

    output_index = []
    output_items = []
    for i, item in enumerate(data_columns):
        if item in reference_columns:
            output_index.append(i)
            output_items.append(item)

    # Detect problems
    if len(output_index) < len(reference_columns):
        print("The data in the file does not have all the information.")
        output_index = []

    if len(set(output_items)) != len(output_items):
        print('''Some column is in the data more than once.
            We will use only the first one''')

        new_output_index = []
        control = []
        for j, index in enumerate(output_index):
            if output_items[j] not in control:
                new_output_index.append(j)

        output_index = new_output_index

    return output_index


def save_one_line(dict_active, ip, foutput, char_division):
    '''
    Save the infomation of of an specific ip to the output fileself.

    Inputs:
        dict_active = dictionary
        ip = string
        foutput = string
    Output:
        dict_active = dictionary
    '''

    string_output = (ip + char_division
        + str(dict_active[ip]["first"]) + char_division
        + str(dict_active[ip]["last"]) + char_division
        + str(dict_active[ip]["duration"]) + char_division
        + str(dict_active[ip]["number"]) + "\n")

    f = open(foutput,'a')
    f.write(string_output)
    f.close()

    return 0


def actualize_data(dict_active, ip, date_time, sum_number = 0):
    '''
    Actualizes the dictionary regarding last time, number of consultations,
    and duration.

    Inputs:
        dict_active = dictionary
        ip = string
        date_time = datetime object
        sum_number = integer
    Output:
        dict_active = dictionary
    '''

    dict_active[ip]["last"] = date_time
    dict_active[ip]["number"] = dict_active[ip]["number"] + sum_number

    duration = dict_active[ip]["last"] - dict_active[ip]["first"]
    duration = int(duration.total_seconds() + 1)
    dict_active[ip]["duration"] = duration

    return dict_active


def save_lines_in_order(dict_active, inactivity_period, foutput, CURRENT_TIME,
    char_division, intermediate_process = True):
    '''
    Saves all sessions that finished, in order according to the first
    appereance of the ip.

    Inputs:
        dict_active = dictionary
        inactivity_period = integer
        foutput = string
        CURRENT_TIME = datetime object
        char_division = character
        intermediate_process = boolean
    Output:
        dict_active = dictionary
    '''

    # Detect all those to save
    keys_save = []
    order_save = []
    for k, v in dict_active.items():
        if intermediate_process:
            if ((CURRENT_TIME - v["first"]).total_seconds() + 1
                ) > inactivity_period:
                keys_save.append(k)
                order_save.append(v["order"])
        else:
            keys_save.append(k)
            order_save.append(v["order"])

    # Order those to save:
    order_save_index = sorted(
        range(len(order_save)), key=lambda k: order_save[k])

    # Save according to the defined order
    for num in order_save_index:
        save_one_line(dict_active, keys_save[num], foutput, char_division)
        dict_active.pop(keys_save[num])

    return dict_active


def process_file(fname, foutput, inactivity_period, char_division):
    '''
    This is the main function of the program. Computes the processes to
    generate the file sessionization.txt. That is to say, it creates a file
    with the following columns: ip, datetime first request, datetime last
    request, duration, # requests

    Inputs:
        fname = string
        foutput = string
        inactivity_period = integer
        char_division = string
    Outputs:
        It saves the file sessionization.txt
    '''

    # Obtain headline
    head = linecache.getline(fname, 1).strip().split(char_division)
    reference_cols = ["ip", "date", "time", "zone", "cik",
        "accession", "extention", "code"]
    index_information = get_index_numbers(head, reference_cols)
    dic_information = dict(zip(reference_cols, index_information))

    num_lines = int(check_output(["wc", "-l", fname]).split()[0])

    dict_active = {}
    for i in range(2, num_lines + 1):
        if i == 2:
            print("You initiated the program. You are processing the line: ", i)
        else:
            if i % 5000 == 0:
                print("You are processing the line: ", i)

        if i == 2:
            try:
                os.remove(foutput)
            except:
                pass

        row = linecache.getline(fname, i).strip().split(char_division)
        ip = row[dic_information["ip"]]
        date_str = row[dic_information["date"]].split("-")
        date = datetime.date(
            int(date_str[0]), int(date_str[1]), int(date_str[2]))
        time_ = datetime.datetime.strptime(
            row[dic_information["time"]],"%H:%M:%S").time()
        date_time = datetime.datetime.combine(date, time_)

        CURRENT_TIME = date_time

        if ip not in dict_active.keys():
            dict_active[ip] = {"first": date_time , "last": date_time ,
                "duration": 1 , "number": 1, "order": i}
        else:

            dict_active = actualize_data(dict_active, ip, date_time, 1)

        dic_active = save_lines_in_order(dict_active, inactivity_period,
            foutput, CURRENT_TIME, char_division)

    # If after finish the loop there is something in the dictionary,
    # all that infomation is saved in the output file

    if len(dict_active.keys()) > 0:

        dict_active = save_lines_in_order(dict_active, inactivity_period,
            foutput, CURRENT_TIME, char_division, False)

    print("Data saved.")

    return 0
