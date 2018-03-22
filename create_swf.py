#!/usr/bin/env python


import commands
import argparse
import csv
from sys import exit
from os import system, path, remove
import string
from re import findall
from multiprocessing import Pool, cpu_count
#http://www.cs.huji.ac.il/labs/parallel/workload/swf.html

def file_exists(file_name):
    return path.exists(file_name)

def base_path(cur_path):
    BASE = path.basename(path.normpath(cur_path)).split(".")[0]
    nums =  findall(r'\d+', BASE)
    return nums[-1]

def call_command(command):
    return commands.getstatusoutput(command)[1].strip()

class CurateSacct:
    #Job_ID _       _       _        SubmitTime Ntasks WallClock    StartTime   EndTime     Status
    #   *   *   synthetic   fixed   4609            4   21865       4609        9111        completed
    def __init__(self, job_id):
        self.job_id = str(job_id)

    def synthetic(self):
        return "synthetic"
    def status(self):
        return "completed"
    def fixed(self):
        return "fixed"
    def asterix(self):
        return "*"
    def job_name(self):
        command='sacct -n --format=jobname%60 -j ' + self.job_id + '.batch'
        return call_command(command)
    def max_rss(self):
        command = 'sacct -n --format=maxrss -j '+ self.job_id + '.batch'
        return call_command(command).split("K")[0]
    def ntasks(self):
        command = 'sacct -n --format=ntasks -j '+ self.job_id + '.batch'
        return call_command(command)

    def time_in_sec(self, tstring, CHECK=False):
        if not CHECK:
            command = tstring.split("T")[1].split(":")
        else:
            command = tstring.split(":")
        t_sec   = float(command[0])*3600.0 + float(command[1])*60.0 + float(command[2])
        return str(int(t_sec))

    def start_time(self):
        command = 'sacct -n --format=start -j ' + self.job_id + '.batch'
        value   = call_command(command)
        return self.time_in_sec(value)
    def end_time(self):
        command = 'sacct -n --format=end -j '   + self.job_id + '.batch'
        value   = call_command(command)
        return self.time_in_sec(value)
    def submit_time(self):
        command = 'sacct -n --format=submit -j '+ self.job_id + '.batch'
        value   = call_command(command)
        return self.time_in_sec(value)
    def elapsed_time(self):
        command = 'sacct -n --format=elapsed -j ' + self.job_id + '.batch'
        value   = call_command(command)
        return self.time_in_sec(value, True)

class Generate_SWF:
    def __init__(self, JOB_NAME, MAX_RSS, NTASKS, START_TIME, END_TIME, SUBMIT_TIME, ELAPSED_TIME, TRACE, ASTERIX, FIXED, STATUS, SYNTHETIC):
        #basic parameters
        self.trace       = TRACE
        self.ntasks      = NTASKS
        self.max_rss     = MAX_RSS
        self.start_time  = START_TIME
        self.submit_time = SUBMIT_TIME
        self.wc_time     = ELAPSED_TIME
        self.end_time    = END_TIME
        self.delimiter   = '\t'
        self.index       = '*'
        self.basename    = '.swf'
        self.asterix     = ASTERIX
        self.fixed       = FIXED
        self.status      = STATUS
        self.synthetic   = SYNTHETIC

    def join_as_list(self):
        #header   = ['index',   'ntasks'    ,   'wc_time',     'start_time', 'end_time'   , 'submit_time'   , 'max_rss']
        to_write = list()
        header = list()
                 #      *                   *             synthetic         fixed   4609            4               21865       4609        9111        completed
        header    = ['index',           'asterix',   'synthetic',     'fixed', 'submit_time', 'ntasks',       'wc_time', 'start_time', 'end_time', 'status', 'max_rss']
        to_write  = [self.asterix, self.asterix,  self.synthetic, self.fixed, self.submit_time, self.ntasks, self.wc_time, self.start_time, self.end_time, self.status, self.max_rss]
        return header, to_write

    def write_to_file(self, this_job):
        with open(self.trace + self.basename, 'a+') as swf_file:
            for l in range(len(this_job)-1):
                swf_file.write(this_job[l] + self.delimiter)
            swf_file.write(this_job[-1] + '\n')

class Generate_Cprog:
    def __init__(self, header, trace_name):
        self.header    = header
        self.file_name = 'swf2trace_' + trace_name + '.c'

    def start_if(self, count):
        base_if = 'if(i==' + str(count) + ') {'
        return base_if

    def else_if(self, count):
        base_if = 'else if(i==' + str(count) + ') {'
        return base_if

    def end_if(self):
        return ' }'

    def job_arr(self):
        job_array = 'job_arr[idx]'
        return job_array

    def print_statement(self, base_string, value):
        to_print = ' printf("' + str(base_string) + r': %s\n", ' + str(value) + ');'
        return to_print

    def int_print_statement(self, base_string, value):
        to_print = ' printf("' + str(base_string) + r': %d\n", ' + str(value) + ');'
        return to_print

    def parse_header(self):
        in_file = list()
        for head in self.header:
            idx = header.index(head)
            if head == 'index':
                in_file.append(self.start_if(idx))
                ja = self.job_arr()
                ja = " " + ja + '.job_id = ++job_index;'
                in_file.append(ja)
                in_file.append(self.int_print_statement("Index is", "job_index"))
                in_file.append(self.end_if())
            elif head == 'ntasks':
                in_file.append(self.else_if(idx))
                ja = self.job_arr()
                in_file.append(self.print_statement("Ntasks/nodes","p"))
                ja = " " + ja + '.tasks = atoi(p);'
                in_file.append(ja)
                in_file.append(self.end_if())
            elif head == 'wc_time':
                in_file.append(self.else_if(idx))
                ja = self.job_arr()
                in_file.append(self.print_statement("Wallclock limit","p"))
                ja = " " + ja + '.wclimit = ceil((double)atoi(p)/60);'
                in_file.append(ja)
                in_file.append(self.end_if())
            elif head == 'start_time':
                in_file.append(self.else_if(idx))
                in_file.append(self.print_statement("Startime","p"))
                ja = " " + 'start_time = atoi(p);'
                in_file.append(ja)
                in_file.append(self.end_if())
            elif head == 'end_time':
                in_file.append(self.else_if(idx))
                in_file.append(self.print_statement("End time","p"))
                ja = self.job_arr()
                ja = " "  + ja + '.duration = atoi(p) - start_time;'
                in_file.append(ja)
                in_file.append(self.int_print_statement("Duration","job_arr[idx].duration"))
                in_file.append(self.end_if())
            elif head == 'submit_time':
                in_file.append(self.else_if(idx))
                in_file.append(self.print_statement("Submit time","p"))
                in_file.append(' if (first_arrival == 0) ')
                ja = self.job_arr()
                ja = " " + ja + '.submit = 100 + atoi(p) - first_arrival;'
                in_file.append('first_arrival = atoi(p);')
                in_file.append(ja)
                in_file.append(self.end_if())
            elif head == 'max_rss':
                in_file.append(self.else_if(idx))
                ja = self.job_arr()
                ja = " " + ja + '.maxrss = atoi(p);'
                in_file.append(ja)
                in_file.append(self.print_statement("max_rss","p"))
                in_file.append(self.end_if())
            else:
                print ('header %s not found', head)
        return in_file

    def write_to_file(self, in_file):
        file_to_write = 'PATH TO TEMPLATE'
        if not file_exists(file_to_write): exit('Ooops template file not found')
        if file_exists(self.file_name):
            print ("deleting old file: "  + self.file_name)
            remove(self.file_name)
        g = open(file_to_write)
        lines = g.readlines()
        with open(self.file_name, 'w') as f:
            for line in lines:
                if '//WRITE_HERE' in line:
                    for inp in in_file:
                        if "}" in inp:
                            f.write(''+ inp+'\n')
                        else:
                            f.write(''+ inp)
                else:
                    f.write(line)

def multiprocess_em(CJOB_ID):
    JOB_ID = base_path(CJOB_ID)
    CS           = CurateSacct(JOB_ID)
    JOB_NAME     = CS.job_name()
    MAX_RSS      = CS.max_rss()
    NTASKS       = CS.ntasks()
    START_TIME   = CS.start_time()
    END_TIME     = CS.end_time()
    SUBMIT_TIME  = CS.submit_time()
    ELAPSED_TIME = CS.elapsed_time()
    ASTERIX      = CS.asterix()
    FIXED        = CS.fixed()
    STATUS       = CS.status()
    SYNTHETIC     = CS.synthetic()
    swf = Generate_SWF(JOB_NAME, MAX_RSS, NTASKS, START_TIME, END_TIME, SUBMIT_TIME, ELAPSED_TIME, args.trace, ASTERIX, FIXED, STATUS, SYNTHETIC)
    header, this_job = swf.join_as_list()
    swf.write_to_file(this_job)
    return header

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--basepath'    , required=True, help='path for job_ids', type=str)
    parser.add_argument('--trace'       , required=True, help='trace name', type=str)
    parser.add_argument('--generate'    , help='generate c file', action='store_true')
    args = parser.parse_args()

    MAIN_PATH = args.basepath
    COMMAND   = 'grep "Final" ' + MAIN_PATH + '/*.out -l'
    JOB_IDS = list(commands.getstatusoutput(COMMAND)[1:])[0].split("\n")
    p = Pool(processes=cpu_count())
    header = p.map(multiprocess_em, JOB_IDS)
    #for CJOB_ID in JOB_IDS:
    #    multiprocess_em(CJOB_ID)
    header = header[0]
    if args.generate:
        binary_name = 'swf2trace_' + args.trace
        if file_exists(binary_name):
            print ("removing old binary")
            remove(binary_name)
        cprog    = Generate_Cprog(header, args.trace)
        in_file  = cprog.parse_header()
        cprog.write_to_file(in_file)
        compile_bin = 'gcc ' + binary_name + '.c -lm -o ' + binary_name
        system(compile_bin)
        print ("generating trace file")
        trace_f = './swf2trace_' + args.trace +  ' ' + args.trace + '.swf'
        system(trace_f)
        print ("copy *.c and binary to: sim path")
