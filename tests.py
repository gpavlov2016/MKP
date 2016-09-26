print "Tests"

max_cores = 10

import random

def gen_test_case(numjobs, numresources, max_cores, max_exec_time):
    jobstr = ""
    resourcestr = ""
    for i in range(numresources):
        numcores = random.randint(1, max_cores)
        resourcestr += "compute" + str(i) + ": " + str(numcores) + "\n"

    for i in range(numjobs):
        numcores = random.randint(1, max_cores)
        exec_time = random.randint(1, max_exec_time)

        jobstr += "task" + str(i) + ":\n"
        jobstr += "cores_required: " + str(numcores) + "\n"
        jobstr += "execution_time: " + str(exec_time) + "\n"

    return jobstr, resourcestr

def gen_dependencies(jobs, maxnumparents):
    jobnames = jobs.keys()

    for i in range(len(jobnames)):
        job = jobnames[i]
        jobs[job]['parents'] = set()
        for j in range(len(jobnames)):
            if i > j and random.randint(0,1):
                parent = jobnames[j]
                if len(jobs[job]['parents']) < maxnumparents:
                    jobs[job]['parents'].add(parent)


from main import *
from validation import *
from visualization import *
import time


def create_and_run_test(numjobs, numresources):
    good_input = False
    while not good_input:
        #generate test case
        print "Generating test case: "
        jobstr, resourcestr = gen_test_case(numjobs, numresources, 5, 100)

        print "Parsing input: "
        #jobs, resources = parse_input()
        jobs, resources = parse_input(jobstr, resourcestr)

        max_avail_cores = max(resources.values())
        max_required_cores = max(jobs[x]['cores'] for x in jobs.keys())
        if max_avail_cores >= max_required_cores:
            good_input = True

    gen_dependencies(jobs, 2)

    print "Jobs:"
    print jobs
    print "Resources:"
    print resources

    total_cores = sum(resources.values())
    print 'Total cores: ' + str(total_cores)

    print "Calculating EST: "
    calc_est(jobs)
    print "Scheduling: "
    start = time.time()
    schedule_log, time_log = schedule_jobs(jobs, resources)
    end = time.time()
    sched_duration = 1000*(end - start) #ms
    print 'Scheduling took: ' + "%.2f" % (sched_duration) + 'ms'

    start = time.time()
    validate(schedule_log, time_log, jobs, resources)
    end = time.time()
    print 'Validation took: ' + "%.2f" % (1000*(end - start)) + 'ms'

    start = time.time()
    visualize_schedules(schedule_log, time_log, jobs, resources)
    end = time.time()
    print 'Visualization took: ' + "%.2f" % (1000*(end - start)) + 'ms'

    #compute metrics
    return sched_duration

def mean(numbers):
    return float(sum(numbers)) / max(len(numbers), 1)

#random.seed(0)

maxjobs = 15
maxresources = 15
repetitions = 10
sched_dur_matrix = [[0.0 for i in range(maxresources+1)] for j in range(maxjobs+1)]
for numjobs in range(1, maxjobs+1):
    for numresources in range(1, maxresources+1):
        print 'Jobs: ' + str(numjobs) + ', Resources: ' + str(numresources)
        durlist = [create_and_run_test(numjobs, numresources) for x in range(repetitions)]
        sched_dur_matrix[numjobs][numresources] = mean(durlist)
#print shcedulng duration matrix
s = ""
for numresources in range(1, maxresources + 1):
    s += "|" + "%3d" % (numresources) + "|  "
print "#Jobs\#Resources  " + s
for numjobs in range(1, maxjobs+1):
    s = ""
    for numresources in range(1, maxresources+1):
        s += "%7.1f" % (sched_dur_matrix[numjobs][numresources])
    print "\t\t|" + "%3d" % (numjobs) + "|\t" + s

