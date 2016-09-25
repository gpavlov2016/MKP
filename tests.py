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

from main import *
import time

good_input = False
while not good_input:
    #generate test case
    print "Generating test case: "
    jobstr, resourcestr = gen_test_case(5, 5, 5, 100)

    print "Parsing input: "
    #jobs, resources = parse_input()
    jobs, resources = parse_input(jobstr, resourcestr)

    max_avail_cores = max(resources.values())
    max_required_cores = max(jobs[x]['cores'] for x in jobs.keys())
    if max_avail_cores >= max_required_cores:
        good_input = True

print jobs
print resources

total_cores = sum(resources.values())
print 'Total cores: ' + str(total_cores)

print "Calculating EST: "
calc_est(jobs)
print "Scheduling: "
start = time.time()
schedule_log, time_log = schedule_jobs(jobs, resources)
end = time.time()
print 'Scheduling took: ' + str(1000*(end - start)) + 'ms'

from validation import *
from visualization import *

start = time.time()
validate(schedule_log, time_log, jobs, resources)
end = time.time()
print 'Validation took: ' + str(1000*(end - start)) + 'ms'

start = time.time()
visualize_schedules(schedule_log, time_log, jobs, resources)
end = time.time()
print 'Visualization took: ' + str(1000*(end - start)) + 'ms'


#run scheduling and validation

#compute metrics