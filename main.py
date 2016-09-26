import sys


def parse_input(jobstr = None, resourcestr = None):
    jobs =  {}
    resources = {}

    if jobstr == None:
        filename = 'jobs.txt'
        f = open(filename, 'r')
        if not f:
            print "Error oppening file: " + filename
            return jobs, resources
    else:
        f = jobstr.split("\n")

    for line in f:
        words = line.strip().split(":")
        if not words or words == ['']:
            continue    #empty line

        if len(words) == 1 or words[1] == '':
            job = words[0]
            jobs[job] = {'children': set(), 'est': 0, 'lst': 0}
        elif words[0] == 'cores_required':
            jobs[job]['cores'] = int(words[1])
        elif words[0] == 'execution_time':
            jobs[job]['time'] = int(words[1])
        elif words[0] == 'parent_tasks':
            tasks = words[1].replace('"', '').strip().split(",")
            tasks = [x.strip() for x in tasks]
            jobs[job]['parents'] = set(tasks)
            for task in tasks:
                jobs[task]['children'].add(job)
        else:
            print("Jobs file parsing error: " + ', '.join(words))
            exit()
    if jobstr == None:
        f.closed

    if jobstr == None:
        filename = 'resources.txt'
        f = open(filename, 'r')
        if not f:
            print "Error oppening file: " + filename
            return jobs, resources
    else:
        f = resourcestr.split("\n")

    for line in f:
        words = line.strip().split(":")
        if not words or len(words) < 2:
            continue    #empty line
        resources[words[0]] = int(words[1])

    if resourcestr == None:
        f.closed

    return jobs, resources

#update the est field in each job dictionary value
def calc_est(jobs):
    visited = set()
    for job in jobs:
        if job in visited:
            continue
        stack = [job]
        while stack:
            vertex = stack.pop()
            if 'parents' in jobs[vertex] and jobs[vertex]['parents']:
                prnts_notvisited = [x for x in jobs[vertex]['parents'] if x not in visited]
                prnts_visited = [x for x in jobs[vertex]['parents'] if x in visited]
                if len(prnts_notvisited) == 0:
                    jobs[vertex]['est'] = max([jobs[x]['est'] + jobs[x]['time'] for x in prnts_visited])
                    visited.add(vertex)
                else:
                    if vertex in stack:
                        print "EST calculation error: circular dependencies detected: " + str(stack)
                        exit(1)
                    else:
                        stack.append(vertex)
                        stack += prnts_notvisited
            else:
                jobs[vertex]['est'] = 0
                visited.add(vertex)

def knapsack(capacity, tasks, jobs):
    tasklist = list(tasks)
    w = [jobs[task]['cores'] for task in tasklist]
    m = [[0 for i in range(capacity+1)] for j in range(len(tasklist)+1)]
    for j in range(capacity+1):
        m[0][j] = 0

    #populate the matrix
    for i in range(1, len(tasklist)+1):
        for j in range(capacity+1):
            if w[i-1] > j:
                m[i][j] = m[i-1][j]
            else:
                m[i][j] = max(m[i-1][j], m[i-1][j-w[i-1]] + w[i-1])

    #reconstruct results (tasks names)
    taskset = set()
    i = len(tasklist)
    maxw = max(w)
    while i > 0:
        if m[i][j] != m[i - 1][j]:
            taskset.add(tasklist[i - 1])
            j -= w[i - 1]
        i -= 1
    return taskset

#capacity - int capacity of the bin
#tasks - set of task names
def pack(capacity, tasks, jobs):

#    return knapsack(capacity, tasks, jobs)

    c = capacity
    p = set() #pack assignment
    if not tasks or not c:
        return set()    #no tasks to pack
    if len(tasks) == 1:
        task = next(iter(tasks))
        w = jobs[task]['cores']
        if w <= c:
            return tasks    #one task fits remaining capacity
        else:
            return set()    #one task is larger than remaining capacity
    maxw = 0
    maxtask = ""
    maxpack = set()
    for task in tasks:
        w = jobs[task]['cores'] #weight
        if w > c:
            continue
        p1 = pack(c - w, tasks - set([task]), jobs) #include current task
        p1.add(task)
        #p2 = pack(c, tasks - set([task]), jobs)                        #exclude current task
        p1sum = sum([jobs[x]['cores'] for x in p1])
        #p2sum = sum([jobs[x]['cores'] for x in p2])
        if p1sum > maxw:
            maxw = p1sum
            maxtask = task
            maxpack = p1

    return maxpack

#bins - set of resource names
#tasks - set of task names
#resources - dict of resoure name -> available cores
def choose_bins(bins, tasks, jobs, resources):

    if not tasks:
        return {}, tasks
    if len(bins) == 1:
        bin = next(iter(bins))
        c = resources[bin]   #bin capacity
        p = pack(c, tasks, jobs)
        if not p:
            return {}, tasks
        else:
            return {bin: p}, tasks - p  #pack the max number of tasks in the only bin and calculate the rest

    minnonpacked = sys.maxint
    minrs = None
    minassignments = {}
    #Find minimum cost (recursive):
    for rs in bins:
        p = pack(resources[rs], tasks, jobs)
        if not p:
            continue
        newbins = bins - set([rs])
        newtasks = tasks - p
        assignments, leftovers = choose_bins(newbins, newtasks, jobs, resources)      #include current bin
        #TODO - there are 2 criteria that we can optimize for:
        #       - minimum resources used#        cost = sum([resources[x] for x in assignments.keys()])
        #       - maximum tasks packed
        if p:
            assignments[rs] = p #add current bin
        nonpackedsum = sum([jobs[x]['cores'] for x in leftovers])
        if nonpackedsum < minnonpacked:
            minnonpacked = nonpackedsum
            minrs = rs
            minassignments = assignments

    packedjobs = set()
    for taskset in minassignments.values():
        packedjobs = packedjobs.union(taskset)

    return minassignments, tasks - packedjobs  #return dictionary of bins selected mapped to their task set

#schedule - dict of resource mapped to tasks
def update_resources(schedule, cores_availability, curtime, jobs):
    for (resource, tasks) in schedule.items():
        for task in tasks:
            dur = jobs[task]['time']
            numcores = jobs[task]['cores']
            for core in range(numcores):
                core_avail_list = cores_availability[resource]
                core_avail_list.sort()
                if core_avail_list[0] > curtime:
                    print "Update resource failed, all cores are busy: task: " \
                          + task + ", resource: " + resource + ", time: " + str(curtime)
                    break
                else:
                    core_avail_list[0] = curtime + dur
            #scheduled_jobs.add(task)  # just for logging
            print task + ": " + resource


def update_est(tasks, howlong, jobs):
    for task in tasks:
        jobs[task]['est'] += howlong
        update_est(jobs[task]['children'], howlong, jobs)

def schedule_jobs(jobs, resources):
    #sort jobs according to EST
    sorted_jobs = sorted(jobs.items(), key=lambda x: x[1]['est'])
    sorted_jobs = [k for (k,v) in sorted_jobs]

    cores_availability = {}  # resource name --> list of availability times for each core
    for (resource, numcores) in resources.items():
        cores_availability[resource] = [0 for x in range(numcores)] #resource name --> list of availability times for each core

    schedule_log = []
    time_log = []
    curtime = 0
    print "Time: " + str(curtime)
    while sorted_jobs:
        # sort jobs according to EST
        sorted_jobs = sorted(sorted_jobs, key=lambda x: jobs[x]['est'])

        #TODO - match resourcees and ready jobs
        ready_jobs = set()
        next_est = curtime
        while sorted_jobs:
            job = sorted_jobs[0]
            if jobs[job]['est'] <= curtime:
                ready_jobs.add(job)
                sorted_jobs.pop(0)
            else:
                next_est = jobs[job]['est']
                break

        while ready_jobs:
            # build free resources dict
            free_resources = {}
            for (resource, core_avail_list) in cores_availability.items():
                #core is a dict {'job': "", 'endtime':0}, v is a list one cell for each core
                free_cores = sum([core_avail_list[i] <= curtime for i in range(len(core_avail_list))])
                if free_cores > 0:
                    free_resources[resource] = free_cores
            leftovers = set()
            #Schedule available jobs to free resources
            if free_resources:
                #schedule max amount of jobs:
                schedule, leftovers = choose_bins(set(free_resources.keys()), ready_jobs, jobs, free_resources)
                if schedule:
                    schedule_log.append(schedule)
                    time_log.append(curtime)
                #update resources:
                update_resources(schedule, cores_availability, curtime, jobs)
                ready_jobs = leftovers
            flat_core_avail = [item for sublist in cores_availability.values() for item in sublist]
            flat_core_avail.sort()
            #advance time:
            prevtime = curtime
            for x in flat_core_avail:
                if x > curtime:
                    curtime = x
                    print "Time: " + str(curtime)
                    break
            if leftovers:
                update_est(leftovers, curtime - prevtime, jobs)
            #TODO - possible heuristic predicting optimal increase in time

        #update_est(sorted_jobs, next_est - prevtime, jobs)
        curtime = next_est
        print "Time: " + str(curtime)
    return schedule_log, time_log


from main import *
from validation import *
from visualization import *
import time

if sys.argv[0].find('main.py') != -1:
    print "Parsing input: "
    jobs, resources = parse_input()
    #jobs, resources = parse_input(jobstr, resourcestr)

    max_avail_cores = max(resources.values())
    max_required_cores = max(jobs[x]['cores'] for x in jobs.keys())
    if max_avail_cores < max_required_cores:
        print "Job to big to fit on any resource: max available cores: " + \
              max_avail_cores + ", required cores: " + max_required_cores

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

