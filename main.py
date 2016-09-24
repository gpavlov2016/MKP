import sys
#jobs = []
jobs =  {}
resources = {}

with open('jobs.txt', 'r') as f:
    for line in f:
        words = line.strip().split(":")
        if words[1] == '':
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
f.closed

with open('resources.txt', 'r') as f:
    for line in f:
        words = line.strip().split(":")
        resources[words[0]] = int(words[1])
f.closed


'''
def dfs(graph, start):
    visited, stack = set(), [start]
    while stack:
        vertex = stack.pop()
        if vertex not in visited:
            visited.add(vertex)
            if 'children' in graph[vertex]:
                stack.extend(graph[vertex]['children'] - visited)
    return visited
'''
visited = set()
for job in jobs:
    if job in visited:
        continue
    stack = [job]
    while stack:
        vertex = stack.pop()
        if 'parents' in jobs[vertex]:
            prnts_notvisited = [x for x in jobs[vertex]['parents'] if x not in visited]
            prnts_visited = [x for x in jobs[vertex]['parents'] if x in visited]
            if len(prnts_notvisited) == 0:
                jobs[vertex]['est'] = max([jobs[x]['est'] + jobs[x]['time'] for x in prnts_visited])
                visited.add(vertex)
            else:
                stack.append(vertex)
                stack += prnts_notvisited
        else:
            jobs[vertex]['est'] = 0
            visited.add(vertex)

#capacity - int capacity of the bin
#tasks - set of task names
def pack(capacity, tasks, jobs):
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
def choose_bins(bins, tasks, resources):

    if not tasks:
        return {}, tasks
    if len(bins) == 1:
        bin = next(iter(bins))
        c = resources[bin]   #bin capacity
        wsum = sum([jobs[x]['cores'] for x in tasks])
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
        assignments, leftovers = choose_bins(newbins, newtasks, resources)      #include current bin
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

def update_resources(schedule, cur_resources):
    for (k, v) in schedule.items():  # k is the resource name, v is a list of tasks scheduled on the resource
        for task in v:
            dur = jobs[task]['time']
            numcores = jobs[task]['cores']
            for core in cur_resources[k]:  # core is a dict {'job': "", 'endtime':0}
                if core['endtime'] <= curtime:
                    core['endtime'] = curtime + dur
                    numcores -= 1
                    #print "run job " + task + " on core " + str(core['id']) + " on resource " + k
                if numcores == 0:
                    break
            if numcores > 0:
                print "failed to schedule job " + task + " on resource " + k + ", cores missing: " + str(numcores)
                # TODO update EST of affected children
            else:
                scheduled_jobs.add(task)  # just for logging
                print task + ": " + k


'''
def print_assignments(assignemtns):
    for rs in assignemtns.keys():
        print ":" + str(resources[rs]) + ": " + rs
        for task in assignemtns[rs]:
            print "\t:" + str(jobs[task]['cores']) + ": " + task
'''

import time

'''
print "packaging: "
start = time.time()
a = choose_bins(set(resources.keys()), set(jobs.keys()))
end = time.time()
print_assignments(a)
print 'choose_bins(): ' + str(1000*(end - start)) + 'ms'
'''

total_cores = sum(resources.values())
print 'total_cores: ' + str(total_cores)

print "scheduling: "
start = time.time()

#sort jobs according to EST
sorted_jobs = sorted(jobs.items(), key=lambda x: x[1]['est'])
sorted_jobs = [k for (k,v) in sorted_jobs]
curtime = 0
rscnow = resources
for (k, v) in rscnow.items():
    rscnow[k] = [{'job': "", 'endtime': 0, 'id': x} for x in range(v)]  # each core has info about task and endtime

schedule_log = []
time_log = []
while sorted_jobs:
    #TODO - possible heuristic do not aggregate more jobs than avail resources
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

    while (ready_jobs):
        # build free resources dict
        free_resources = {}
        for (k, v) in rscnow.items():
            #core is a dict {'job': "", 'endtime':0}, v is a list one cell for each core
            free_cores = sum([core['endtime'] <= curtime for core in v])
            if free_cores > 0:
                free_resources[k] = free_cores
        if free_resources:
            #schedule max amount of jobs:
            schedule, leftovers = choose_bins(set(free_resources.keys()), ready_jobs, free_resources)
            if schedule:
                schedule_log.append(schedule)
                time_log.append(curtime)

            scheduled_jobs = set()
            if schedule == None:
                print "Error: unable to schedule at time: " + str(curtime)
            #update resources:
            update_resources(schedule, rscnow)
            ready_jobs = leftovers
        #advance time:
        curtime += 100
        print "Time: " + str(curtime)
        #TODO - possible heuristic predicting optimal increase in time


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
