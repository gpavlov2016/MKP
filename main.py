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

#schedule - dict of resource mapped to tasks
def update_resources(schedule, cores_availability, curtime):
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
                    core_avail_list[0] += dur
                    #scheduled_jobs.add(task)  # just for logging
                    print task + ": " + resource


def update_est(tasks, howlong):
    for task in tasks:
        jobs[task]['est'] += howlong
        update_est(jobs[task]['children'], howlong)

import time

total_cores = sum(resources.values())
print 'Total cores: ' + str(total_cores)

print "Scheduling: "
start = time.time()

#sort jobs according to EST
sorted_jobs = sorted(jobs.items(), key=lambda x: x[1]['est'])
sorted_jobs = [k for (k,v) in sorted_jobs]
curtime = 0

cores_availability = {}  # resource name --> list of availability times for each core
for (resource, numcores) in resources.items():
    cores_availability[resource] = [0 for x in range(numcores)] #resource name --> list of availability times for each core

schedule_log = []
time_log = []
while sorted_jobs:
    # sort jobs according to EST

    sorted_jobs = sorted(sorted_jobs, key=lambda x: jobs[x]['est'])

    #TODO - match resourcees and ready jobs
    ready_jobs = set()
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
        for (resource, core_avail_list) in cores_availability.items():
            #core is a dict {'job': "", 'endtime':0}, v is a list one cell for each core
            free_cores = sum([core_avail_list[i] <= curtime for i in range(len(core_avail_list))])
            if free_cores > 0:
                free_resources[resource] = free_cores
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
            update_resources(schedule, cores_availability, curtime)
            ready_jobs = leftovers
        flat_core_avail = [item for sublist in cores_availability.values() for item in sublist]
        flat_core_avail.sort()
        #advance time:
        prevtime = curtime
        for x in flat_core_avail:
            if x > curtime:
                curtime = x
                break
        update_est(leftovers, curtime - prevtime)
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
