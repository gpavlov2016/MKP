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

print jobs
print resources

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
print 'jobs:'
print jobs

total_cores = sum(resources.values())
print 'total_cores: ' + str(total_cores)

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
        return {}
    if len(bins) == 1:
        bin = next(iter(bins))
        c = resources[bin]   #bin capacity
        wsum = sum([jobs[x]['cores'] for x in tasks])
        if wsum == 0:
            return {}                #no tasks to pack, no bins selected
        elif wsum <= c:
            return {bin: tasks}  #packing all remaining resources to the only bin
        else:
            return None                 #not enough capacity in the only bin to pack all remaining resources

    mincost = sys.maxint
    minrs = None
    minassignments = {}
    #Find minimum cost (recursive):
    for rs in bins:
        p = pack(resources[rs], tasks, jobs)
        if not p:
            continue
        assignments = choose_bins(bins - set([rs]), tasks - p, resources)      #include current bin
        #TODO - relax the following requirement to accomaodate partial scheduling:
        if assignments == None:
            continue

        if p:
            assignments[rs] = p #add current bin

        cost = sum([resources[x] for x in assignments.keys()])
        if cost < mincost:
            mincost = cost
            minrs = rs
            minassignments = assignments

    if mincost == sys.maxint:
        return None     #cant pack all resources into bins
    else:
#        print minassignments
        return minassignments  #return dictionary of bins selected mapped to their task set


def print_assignments(assignemtns):
    for rs in assignemtns.keys():
        print ":" + str(resources[rs]) + ": " + rs
        for task in assignemtns[rs]:
            print "\t:" + str(jobs[task]['cores']) + ": " + task


import time

'''
print "packaging: "
start = time.time()
a = choose_bins(set(resources.keys()), set(jobs.keys()))
end = time.time()
print_assignments(a)
print 'choose_bins(): ' + str(1000*(end - start)) + 'ms'
'''

print "scheduling: "
start = time.time()

#sort jobs according to EST
sorted_jobs = sorted(jobs.items(), key=lambda x: x[1]['est'])
sorted_jobs = [k for (k,v) in sorted_jobs]
print sorted_jobs
curtime = 0
rscnow = resources
for (k, v) in rscnow.items():
    rscnow[k] = [{'job': "", 'endtime': 0, 'id': x} for x in range(v)]  # each core has info about task and endtime

schedule_log = []
time_log = []
while sorted_jobs:
    #build free resources dict
    free_resources = {}
    for (k, v) in rscnow.items():
        free_cores = sum([core['endtime'] <= curtime for core in v])  # core is a dict {'job': "", 'endtime':0}, v is a list one cell for each core
        if free_cores > 0:
            free_resources[k] = free_cores

    #TODO possible heuristic do not aggregate more jobs than avail resources
    ready_jobs = set()
    while sorted_jobs:
        job = sorted_jobs[0]
        if jobs[job]['est'] <= curtime:
            ready_jobs.add(job)
            sorted_jobs.pop(0)
        else:
            break


    #schedule all jobs:
    schedule = choose_bins(set(free_resources.keys()), ready_jobs, free_resources)
    new_schedule = schedule
    if schedule:
        schedule_log.append(schedule)
        time_log.append(curtime)

    scheduled_jobs = set()
    #update resources:
    for (k,v) in schedule.items(): #k is the resource name, v is a list of tasks scheduled on the resource
        for task in v:
            dur = jobs[task]['time']
            numcores = jobs[task]['cores']
            for core in rscnow[k]:  #core is a dict {'job': "", 'endtime':0}
                if core['endtime'] <= curtime:
                    core['endtime'] = curtime + dur
                    numcores -= 1
                    print "run job " + task + " on core " + str(core['id']) + " on resource " + k
                if numcores == 0:
                    break
            if numcores > 0:
                print "failed to schedule job " + task + " on resource " + k + ", cores missing: " + str(numcores)
                #TODO update EST of affected children
            else:
                scheduled_jobs.add(task)    #just for logging
                print "job " + task + " scheduled on resource " + k

    #advance time:
    curtime += 100
    print "time:" + str(curtime)

    #TODO possible heuristic predicting optimal increase in time

end = time.time()
print 'Scheduling took: ' + str(1000*(end - start)) + 'ms'

from validation import *

start = time.time()
validate(schedule_log, time_log, jobs, resources)
end = time.time()
print 'Validation took: ' + str(1000*(end - start)) + 'ms'

