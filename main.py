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
    if not c > 0 or not tasks:
        return set()
    if len(tasks) == 1:
        task = next(iter(tasks))
        w = jobs[task]['cores']
        if w <= c:
            return tasks    #one task fits remaining capacity
        else:
            return set()    #one task is larger than remaining capacity
    for task in tasks:
        w = jobs[task]['cores'] #weight
        if w <= c:
            p1 = pack(c - w, tasks - set([task]), jobs).union([task])  #include current task
        else:
            p1 = set()
        p2 = pack(c, tasks - set([task]), jobs)                        #exclude current task
        p1sum = sum([jobs[x]['cores'] for x in p1])
        p2sum = sum([jobs[x]['cores'] for x in p2])
        if p1sum > p2sum:
            p.add(task)
    return p

#rset - set of resource names
def cost(rset):
    return sum([resources[x] for x in rset])

#bins - set of resource names
#tasks - set of task names
def choose_bins(bins, tasks):

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
        assignments = choose_bins(bins - set([rs]), tasks - p)      #include current bin
        #        b2[rs] = choose_bins(bins - set([rs]), tasks)          # exclude current bin
        if assignments == None:
            continue

        if p:
            assignments[rs] = p #add current bin
        cst = cost(assignments.keys())
        if cst < mincost:
            mincost = cst
            minrs = rs
            minassignments = assignments

    if mincost == sys.maxint:
        return None     #cant pack all resources into bins
    else:
        return minassignments  #return dictionary of bins selected mapped to their task set


def print_assignments(assignemtns):
    for rs in assignemtns.keys():
        print ":" + str(resources[rs]) + ": " + rs
        for task in assignemtns[rs]:
            print "\t:" + str(jobs[task]['cores']) + ": " + task

import time

print "packaging: "
start = time.time()
a = choose_bins(set(resources.keys()), set(jobs.keys()))
end = time.time()
print_assignments(a)
print 'choose_bins(): ' + str(1000*(end - start)) + 'ms'
