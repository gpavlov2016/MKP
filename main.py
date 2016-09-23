import sys
#jobs = []
jobs =  {}
resources = []

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
        resources.append([words[0], int(words[1])])
f.closed

print jobs
print resources


def dfs(graph, start):
    visited, stack = set(), [start]
    while stack:
        vertex = stack.pop()
        if vertex not in visited:
            visited.add(vertex)
            if 'children' in graph[vertex]:
                stack.extend(graph[vertex]['children'] - visited)
    return visited

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

total_cores = sum([x[1] for x in resources])
print 'total_cores: ' + str(total_cores)

total