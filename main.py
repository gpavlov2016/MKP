
#jobs = []
jobs =  {'A': set(['B', 'C']),
         'B': set(['A', 'D', 'E']),
         'C': set(['A', 'F']),
         'D': set(['B']),
         'E': set(['B', 'F']),
         'F': set(['C', 'E'])}

resources = []

with open('jobs.txt', 'r') as f:
    for line in f:
        words = line.strip().split(":")
        if words[1] == '':
            jobs.append([words[0], 0, 0, []])
#            jobs[words[0]] = ""
        elif words[0] == 'cores_required':
            jobs[-1][1] = words[1]
        elif words[0] == 'execution_time':
            jobs[-1][2] = words[1]
        elif words[0] == 'parent_tasks':
            tasks = words[1].replace('"', '').strip().split(",")
            tasks = [x.strip() for x in tasks]
            jobs[-1][3] = tasks
        else:
            print("Jobs file parsing error: " + ', '.join(words))
            exit()
f.closed

with open('resources.txt', 'r') as f:
    for line in f:
        words = line.strip().split(":")
        resources.append(words)
f.closed

print jobs
print resources

est = []

graph = {'A': set(['B', 'C']),
         'B': set(['A', 'D', 'E']),
         'C': set(['A', 'F']),
         'D': set(['B']),
         'E': set(['B', 'F']),
         'F': set(['C', 'E'])}

def dfs(graph, start):
    visited, stack = set(), [start]
    while stack:
        vertex = stack.pop()
        if vertex not in visited:
            visited.add(vertex)
            stack.extend(graph[vertex] - visited)
    return visited

print dfs(graph, 'A') # {'E', 'D', 'F', 'A', 'C', 'B'}
