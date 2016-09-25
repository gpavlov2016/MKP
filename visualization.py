print "Visualization"

rowlen = 50


def visualize_item(row, start, end, makespan, task):
    scale = (1.0 * rowlen) / makespan

    scaledstart = int(start * scale)
    scaledend = min(rowlen-1, int(end * scale))
    row[scaledstart] = "|"
    row[scaledstart + 1:scaledend] = "." * (scaledend - scaledstart - 2)
    row[scaledstart + 1:min(scaledend, len(task))] = list(task)[:min(len(task), scaledend-scaledstart)]
    row[scaledend] = "|"


def visualize_schedules(schedules, timeline, jobs, resources):
    if len(schedules) != len(timeline):
        print "visualize_schedules() error: schedule and timeline mismatch"
        return

    #for each resource build a list of tasks, each cell with start time and end time
    exec_dict = {}
    for i in range(len(schedules)):
        schedule = schedules[i]
        for (resource, tasks) in schedule.items():
            if not exec_dict.has_key(resource):
                exec_dict[resource] = []
            start = timeline[i]
            #TODO relax the following constraint to allow better packing:
            for task in tasks:
                duration = jobs[task]['time']
                cores = jobs[task]['cores']
                end = start + duration
                exec_unit = {'start': start, 'end': end, 'cores': cores, 'task': task}
                exec_dict[resource].append(exec_unit)

    makespan = 0
    # Calculate latest execution time for each resource
    for (resource, dictlist) in exec_dict.items():
        # sort in ascending start time order
        maxtime = max(dictlist, key=lambda x: x['end'])['end'] #max end time for all the exec items in the list
        makespan = max(maxtime, makespan)

    # for each resource draw the execution line
    for (resource, exec_list) in exec_dict.items():
        # sort in ascending start time order
        sorted(exec_list, key=lambda x: x['start'])
        numcores = resources[resource]
        cores_availability = [[0, x] for x in range(numcores)]   #list of times at which each core is available
        cores_vis = [[" "] * rowlen for x in range(numcores)]
        for exec_item_idx in range(0, len(exec_list)):
            cores_required = exec_list[exec_item_idx]['cores']
            for core_idx in range(cores_required):
                cores_availability.sort()
                next_avail = cores_availability[0][0]
                if next_avail > exec_list[exec_item_idx]['start']:
                    print "Visualization error - temporal contention, resouce: " + resource + " time: " + str(exec_list[exec_item_idx])
                else:
                    row = cores_vis[cores_availability[0][1]]
                    start = exec_list[exec_item_idx]['start']
                    end = exec_list[exec_item_idx]['end']
                    task = exec_list[exec_item_idx]['task']
                    visualize_item(row, start, end, makespan, task)
                    cores_availability[0][0] = exec_list[exec_item_idx]['end']
        print "\t\t" + "%-10s" % (resource + ":")
        for i in range(numcores):
            print "\t\t\t" + "core%-4s" % (str(i) + ":") + ''.join(cores_vis[i])

    xaxis = ""
    axis_interval = 5
    scale = (1.0 * rowlen) / makespan
    for i in range(0, rowlen+axis_interval, axis_interval):
        xaxis += "%-5s" % str(int(i/scale))
    print "\t\t\t\t    " + xaxis

    print "Makespan: " + str(makespan)
