print "Visualization"

def visualize_schedules(schedules, timeline, jobs, resources):
    if len(schedules) != len(timeline):
        print "visualize_schedules() error: schedule and timeline mismatch"
        return

    # for each resource build a list of tasks, each cell with start time and end time
    exec_dict = {}
    for i in range(len(schedules)):
        schedule = schedules[i]
        for (resource, tasks) in schedule.items():
            if not exec_dict.has_key(resource):
                exec_dict[resource] = []
            start = timeline[i]
            # TODO relax the following constraint to allow better packing:
            durations = [jobs[x]['time'] for x in tasks]
            maxduration = max(durations)
            end = start + maxduration
            exec_dict[resource].append([start, end])

    mkspan_dict = {}
    maxmkspan = 0
    # Calculate latest execution time for each resource
    for (resource, l) in exec_dict.items():
        # sort in ascending start time order
        l.sort
        mkspan_dict[resource] = l[-1][1]    #end  time of the last element
        maxmkspan = max(mkspan_dict[resource], maxmkspan)

    print "Makespan: " + str(maxmkspan)
    rowlen = 50
    scale = (1.0*rowlen)/maxmkspan
    for (resource, l) in exec_dict.items():
        row = [""] * rowlen
        for [start, end] in l:
            scaledstart = int(start*scale)
            scaledend = int(end*scale)
            row[scaledstart] = "|"
            row[scaledstart+1:scaledend] = "x"*(scaledend-scaledstart)
            row[scaledend] = "|"
        print "\t\t" + "%-10s" % (resource + ":") + ''.join(row)

    xaxis = ""
    axis_interval = 5
    for i in range(0, rowlen+axis_interval, axis_interval):
        xaxis += "%-5s" % str(int(i/scale))
    print "\t\t\t\t  " + xaxis
