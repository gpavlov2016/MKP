print "Validation"

#schedules is a list of schedule items
#each schedule item is a dict mapping resource --> set of tasks executed on it
#timeline is list of time values corresponding to each of the schedules
def validate(schedules, timeline, jobs, resources):

    if len(schedules) != len(timeline):
        print "Validate() error: schedule and timeline mismatch"
        return

    # validate that all jobs are executed
    jobnames = set(jobs.keys())
    for i in range(len(schedules)):
        schedule = schedules[i]
        for tasks in schedule.values():
            jobnames -= tasks
    if jobnames:
        print "Validation error, jobs not executed:" + str(jobnames)

#for each resource build a list of tasks, each cell with start time and end time
    exec_dict = {}
    for i in range(len(schedules)):
        schedule = schedules[i]
        for (resource, tasks) in schedule.items():
            if not exec_dict.has_key(resource):
                exec_dict[resource] = []
            start = timeline[i]
            #TODO relax the following constraint to allow better packing:
            durations = [jobs[x]['time'] for x in tasks]
            maxduration = max(durations)
            end = start + maxduration
            exec_dict[resource].append([start, end])

    # for each resource make sure that there is no contention
    for (resource, l) in exec_dict.items():
        # sort in ascending start time order
        l.sort
        for i in range(1, len(l)):
            if l[i][0] < l[i-1][1]:     #current start < prev end
                print "Validation error - temporal contention, resouce: " + resource + " time: " + str(l[i])

#validate dependencies

print "Validation PASSED!"