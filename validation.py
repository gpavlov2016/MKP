print "Validation"

#schedules is a list of schedule items
#each schedule item is a dict mapping resource --> set of tasks executed on it
#timeline is list of time values corresponding to each of the schedules
def validate(schedules, timeline, jobs, resources):

    if len(schedules) != len(timeline):
        print "Validate() error: schedule and timeline mismatch"

    # validate that all jobs are executed
    jobnames = set(jobs.keys())
    for i in range(len(schedules)):
        schedule = schedules[i]
        for tasks in schedule.values():
            jobnames -= tasks
    if jobnames:
        print "Validation error, jobs not executed:" + str(jobnames)
'''
#validate dependencies
#for each resource build a list of tasks, each cell with start time and end time
    exec_dict = {}
    for i in range(len(schedules)):
        schedule = schedules[i]
        for (resource, tasks) in schedule.items():
            if not exec_dict.has_key(resource):
                exec_dict[resource] = []
            start = timeline[i]
            #TODO relax the following constraint to allow better packing:
            maxduration =
            end = start +

            exec_dict[resource].append([])
'''
#sort according to start time
#for each resource make sure that there is no contention


#validate temporal contention

print "Validation PASSED!"