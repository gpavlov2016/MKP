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
            for task in tasks:
                duration = jobs[task]['time']
                cores = jobs[task]['cores']
                end = start + duration
                exec_unit = {'start': start, 'end': end, 'cores': cores}
                exec_dict[resource].append(exec_unit)

    # for each resource make sure that there is no contention
    for (resource, exec_list) in exec_dict.items():
        # sort in ascending start time order
        sorted(exec_list, key=lambda x: x['start'])
        numcores = resources[resource]
        cores_availability = [0 for x in range(numcores)]   #list of times at which each core is available
        for exec_item_idx in range(0, len(exec_list)):
            cores_required = exec_list[exec_item_idx]['cores']
            for core_idx in range(cores_required):
                cores_availability.sort()
                next_avail = cores_availability[0]
                if next_avail > exec_list[exec_item_idx]['start']:
                    print "Validation error - temporal contention, resouce: " + resource + " time: " + str(exec_list[exec_item_idx])
                else:
                    cores_availability[0] = exec_list[exec_item_idx]['end']



#validate dependencies

print "Validation PASSED!"