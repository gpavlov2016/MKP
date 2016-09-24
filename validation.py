print "Validation"

#schedules is a list of schedule items
#each schedule item is a dict mapping resource --> set of tasks executed on it
#timeline is list of time values corresponding to each of the schedules
def validate(schedules, timeline, jobs, resources):

    if len(schedules) != len(timeline):
        print "Validate() error: schedule and timeline mismatch"

    # validate that all jobs are executed
    jobnames = set(jobs.keys())
    for i in range(schedules):
        schedule = schedules[i]
        jobnames -= set(schedule.values())
    if jobnames:
        print "Validation error, jobs not executed:" + str(jobnames)

#validate dependencies
#validate temporal contention