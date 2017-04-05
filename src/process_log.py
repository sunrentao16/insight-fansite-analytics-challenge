# insight fansite analytics challenge
# Author: Rentao Sun

# Implemented a get_info module that contains functions extract infomation from lines
import get_info

# read data -------------------------------------
import os 
os.chdir(os.path.abspath('.')) # set working dictionary
log = open("log_input/log.txt","r")
data=log.readlines() # raw data
    
# feature 1 ----------------------------------
def find_active_host( data ):
#    from collections import Counter # use Counter to sort and find top active hosts
    num_top=10 # set the number of top active hosts
    # compute occurrences 
    counts=dict() # use dictionary data structure; key is host; value is num of visits
    for line in data:
        key = get_info.get_host(line)
        counts[key] = counts.get(key,0) + 1
    # find most active host
#    active_host = Counter(counts)
#    for host, v in active_host.most_common(num_top):
    active_host = dict()
    for host, v in counts.iteritems():
        if len(active_host) < num_top:
            active_host[host] = v
        elif v > min(active_host.values()):
            # replace the min in active_host by new host
            # this part can be more efficient if use min heap.
            active_host.pop(min(active_host, key = active_host.get))
            active_host[host] = v
    # document most active hosts
    text_file = open("log_output/hosts.txt",'w')
    for host in sorted(active_host, key=active_host.get, reverse=True):
        text_file.write( "%s,%d\n" % (host, active_host[host]))
    text_file.close()

# feature 2--------------------------------------
def find_popular_resource( data ):
#    from collections import Counter # use Counter to find top popular resource
    num_top=10 # set the number of most popular resource
    # compute occurrences 
    bandwidth=dict() # use dictionary data structure
    for line in data:
        key=get_info.get_resource(line)
        bandwidth[key] = bandwidth.get(key,0) + get_info.get_byte(line)
        
    # find most popular resource
#    popular_resource = Counter(bandwidth)
    popular_resource = dict()
    for resource, v in bandwidth.iteritems():
        if len(popular_resource) < num_top:
            popular_resource[resource] = v
        elif v > min(popular_resource.values()):
            popular_resource.pop(min(popular_resource, key = popular_resource.get))
            popular_resource[resource] = v

    # document most popular resource
    text_file = open("log_output/resources.txt",'w')
    for re in sorted(popular_resource, key = popular_resource.get, reverse=True):
        text_file.write( "%s\n" % (re))
    text_file.close()  

# feature 3-----------------------------------------
def find_busiest_time( data ):
    import datetime
    num_top = 10 # number of busiest periods 
    time_delta = datetime.timedelta( minutes=60 ) # length of period considered; 60 min in this case
    result = dict() # contains 10 busiest periods and number of visits
    start_time = get_info.get_time(get_info.get_raw_time(data[0])) # starting time of sliding 1 hour window
    end_time_of_data = get_info.get_time(get_info.get_raw_time(data[len(data)-1])) # end time of data
    queue_start = 0  # index of front of queue in the sliding time window
    queue_end = 0 # index of end of queue in the sliding time window
    while len(result) < num_top or start_time < end_time_of_data - time_delta :
        # enqueue new lines after time window shift
        if queue_end < len(data) - 1 : # if queue_end does Not reach the end of data
            while get_info.get_time(get_info.get_raw_time(data[queue_end])) <= (start_time + time_delta):
                queue_end +=1
                if queue_end >= len(data)-1:
                    break
        if queue_end == len(data)-1:
            # once reached end of data, queue_end always stay in sliding window
            queue_end = len(data) # set queueu_end to this number for convenience of calculating the size of queue
        
        # dequeue the front until it is in the sliding time window
        while get_info.get_time(get_info.get_raw_time(data[queue_start])) < start_time:
            queue_start += 1

        # store busiest period into result
        if len(result) < num_top:
            result[start_time] = queue_end - queue_start
        elif (queue_end - queue_start) > min(result.values()):
            # replace min of result by new busy period
            result.pop(min(result, key = result.get))
            result[start_time] = queue_end - queue_start
        # shift the time window 1 second
        start_time += datetime.timedelta(seconds=1)
        
    # sort result and write result in text file
    text_file = open("log_output/hours.txt",'w')
    for key, v in sorted(result.iteritems(),  key=lambda(k, v): (-v, k)):
        time = key.strftime('%d/%b/%Y:%H:%M:%S -0400') 
        text_file.write("%s,%d\n" % (time, v))
    text_file.close()

# feature 4 -----------------------------------------
def find_blocked(data):
    import Queue
    import datetime
    blocked_host = dict() # contains blocked host; value is beginning time of blocked
    failed_login = dict() # contains all failed login for 20 seconds; value is a queue containing failed login
    time_blocked = datetime.timedelta(minutes=5) # length of time blocked
    time_window = datetime.timedelta(seconds=20) # length of slideing window
    num_of_fails = 3 # number of failed login
    text_file = open("log_output/blocked.txt", 'w') # open log text file
    
    for line in data:
        time_of_line = get_info.get_time(get_info.get_raw_time(line)) # get the time of current line
        
        # found in blocked_host, then document it
        if get_info.get_host(line) in blocked_host :
            # still in 5 mins blocked period
            if time_of_line - blocked_host[get_info.get_host(line)] <= time_blocked :
                text_file.write("%s" % line) # document blocked login
            # blocked time ends
            elif get_info.get_reply(line) != '401' :
                blocked_host.pop(get_info.get_host(line)) # delete host since blocked time ends
            elif get_info.get_reply(line) == '401' : # detect a failed login
                blocked_host.pop(get_info.get_host(line)) # delete host since blocked time ends
                q= Queue.Queue(0) # initialize queue
                q.put(line) # enqueue failed login
                failed_login[get_info.get_host(line)]= q # input queue into failed_login

        # not found in blocked_host
        elif get_info.get_reply(line) == '401': # login failed
            if get_info.get_host(line) not in failed_login: # current host is NOT in failed_login
                q = Queue.Queue(0) # initialize queue
                q.put(line) # enqueue failed login
                failed_login[get_info.get_host(line)]= q # input queue into failed_login
            else:
                current_q = failed_login[get_info.get_host(line)] # current host is already in failed_login
                if time_of_line - get_info.get_time(get_info.get_raw_time(current_q.queue[0])) <= time_window :
                    if current_q.qsize() >= (num_of_fails-1) : # found 3 consecutive failed login
                        blocked_host[get_info.get_host(line)] = time_of_line # store in blocked_host
                        failed_login.pop(get_info.get_host(line)) # delete it from failed_login
                    else:
                        # found only 1 in queue, enqueue current line
                        current_q.put(line)
                else: # front of queue get out of the 20 seconds of sliding window
                    while time_of_line - get_info.get_time(get_info.get_raw_time(current_q.queue[0])) > time_window :
                        current_q.get() # front get out of sliding window
                        if current_q.empty():
                            break                        
                    current_q.put(line) # enqueue current line
        elif get_info.get_reply(line) == '200' : # succeed login
            if get_info.get_host(line) in failed_login:
                failed_login.pop(get_info.get_host(line)) # delete record of failed login
    text_file.close() # close file

# Additional feature----------------------------------
# This feature is similar to feature 3; it finds the busiest 1 hour period starting from an event occurs
def find_busiest_time_event_occur( data ):
    import datetime
    import Queue
    num_top = 10 # number of busiest periods 
    time_delta = datetime.timedelta( minutes=60 ) # length of period considered; 60 min in this case
    result = dict() # contains 10 busiest periods and number of visits
    q = Queue.Queue(0) # use a queue count number of visits in 60 mins period
    for line in data:
        raw_time = get_info.get_raw_time(line)
        if q.empty():
            q.put(raw_time)
        elif get_info.get_time(raw_time) - get_info.get_time(q.queue[0]) <= time_delta:
            q.put(raw_time)
        else:
            # queue contains more than 60 mins visits; store them in result if necessary
            if len(result) < num_top: # result has less than 10 records
                result[q.queue[0]] = result.get(q.queue[0], 0) + q.qsize()
            elif q.qsize() > min(result.values()):
                # replace the min of result by current period
                result.pop(min(result, key = result.get))
                result[q.queue[0]] = result.get(q.queue[0], 0) + q.qsize()
            # delete the front of queue until queue only contains 60 min period
            while get_info.get_time(raw_time) - get_info.get_time(q.queue[0]) > time_delta:
                q.get()
                if q.empty():
                    break
            q.put(raw_time) # enque the new time
            
    # Note in above implement, the last window may not be able to enter results!
    # So handle the last sliding window seperately; it's already in q
    if q.queue[0] not in result:
        if len(result) < num_top:
            result[q.queue[0]] = result.get(q.queue[0], 0) + q.qsize()
        elif q.qsize() > min(result.values()):
            # replace the min of result by current period
            result.pop(min(result, key = result.get))
            result[q.queue[0]] = result.get(q.queue[0], 0) + q.qsize()
    
    # sort result and write result in text file
    text_file = open("log_output/hours_envent_occur.txt",'w')
    for key in sorted(result, key=result.get, reverse=True):
        text_file.write("%s,%d\n" % (key, result[key]))
    text_file.close()


# main--------------------------------------------
def main():
    find_active_host(data)
    find_popular_resource(data)
    find_busiest_time(data)
    find_blocked(data)
# call main function  
main()
# call additional feature function
#find_busiest_time_event_occur(data)

# test--------------------------------------------
#
#test_log=open("insight_testsuite/tests/test_features/log_input/log.txt", 'r')
#data=test_log.readlines()
#main()
#find_active_host(data)
#find_popular_resource(data)
#find_busiest_time(data)
#find_busiest_time_event_occur( data )
