# insight fansite analytics challenge
# Author: Rentao Sun

# read data -------------------------------------
# import os
# os.chdir('..') # set working dictionary
log = open("log_input/log.txt","r")
data=log.readlines() # raw data
    
# extract information from each line-------------
# get host name from each line
def get_host( line ):
    end=line.find(" ")
    host=line[0:end]
    return host

# get timestamp
def get_raw_time( line ):
    begin=line.find("[")+1
    end=line.find("]")
    raw_time = line[begin:end] # raw time in string form
    return raw_time

def get_time( raw_time ):
    import datetime # convert string to datetime
    # convert raw time to  datetime object
    # because all lines have the same time zone, ignore timezone for now!
    time = datetime.datetime.strptime(raw_time[:-5], "%d/%b/%Y:%H:%M:%S ")
    return time

# get resource
# request content are not in consistant format, need careful handle!
def get_resource( line ):
    begin_request=line.find('"') # request start with '"'
    end_request = line.find( '"', begin_request + 1 )    
    begin=line.find(' ', begin_request) + 1 # begin of resource
    if 'HTTP/1.0' in line:
        end = line.rfind(' ', begin_request, end_request) # end of resource
    else:
        end=end_request
    res = line[begin:end]
    return res
    
# get reply
def get_reply( line ):
    end=line.rfind(' ')
    begin= line.rfind(' ', 0,end)+1
    reply= line[begin:end]
    return reply
    
# get byte
def get_byte( line ):
    begin=line.rfind(' ')+1
    end = line.rfind('\n')
    tmp = line[begin:end]
    if tmp == '-': # convert '-' to 0 byte
        ret = 0
    else:
        ret = int(tmp)
    return ret
    
# feature 1 ----------------------------------
def find_active_host( data ):
    from collections import Counter # use Counter to find top active hosts
    num_top=10 # set the number of top active hosts
    # compute occurrences 
    counts=dict() # use dictionary data structure
    for line in data:
        key=get_host(line)
        counts[key] = counts.get(key,0) + 1
    # find most active host
    active_host = Counter(counts)
    text_file = open("log_output/hosts.txt",'w')
    for host, v in active_host.most_common(num_top):
        text_file.write( "%s,%d\n" % (host,v))
    text_file.close()

# feature 2--------------------------------------
def find_popular_resource( data ):
    from collections import Counter # use Counter to find top popular resource
    num_top=10 # set the number of most popular resource
    # compute occurrences 
    bandwidth=dict() # use dictionary data structure
    for line in data:
        key=get_resource(line)
        bandwidth[key] = bandwidth.get(key,0) + get_byte(line)
    # find most popular resource
    popular_resource = Counter(bandwidth)
    text_file = open("log_output/resources.txt",'w')
    for resource, v in popular_resource.most_common(num_top):
        text_file.write( "%s\n" % (resource))
    text_file.close()  

# feature 3-----------------------------------------
def find_busiest_time( data ):
    import datetime
    import Queue
    num_top = 10 # number of busiest periods 
    time_delta = datetime.timedelta( minutes=60 ) # length of period considered; 60 min in this case
    result = dict() # contains 10 busiest periods and number of visits
    q = Queue.Queue(0) # use a queue count number of visits in 60 mins period
    for line in data:
        raw_time = get_raw_time(line)
        if q.empty():
            q.put(raw_time)
        elif get_time(raw_time) - get_time(q.queue[0]) <= time_delta:
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
            while get_time(raw_time) - get_time(q.queue[0]) > time_delta:
                q.get()
                if q.empty():
                    break
            q.put(raw_time) # enque the new time
    # note in above implement, last window can never enter results; problem! 
    # sort result and write result in text file
    text_file = open("log_output/hours.txt",'w')
    for key in sorted(result, key=result.get, reverse=True):
        text_file.write("%s,%d\n" % (key, result[key]))
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
        time_of_line = get_time(get_raw_time(line)) # get the time of current line
        
        # found in blocked_host, then document it
        if get_host(line) in blocked_host :
            # still in 5 mins blocked period
            if time_of_line - blocked_host[get_host(line)] <= time_blocked :
                text_file.write("%s" % line) # document blocked login
            # blocked time ends
            elif get_reply(line) != '401' :
                blocked_host.pop(get_host(line)) # delete host since blocked time ends
            elif get_reply(line) == '401' : # detect a failed login
                blocked_host.pop(get_host(line)) # delete host since blocked time ends
                q= Queue.Queue(0) # initialize queue
                q.put(line) # enqueue failed login
                failed_login[get_host(line)]= q # input queue into failed_login

        # not found in blocked_host
        elif get_reply(line) == '401': # login failed
            if get_host(line) not in failed_login: # current host is NOT in failed_login
                q = Queue.Queue(0) # initialize queue
                q.put(line) # enqueue failed login
                failed_login[get_host(line)]= q # input queue into failed_login
            else:
                current_q = failed_login[get_host(line)] # current host is already in failed_login
                if time_of_line - get_time(get_raw_time(current_q.queue[0])) <= time_window :
                    if current_q.qsize() >= (num_of_fails-1) : # found 3 consecutive failed login
                        blocked_host[get_host(line)] = time_of_line # store in blocked_host
                        failed_login.pop(get_host(line)) # delete it from failed_login
                    else:
                        # found only 1 in queue, enqueue current line
                        current_q.put(line)
                else: # front of queue get out of the 20 seconds of sliding window
                    while time_of_line - get_time(get_raw_time(current_q.queue[0])) > time_window :
                        current_q.get() # front get out of sliding window
                        if current_q.empty():
                            break                        
                    current_q.put(line) # enqueue current line
        elif get_reply(line) == '200' : # succeed login
            if get_host(line) in failed_login:
                failed_login.pop(get_host(line)) # delete record of failed login
    text_file.close() # close file

# main--------------------------------------------
def main():
    find_active_host(data)
    find_popular_resource(data)
    find_busiest_time(data)
    find_blocked(data)
# call main function  
main()
# find_active_host(data)

# test--------------------------------------------
#
#test_log=open("my_test_log_input.txt", 'r')
#test_data=test_log.readlines()
#find_blocked(test_data)
