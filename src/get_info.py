# extract information from each line of log.txt -------------

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
    