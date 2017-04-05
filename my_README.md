# Insight Fansite Analytics Challenge
Used Python to solve the problem.

# Publicly Available Modula used

### import datetime
Used datetime Modula in feature 3 and 4 to create datetime objects.

### import Queue
Used Queue Modula in feature 4 and additional feature to store visits in a sliding time window.

### from collections import Counter
I'm not actually using collections Modula. I initially used Counter function from collections modula in Feature 1 and 2. Used it to sort so that we can get most active hosts and resources.
As the problem asks for only top 10 of hosts and resources, sorting is actually not necessary. So I can simply go over the lists and find the top 10 using min-heap, which can be more efficient than sorting the whole list.
But if the number of top active hosts is changed to a large number that is comparable to the length of list, sorting method would be the same efficient.

# Additional Feature
This feature is similar to feature 3, but it finds the busiest 1 hour period starting from an event occurs.
I believe this makes more sense, compared to feature 3. Because if using feature 3, once the busiest period is found, denoted by b_t, then the second busiest is
probably b_t + 1 second, and third busiest period is probably b_t + 2 second and so on so forth. This happens because a lot of 
events are colustered within a short period. My additional feature may handle this situation better, but the best way to handle
this, I believe, is to find the 10 busiest periods that are not overlapping with each other. But this will be time consuming.  

To run the additional feature, please call function find_busiest_time_event_occur(data). The main() function will not call it automatically.
find_busiest_time_event_occur will write its results in log_output/hours_envent_occur.txt file.