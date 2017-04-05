# Insight Fansite Analytics Challenge
Used Python to solve the problem.

# Publicly Available Modula used
### from collections import Counter
Used Counter function from collections modula in Feature 1 and 2. Used it to sort so that we can get most active hosts and resources.
As the problem asks for only top 10 of hosts and resources, sorting is actually not necessary. We can simply go over the lists and find the top 10 using min-heap, which can be more efficient than sorting the whole list.
But if the number of top active hosts is changed to a large number that is comparable to the length of list, sorting method would be the same efficient.

### import datetime
Used datetime Modula in feature 3 and 4 to create datetime objects.

### import Queue
Used Queue Modula in feature 3 and 4 to count the number of visits in a sliding time window.


