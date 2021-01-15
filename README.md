## Hipages Data Engineer Tech Assignment

####  1- ETL program to read json event by event and perform a series of transformations on it to build a structured table as result.

#### 2- Coding Language: Python

#### 3- Development Environment Installation 
In order to run the ETL smoothly , you need the following tools:

  * Python 3.7, or above
  * [pip] package manager for your Python version
  * git

- In case you decide to run Python locally, I recommend installing a virtual environment for this ETL code.
- This should avoid any side effects regarding packages already installed on your system.
- After you installed your virtual environment including the package manager [pip]: 
  Please change into this repository's root and execute `pip install --upgrade -r requirements.txt` to install all requirements shared in requirements.txt file as per my environment.

Now you should be ready to run the code!

#### 4- ETL Instructions: 
- Please make sure to clone the git repository or place all files at same location including `source_event_data.json`

- The ETL code consists of files 1) `consume_events.py` 2) `event_processor.py`  3) `aggregate_stats.py`.

- Source file `consume_events.py` is the main file that should be executed to consume all the events.

- Source file `aggregate_stats.py` is the main file that should be executed for reporting, to calculate aggregated stats.

### 5- ETL Details and Approach.

- ETL program will consume events as Json and perform required transformations while consuming each event.

- Design approach for this ETL is to consume one event at a time as received so that this solution if required
can be integrated with some distributed file system or message queue and each event can be consumed
when received and further stored after transformations.

#####Table 1 (Csv): `user_web_hits.csv`
- Each event is processed one by one as received and transformed.
- Each event after consumption will be stored in a list to prepare a batch and once the batch size limit is reached,
data will be dumped into CSV (tabular form) which in the real scenario will be Data Lake to perform data Analytics. 

- Followed CSV append approach for dumping the events into csv on the go in batches.

- Batch size is 100 events for this ETL which can be changed if required. 

#####Table 2  (Csv): `aggregated_web_stats.csv`

- As this table is a reporting table with stats at hourly time granularity and as I observed and assumed from the sample event's data,
events are not received in timely order so followed the approach for full load for reporting which can be called on demand or setup to run once a day.


- ETL program `aggregate_stats.py` will fetch the processed events from the csv/table `user_web_hits.csv`. 
  ETL will perform aggregations to calculate unique_user_count and activity_counts 
  at hourly time granularity, url_level1 and activity in order to prepare the second table.
  
- Result will be stored as a second csv in tabular form `aggregated_web_stats.csv`


### Notes

Test cases are missing as I focused on the core features due to limited time constraint.

Some test scenarios to consider would be as following
1) Testing the user defined functions to check if the output is as expected.
2) Testing the output of the program if it's as required for normal events.
3) Testing by sending incomplete rows and then handling such cases with exception handling.
4) Testing schema violations, missing fields, incomplete urls, incorrect timestamps.
5) Load testing before the program can be pushed to production. 

### Assumptions: 
Events are consumed as received and pushed to the first table/csv (`user_web_hits`) as required. 

For the second table it is assumed that table scope is reporting so table is only aggregated on demand 
or can be setup to run daily by executing the file `aggregate_stats.py`

Event queue approach is not followed for the second task to perform aggregations as stats will be changing all the time with each new event and 
as I observed from data shared in `source_event_data.json` there is no timely sequence/pattern for the events so that is why reporting table is full load on demand or daily basis.

If the program is considered for real time systems and events are received in timely sequence then ETL code can be adjusted to perform incremental aggregations for last 'n' hours.
