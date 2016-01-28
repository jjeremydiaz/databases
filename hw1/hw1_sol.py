# ### * BEGIN STUDENT CODE *

# In[303]:

import apachetime
import time

def apache_ts_to_unixtime(ts):
    """
    @param ts - a Apache timestamp string, e.g. '[02/Jan/2003:02:06:41 -0700]'
    @returns int - a Unix timestamp in seconds
    """
    dt = apachetime.apachetime(ts)
    unixtime = time.mktime(dt.timetuple())
    return int(unixtime)


# In[304]:

def process_logs(dataset_iter):
    """
    Processes the input stream, and outputs the CSV files described in the README.    
    This is the main entry point for your assignment.
    
    @param dataset_iter - an iterator of Apache log lines.
    """
    # Prepare hits.csv
    with open("hits.csv", "w+") as hits_file:
        hits_file.write('ip,timestamp\n')
        
        #parse each line from the data set to get ip and timestamp, then write to hits.csv
        for i, line in enumerate(dataset_iter):
            temp = line.split(" ")
            hits_file.write(temp[0] + ',' + str(apache_ts_to_unixtime(temp[3] + " " + temp[4])) + '\n')
    
    # Prepare sessions.csv  
    #sort hits.csv first by first col, then second col, store in a temp file
    get_ipython().system(u'sort -k 1,1n -k 2,2n {"hits.csv"} > {"temp"}')
    #dummy line for last line to get read
    get_ipython().system(u'echo {"a,1"} >> {"temp"}')
    
    #write to sessions.csv, go one by one, if there is a difference in the second column of > 30 min
    #write the same ip as a new session. also write a counter for number of same session ips (num_hits)
    #and for session_length
    with open("sessions.csv", "w+") as sessions_file:
        sessions_file.write('ip,session_length,num_hits\n')
        with open("temp", "r") as temp_fin:
            start_ip = ""
            prev_ip = ""
            curr_ip = ""
            start_time = 0
            prev_time = 0
            curr_time = 0
            
            session_length = 0
            num_hits = 0
            
            next(temp_fin)
            for line in temp_fin:
                tok = line.split(",")
                curr_ip = tok[0]
                curr_time = int(tok[1])
                
                if start_ip == "":
                    start_ip = curr_ip
                    start_time = curr_time
                    prev_ip = curr_ip
                    prev_time = curr_time
                    session_length = 0
                    num_hits += 1
                    continue
                elif curr_ip != prev_ip or curr_time - prev_time > 1800:
                    sessions_file.write(prev_ip + "," + str(session_length) + "," + str(num_hits) + "\n")
                    start_ip = curr_ip
                    start_time = curr_time
                    prev_ip = curr_ip
                    prev_time = curr_time
                    session_length = 0
                    num_hits = 1
                    continue
                prev_ip = curr_ip
                prev_time = curr_time
                num_hits += 1
                session_length = curr_time - start_time 
    
    # Prepare session_length_plot.csv
    
    #sort sessions.csv by session length
    get_ipython().system(u'tail -n +2 sessions.csv | sort -t"," -k 2,2n > {"temp2"}')
    get_ipython().system(u'echo {"ignore"} >> {"temp2"}')
    with open("session_length_plot.csv", "w+") as session_length_plot:
        session_length_plot.write("left,right,count\n")
        with open("temp2", "r") as fin:
            next(fin)
        
            start = 0
            end = 2
            session_length_count = 1
            session_length = 0
            for line in fin:
                if line == "ignore\n":
                    session_length_plot.write(str(start) + "," + str(end) + "," + str(session_length_count) + "\n")
                    break
                tok = line.split(",")
                session_length = int(tok[1])
                if session_length >= start and session_length < end:
                    session_length_count += 1
                else:
                    session_length_plot.write(str(start) + "," + str(end) + "," + str(session_length_count) + "\n")
                    while not (session_length >= start and session_length < end):
                        start = end
                        end *= 2
                    
                    session_length_count = 1
    
    #remove temp files
    get_ipython().system(u'rm {"temp"}')
    get_ipython().system(u'rm {"temp2"}')
    
    print "Done."


# ### * END STUDENT CODE *
import os
DATA_DIR = os.environ['MASTERDIR'] + '/sp16/hw1/'

import zipfile

def process_logs_large():
    """
    Runs the process_logs function on the full dataset.  The code below 
    performs a streaming unzip of the compressed dataset which is (158MB). 
    This saves the 1.6GB of disk space needed to unzip this file onto disk.
    """
    with zipfile.ZipFile(DATA_DIR + "web_log_large.zip") as z:
        fname = z.filelist[0].filename
        f = z.open(fname)
        process_logs(f)
        f.close()
process_logs_large()
