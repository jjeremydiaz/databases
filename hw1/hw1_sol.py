# ### * BEGIN STUDENT CODE *

# In[4]:

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


# In[5]:

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
    
    #get the max session_length that is generated in sessions.csv
    get_ipython().system(u'tail -n +2 sessions.csv | awk -F"," -v max=0 \'{if($2>max){max = $2}}END{print max}\' > temp2')
    curr_max = 0
    with open("temp2", "r") as max_check:
        curr_max = int(next(max_check))
    #generate range pairs in a list as a tuple key
    ranged_pairs = [[(0,2), 0]]
    start = 2
    end = 4
    while curr_max > start:
        ranged_pairs.append([(start, end), 0])
        start *= 2
        end *= 2
        
    #go through each session and place in the appropriate bin
    with open("sessions.csv", "r") as fin:
        next(fin)
        for line in fin:
            tok = line.split(",")
            session_length = int(tok[1])
            
            for i in ranged_pairs:
                if session_length >= i[0][0] and session_length < i[0][1]:
                    i[1] += 1
                    break
                    
    #write bins in order, if they have a count of 0, skip those bins
    with open("session_length_plot.csv", "w+") as fin:
        fin.write("left,right,count\n")
        for i in ranged_pairs:
            if i[0][1] != 0:
                fin.write(str(i[0][0]) + "," + str(i[0][1]) + "," + str(i[1]) + "\n")
    
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
