import logging
import Queue

from kvstore import DBMStore, InMemoryKVStore

LOG_LEVEL = logging.WARNING

KVSTORE_CLASS = InMemoryKVStore

"""
Possible abort modes.
"""
USER = 0
DEADLOCK = 1

"""
Part I: Implementing request handling methods for the transaction handler

The transaction handler has access to the following objects:

self._lock_table: the global lock table. More information in the README.

self._acquired_locks: a list of locks acquired by the transaction. Used to
release locks when the transaction commits or aborts. This list is initially
empty.

self._desired_lock: the lock that the transaction is waiting to acquire as well
as the operation to perform. This is initialized to None.

self._xid: this transaction's ID. You may assume each transaction is assigned a
unique transaction ID.

self._store: the in-memory key-value store. You may refer to kvstore.py for
methods supported by the store.

self._undo_log: a list of undo operations to be performed when the transaction
is aborted. The undo operation is a tuple of the form (@key, @value). This list
is initially empty.

You may assume that the key/value inputs to these methods are already type-
checked and are valid.
"""

class TransactionHandler:

    def __init__(self, lock_table, xid, store):
        self._lock_table = lock_table
        self._acquired_locks = []
        self._desired_lock = None
        self._xid = xid
        self._store = store
        self._undo_log = []
    def perform_put(self, key, value):
        """
        Handles the PUT request. You should first implement the logic for
        acquiring the exclusive lock. If the transaction can successfully
        acquire the lock associated with the key, insert the key-value pair
        into the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.
        Hint: be aware that lock upgrade may happen.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted. See the code in abort()
        for the exact format.

        @param self: the transaction handler.
        @param key, value: the key-value pair to be inserted into the store.

        @return: if the transaction successfully acquires the lock and performs
        the insertion/update, returns 'Success'. If the transaction cannot
        acquire the lock, returns None, and saves the lock that the transaction
        is waiting to acquire in self._desired_lock.
        """
        # Part 1.1: your code here!
        #If there is no lock add an exclusive lock to the lock table and add
		#the new lock to the transaction's acquired_locks. Lock table has
        #format (curr_type, [xid], waitlist)
        if key not in self._lock_table:
	    	#If the key did not exist previously add (key, None) to undo_log
			#so the key can be removed if the transaction gets aborted
            if self._store.get(key):
                self._undo_log.append((key, None))
            else:
                self._undo_log.append((key, self._store[key]))
            
            self._lock_table[key] = ['X', [self._xid], []]
            self._store.put(key, value)
            self._acquired_locks.append(key)
            return 'Success'
        #Already have X lock to write
        elif key in self._lock_table and self._lock_table.get(key)[0] == 'X' and self._xid in self._lock_table.get(key)[1]:
            self._store.put(key, value)
            return 'Success'
		#There is a conflicting lock, must add lock request to queue and set
		#the desired lock, the transaction will be paused until the desired
		#lock is free
        else:
            #Handles upgrade case, which only occurs when queue is empty and curr lock
			#type is shared and it is the sole owner
            if not self._store.get(key): #lock exists but no value exists
                self._undo_log.append((key, None))
            else:
                self._undo_log.append((key, self._store.get(key)))
            
            temp = self._lock_table[key]
            if not temp[2] and temp[0] == 'S':
                if len(temp[1]) == 1 and self._xid == temp[1][0]:
                    temp[0] = 'X'
                    self._store.put(key, value)
                    return 'Success'			  

			#Need to queue for an X lock
            self._desired_lock = (key, 'X', value)
            temp[2].append((self._xid, 'X'))
            return None

    def perform_get(self, key):
        """
        Handles the GET request. You should first implement the logic for
        acquiring the shared lock. If the transaction can successfully acquire
        the lock associated with the key, read the value from the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.

        @param self: the transaction handler.
        @param key: the key to look up from the store.

        @return: if the transaction successfully acquires the lock and reads
        the value, returns the value. If the key does not exist, returns 'No
        such key'. If the transaction cannot acquire the lock, returns None,
        and saves the lock that the transaction is waiting to acquire in
        self._desired_lock.
        """
        # Part 1.1: your code here!
        value = self._store.get(key)
        #The value does not exist in the data store put a lock on the value
		#and map it to none
        if value is None:
            if key not in self._lock_table:
                self._lock_table[key] = ['S', [self._xid], []]
                self._acquired_locks.append(key)
            #if key is in lock table it just have a lock but no value
            elif key in self._lock_table and self._lock_table[key][0] == 'S' and self._xid not in self._lock_table[key][1]:
                self._lock_table[key][1].append(self._xid)
                self._acquired_locks.append(key)
            return 'No such key'
		#The value exists
        else:
			#Check if there is a lock, if there is no lock, add a lock, if
			#there is a shared lock, add a lock, if there is an exclusive
			#lock, put the key in the queue and change desired lock to key
            #print "test"
			#Check if another transaction has a X lock on the key, if so add request to queue
            temp = self._lock_table.get(key)
            if temp and temp[0] == 'X' and self._xid not in temp[1]:
                temp[2].append((self._xid, 'S'))
                self._desired_lock = (key, 'S', None)
                return None
			#Check if current transaction has an X lock with the key, if so then return value
            elif temp and temp[0] == 'X' and temp[1][0] == self._xid:
                return value
            elif temp and temp[0] == 'S' and self._xid not in temp[1]:
                temp[1].append(self._xid)
                return value
            else:
				#lock doesn't exist in current transaction, add a shared lock
                #prevents multiple key creation from same transaction
                if key not in self._acquired_locks: 
                    self._lock_table[key] = ['S', [self._xid], []]
                    self._acquired_locks.append(key)
                    return value	
                else: #shared locks exist for the same key
                    self._lock_table[key][1].append(self._xid)
                    return value
			
    def release_and_grant_locks(self):
        """
        Releases all locks acquired by the transaction and grants them to the
        next transactions in the queue. This is a helper method that is called
        during transaction commits or aborts. 

        Hint: you can use self._acquired_locks to get a list of locks acquired
        by the transaction.
        Hint: be aware that lock upgrade may happen.

        @param self: the transaction handler.
        """
        for l in self._acquired_locks:
            temp = self._lock_table.get(l)
            temp[1].remove(self._xid) #removes self from transaction list
            #Check waitlist is not empty
            if not temp[2]:
                if not temp[1]: #if nothing in queue and key is empty, delete key in table
                    del self._lock_table[l]
                continue            

            #Check if upgrade is possible, check if current lock is 'S' and also
            #check if only 1 xid has that lock and check if that xid has a request
            #for an X lock for the same key and also check that queue is not empty   
            if temp[0] == 'S' and len(temp[1]) == 1 and (temp[1][0], 'X') in temp[2]:
                temp[2].remove((temp[1][0], 'X')) #remove from waitlist
                temp[0] = 'X' #Change to a X lock
            #Otherwise traverse waitlist, if pop is X, add if temp[1] is empty, if pop is s and curr lock
            #is S, keep adding all S on the queue until waitlist hits empty or X
            else:
                #There are no transactions so it can add multiple consecutive S transactions or a single X
                if temp[2][0][1] == 'X' and len(temp[1]) == 0:
                    temp[1].append(temp[2].pop(0)[0])                      
                    temp[0] = 'X'
                else: #The next item in waitlist is S, add consecutive S
                    count = 0 
                    for xid, lock in temp[2]:
                        if lock == 'X':
                            break
                        temp[1].append(xid)
                        count += 1
                    #clean waitlist
                    for i in range(count):
                        temp[2].pop(0)
            
            #Check if value is in the waitlist and remove it in case it was aborted
            if self._desired_lock and (self._xid, self._desired_lock[1]) in temp[2]: 
                temp[2].remove((self._xid, self._desired_lock[1]))    
        self._acquired_locks = []
        self._desired_lock = None
    
    def commit(self):
        """
        Commits the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.

        @return: returns 'Transaction Completed'
        """
        self.release_and_grant_locks()
        return 'Transaction Completed'

    def abort(self, mode):
        """
        Aborts the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.
        @param mode: mode can either be USER or DEADLOCK. If mode == USER, then
        it means that the abort is issued by the transaction itself (user
        abort). If mode == DEADLOCK, then it means that the transaction is
        aborted by the coordinator due to deadlock (deadlock abort).

        @return: if mode == USER, returns 'User Abort'. If mode == DEADLOCK,
        returns 'Deadlock Abort'.
        """
        while (len(self._undo_log) > 0):
            k,v = self._undo_log.pop()
            self._store.put(k, v)
        self.release_and_grant_locks()
        if (mode == USER):
            return 'User Abort'
        else:
            return 'Deadlock Abort'

    def check_lock(self):
        """
        If perform_get() or perform_put() returns None, then the transaction is
        waiting to acquire a lock. This method is called periodically to check
        if the lock has been granted due to commit or abort of other
        transactions. If so, then this method returns the string that would 
        have been returned by perform_get() or perform_put() if the method had
        not been blocked. Otherwise, this method returns None.

        As an example, suppose Joe is trying to perform 'GET a'. If Nisha has an
        exclusive lock on key 'a', then Joe's transaction is blocked, and
        perform_get() returns None. Joe's server handler starts calling
        check_lock(), which keeps returning None. While this is happening, Joe
        waits patiently for the server to return a response. Eventually, Nisha
        decides to commit his transaction, releasing his exclusive lock on 'a'.
        Now, when Joe's server handler calls check_lock(), the transaction
        checks to make sure that the lock has been acquired and returns the
        value of 'a'. The server handler then sends the value back to Joe.

        Hint: self._desired_lock contains the lock that the transaction is
        waiting to acquire.
        Hint: remember to update the self._acquired_locks list if the lock has
        been granted.
        Hint: if the transaction has been granted an exclusive lock due to lock
        upgrade, remember to clean up the self._acquired_locks list.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted.

        @param self: the transaction handler.

        @return: if the lock has been granted, then returns whatever would be
        returned by perform_get() and perform_put() when the transaction
        successfully acquired the lock. If the lock has not been granted,
        returns None.
        """
        #Use desired_lock (key, lock, value (optional)) and check the lock table to see if the transaction
        #has gained access to that lock. If yeah return the result of the put or get
        #operation, otherwise return None as it has not been blocked yet.
        desired_lock = self._desired_lock
        wanted_key = desired_lock[0]
        #If request is not in the queue a lock is available
        if (self._xid, desired_lock[1]) not in self._lock_table.get(wanted_key)[2]:
            if desired_lock[1] == 'S' and self._lock_table.get(wanted_key)[0] == 'X' or self._lock_table.get(wanted_key)[0] == 'S':
                return self.perform_get(wanted_key)

            elif desired_lock[1] == 'X' and self._lock_table.get(wanted_key)[0] == 'X':
                return self.perform_put(wanted_key, desired_lock[2]) 
        else:
            return None


"""
Part II: Implement deadlock detection method for the transaction coordinator

The transaction coordinator has access to the following object:

self._lock_table: see description from Part I
"""

class TransactionCoordinator:

    def __init__(self, lock_table):
        self._lock_table = lock_table

    def detect_deadlocks(self):
        """
        Constructs a waits-for graph from the lock table, and runs a cycle
        detection algorithm to determine if a transaction needs to be aborted.
        You may choose which one transaction you plan to abort, as long as your
        choice is deterministic. For example, if transactions 1 and 2 form a
        cycle, you cannot return transaction 1 sometimes and transaction 2 the
        other times.

        This method is called periodically to check if any operations of any
        two transactions conflict. If this is true, the transactions are in
        deadlock - neither can proceed. If there are multiple cycles of
        deadlocked transactions, then this method will be called multiple
        times, with each call breaking one of the cycles, until it returns None
        to indicate that there are no more cycles. Afterward, the surviving
        transactions will continue to run as normal.

        Note: in this method, you only need to find and return the xid of a
        transaction that needs to be aborted. You do not have to perform the
        actual abort.

        @param self: the transaction coordinator.

        @return: If there are no cycles in the waits-for graph, returns None.
        Otherwise, returns the xid of a transaction in a cycle.
        """
        #will return false if there is no cycle otherwise return first deadlocked
        #xid uses variation of http://codereview.stackexchange.com/questions/86021/check-if-a-directed-graph-contains-a-cycle
        def xid_cycle(graph):
            path = set()
            
            def visit(vertex):
                path.add(vertex)
                for neighbour in graph.get(vertex, ()):
                    if neighbour in path or visit(neighbour):
                        return vertex
                path.remove(vertex)
                return False  
            for v in graph:
                test = visit(v)
                if test:
                    return test
            return False

        #Form graph where first value in the waitlist has edges to current values
        #that it is waiting for. Do that for each key in the dictionary
        #print self._lock_table
        graph = {}
        for key, value in self._lock_table.iteritems():
            if not value[2]: #if nothing in queue, skip key
                continue
            graph[value[2][0][0]] = value[1]
        
        #If graph is empty, return None
        if not graph:
            return None
        #otherwise do a dfs and check to see if a cycle exists. Return an xid involved in a cycle
        #print graph 
        else:
            result = xid_cycle(graph)
            if result == False: #There are no cycles
                return None
            else:
               return result
