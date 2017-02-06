
import time
import uuid
from random import randint

MAXI = 999999999999999999

class TransactionRecord:
    
    def __init__(self, tid, start_time, fkey):
        self.tnx_id = tid
        self.orig_timestamp = start_time
        self.lower_bound = TimeStamp(0)
        self.upper_bound = TimeStamp(MAXI)
        self.tmeta = Tnxmeta(tid, fkey)
        # keys on which the transaction have placed intent
        self.write_intent_keys = []
        self.read_inetent_keys = []
        self.transaction_status = "PENDING"

        # queues 
        self.commit_before_them = []

class Tnxmeta:
	def __init__(self, tid, fkey = None):
		self.tnxid = tid
		self.first_key = fkey

#------------------------------------------------------------------
# DB layer

class Versions:
	def __init__(self):
		self.version = {}
	def add_version(self, timestamp, value):
		self.version[timestamp] = value
	def get_version(self, timestamp):
		l = list(self.version.keys())
		l.sort(reverse=True)
		for each in l:
			if each < timestamp:
				return self.version[each]

class MVCC:

	def __init__(self):
		self.db = { }

	def put(self, tnx_timestamp, key, value):
		if key not in self.db:
			new_version = Versions()
			self.db[key] = new_version
		self.db[key].add_version(tnx_timestamp, value)
		print("putting " + key)

	def scan(self, tnx_timestamp, key):
		if key not in self.db:
			print("No such key :" + str(key))
			return
		return self.db[key].get_version(tnx_timestamp)

	# place the transaction record in DB and return the key 
	def put_proto(self, tkey, proto):
		self.db[tkey] = proto


	# get the transaction record stored at a key
	def get_proto(self, tkey):
		return self.db[tkey]

	def get_proto_key(self, fkey, tid):
		return tid

#---------------------------------------------------------------------------
# cache layer

# holds the latest commited transactions timestamp for each key
class Timestampcache:
	def __init__(self):
		self.readcache = {}
		self.writecache = {}

	def add_to_rcache(self, key, timestamp):
		if key not in self.readcache:
			self.readcache[key] = TimeStamp()
			self.readcache[key].equal(timestamp)
		else:
			timestamp.get_ts() > self.readcache[key].get_ts()
			self.readcache[key].equal(timestamp)

	def add_to_wcache(self, key, timestamp):
		if key not in self.writecache:
			print("adding key "+ key + " in write cache")
			self.writecache[key] = TimeStamp()
			self.writecache[key].equal(timestamp)
		else:
			timestamp.get_ts() > self.writecache[key].get_ts()
			self.writecache[key].equal(timestamp)

	def get_latest_read_ts(self, key):
		if key not in self.readcache:
			return 0
		return self.readcache[key]
	def get_latest_write_ts(self, key):
		if key not in self.writecache:
			return 0
		return self.writecache[key]

# holds read and write soft locks for each key
class SoftLocksCache:
	def __init__(self):
		self.read_soft_lock = {}
		self.write_soft_lock = {}

	def place_read_soft_lock(self, key, slock):
		if key not in self.read_soft_lock:
			self.read_soft_lock[key] = []
		self.read_soft_lock[key].append(slock)

	def place_write_soft_lock(self, key, slock):
		if key not in self.write_soft_lock:
			self.write_soft_lock[key] = []
		self.write_soft_lock[key].append(slock)

	def remove_read_lock(self, key, slock = None):
		self.read_soft_lock[key].remove(slock)

	def get_my_read_lock(self, key, tmeta):
		for each in self.read_soft_lock[key]:
			if each.tnx_meta == tmeta:				
				return each

	def remove_write_lock(self, key, slock):
		self.write_soft_lock[key].remove(slock)

	def get_my_write_lock(self, key, slock):
		for each in self.write_soft_lock[key]:
			if each.tnx_meta == tmeta:
				return each

	def get_placed_read_soft_locks(self, key):
		if key not in self.read_soft_lock:
			return None
		return self.read_soft_lock[key]

	def get_placed_write_soft_locks(self, key):
		if key not in self.write_soft_lock:
			return None
		return self.write_soft_lock[key]


class SoftLock:
	def __init__(self, tmeta, key, value = None ):
		self.key = key
		self.value = value
		self.tnx_meta = tmeta


#----------------------------------------------------------------
class TimeStamp:
	def __init__(self, t = None):
		if t == None:
			self.timestamp = int(time.time())
		else:
			self.timestamp = t

	def get_ts(self):
		return self.timestamp

	def next(self, ts):
		self.timestamp + 1

	def previous(self, ts):
		self.timestamp - 1

	def forward(self, ts):
		self.timestamp = ts + 1

	def backward(self, ts):
		self.timestamp = ts - 1

	def equal(self, ts):
		if type(ts) == int:
			self.timestamp = ts
		else:
			self.timestamp = ts.get_ts() 

	def printt(self):
		return str(self.timestamp)

class MaaT:
	def __init__(self, mvcc, tscache, slockcache):
		self.mvcc = mvcc
		self.tscache = tscache
		self.slockcache = slockcache

	def is_valid_range(self, lb_ts, ub_ts):
		if lb_ts.get_ts() <= ub_ts.get_ts():
			return True
		else:
			False

	def validate_write_lock_on_read(self, record, slock):
		print("validaing write lock on read" )
		# get tnx record of tnx that have placed intent
		pkey = self.mvcc.get_proto_key(slock.tnx_meta.first_key, slock.tnx_meta.tnxid)
		l_record = self.mvcc.get_proto(pkey)
		print(l_record.transaction_status)
		#check status of tnx that placed lock
		if l_record.transaction_status == "ABORTED":
			self.slockcache.remove_write_lock(slock.key, slock)
		elif l_record.transaction_status == "COMITTED":
			#write the intent value to the database
			self.mvcc.put(l_record.orig_timestamp.get_ts(), slock.key, slock.value)
			#update the last comitted write timestamp
			self.tscache.add_to_wcache(slock.key , l_record.orig_timestamp)
			#trigger raft replication
			print("Replicating write via raft")
			self.slockcache.remove_write_lock(slock.key, slock)
		elif l_record.transaction_status == "PENDING":
			#adjust my upper bound
			record.upper_bound.backward(l_record.lower_bound.get_ts())

	def validate_read_lock_on_write(self, record, slock):
		print("validating read lock on write")
		# get tnx record of tnx that have placed intent
		pkey = self.mvcc.get_proto_key(slock.tnx_meta.first_key, slock.tnx_meta.tnxid)
		l_record = self.mvcc.get_proto(pkey)

		#check status of tnx that placed lock
		if l_record.transaction_status == "ABORTED":
			self.slockcache.remove_read_lock(slock.key, slock)
		elif l_record.transaction_status == "COMITTED":
			#update last committed read timestamp
			self.tscache.add_to_rcache(slock.key, l_record.orig_timestamp)
			#update my lower bound
			record.lower_bound.forward(l_record.orig_timestamp.get_ts())
		elif l_record.transaction_status == "PENDING":
			l_record.commit_before_them.append(record.tnx_meta)

	def validate_write_lock_on_write(self, record, slock):
		print("validating write lock on write")
		# get tnx record of tnx that have placed intent
		pkey = self.mvcc.get_proto_key(slock.tnx_meta.first_key, slock.tnx_meta.tnxid)
		l_record = self.mvcc.get_proto(pkey)

		#check status of tnx that placed lock
		if l_record.transaction_status == "ABORTED":
			self.slockcache.remove_write_lock(slock.key, slock)
		elif l_record.transaction_status == "COMITTED":
			#write the intent value to the database
			self.mvcc.put(l_record.orig_timestamp.get_ts(), slock.key, slock.value)
			#update the last comitted write timestamp
			self.tscache.add_to_wcache(slock.key, l_record.orig_timestamp)
			#trigger raft replication
			print("Replicating write via raft")
			#adjust my lower bound
			record.lower_bound.forward(l_record.orig_timestamp.get_ts())
		elif l_record.transaction_status == "PENDING":
			l_record.commit_before_them.append(record.tnx_meta)

	def validate_on_commit(self, record, btnx_meta):
		print("validating on commit")
		# get tnx record of tnx that have placed intent
		pkey = self.mvcc.get_proto_key(btnx_meta.first_key, btnx_meta.tnxid)
		l_record = self.mvcc.get_proto(pkey)

		#check status of tnx that placed lock
		if l_record.transaction_status == "ABORTED":
			pass
		elif l_record.transaction_status == "COMITTED":
			record.upper_bound.backward(l_record.orig_timestamp.get_ts())
		elif l_record.transaction_status == "PENDING":
			record.upper_bound.backward(l_record.lower_bound.get_ts())

	def set_commit_timestamp(self, record):
		if record.upper_bound.get_ts() == MAXI:
			record.orig_timestamp.equal(record.lower_bound.get_ts())
		else:
			record.orig_timestamp.equal(randint(record.lower_bound.get_ts(), record.upper_bound.get_ts()))
		print("commiting at " + record.orig_timestamp.printt())
#--------------------------------------------------------------
class Replica:
	def __init__(self):
		self.mvcc = MVCC()
		self.timestampcache = Timestampcache()
		self.softlockscache = SoftLocksCache()
		self.maat = MaaT(self.mvcc, self.timestampcache, self.softlockscache)

	def begin_transaction_request(self, fkey):
		print("begin request")
		tnx_id = uuid.uuid4()
		timestamp = TimeStamp()
		print("starting at "+ timestamp.printt())
		record = TransactionRecord(tnx_id, timestamp, fkey)
		pkey = self.mvcc.get_proto_key(fkey, tnx_id)
		self.mvcc.put_proto(pkey, record)
		return record

	def scan_request(self, tmeta, key):
		print("scan request")
		# get tnx record
		pkey = self.mvcc.get_proto_key(tmeta.first_key, tmeta.tnxid)
		record = self.mvcc.get_proto(pkey)

		# get all placed write locks for the key
		UW = self.softlockscache.get_placed_write_soft_locks(key)
		
		for each in UW:
			self.maat.validate_write_lock_on_read(record, each)

		# get last write ts from write cache
		last_write_ts = self.timestampcache.get_latest_write_ts(key)
		print("last write ts "+ str(last_write_ts.get_ts()))
		# if last_write > my_timestamp : LB = UB = my_timestamp
		if last_write_ts.get_ts() > record.orig_timestamp.get_ts():
			record.lower_bound.equal(record.orig_timestamp)
			record.upper_bound.equal(record.orig_timestamp)
			#read
		else:
			#	 LB = max(last_write + 1, LB)
			record.lower_bound.forward(last_write_ts.get_ts())
			
			# place read lock in read cache
			soft_lock = SoftLock(record.tmeta, key)
			self.softlockscache.place_read_soft_lock(key, soft_lock)
			

		self.mvcc.put_proto(pkey, record)
		return self.mvcc.scan(record.orig_timestamp.get_ts(), key)

	def put_request(self, tmeta, key, value):
		# get tnx record
		print("put request")
		pkey = self.mvcc.get_proto_key(tmeta.first_key, tmeta.tnxid)
		record = self.mvcc.get_proto(pkey)

		# get all placed read locks for the key 
		UR = self.softlockscache.get_placed_read_soft_locks(key)
		
		if UR != None:
			for each in UR:
				self.maat.validate_read_lock_on_write(record, each)

		# get all placed write locks for the key 
		UW = self.softlockscache.get_placed_write_soft_locks(key)
		if UW != None:
			for each in UW:
				self.maat.validate_write_lock_write(record, each)

		#get last read from read from read cache
		last_read_ts = self.timestampcache.get_latest_read_ts(key)
	
		# LB = max(last_write + 1, LB )
		
		record.lower_bound.forward(last_read_ts)
	
		# place write lock for the key 
		soft_lock = SoftLock(record.tmeta, key, value)
		self.softlockscache.place_write_soft_lock(key, soft_lock)

	def commit_request(self, tmeta, write_keys, read_keys):
		print("commit request")
		# get tnx record
		pkey = self.mvcc.get_proto_key(tmeta.first_key, tmeta.tnxid)
		record = self.mvcc.get_proto(pkey)
		print("lb " + record.lower_bound.printt())
		print("ub " + record.upper_bound.printt())
		if not self.maat.is_valid_range(record.lower_bound, record.upper_bound):
			print("maat decision to abort : Invalid range")
			record.transaction_status = "ABORTED"

		for each in record.commit_before_them:
			self.maat.validate_on_commit(record, each)
			if not self.maat.is_valid_range(record.lower_bound, record.upper_bound):
				record.transaction_status = "ABORTED"
		
		if record.transaction_status == "PENDING":
			self.maat.set_commit_timestamp(record)
			record.transaction_status = "COMITTED"

		# handling write locks
		for key in write_keys:
			#remove locks
			slock = self.softlockscache.get_my_write_lock(key, record.tmeta)
			self.softlockscache.remove_write_lock(key, slock)
			#write the intent value to the database
			self.mvcc.put(record.orig_timestamp.get_ts(), key, slock.value)
			#update the last comitted write timestamp
			self.tscache.add_to_wcache(slock.key , record.orig_timestamp)
			#trigger raft replication
			print("Replicating write via raft")

		#handling read locks
		for key in read_keys:
			#remove locks
			slock = self.softlockscache.get_my_read_lock(key, record.tmeta)
			self.softlockscache.remove_read_lock(key, slock)
			#update the last comitted read timestamp
			self.tscache.add_to_rcache(slock.key , record.orig_timestamp)
			
#---------------------------------------------------------------


class TCS_meta:
	def __init__(self):
		self.tnx_proto = None 
		self.write_keys = []
		self.read_keys = []

class Transaction:
	def __init__(self):
		self.tcs_meta = None


	def begin(self):
		pass 

	def read(self, key):
		if self.tcs_meta is None:
			self.tcs_meta = TCS_meta()
			self.tcs_meta.tnx_proto = r.begin_transaction_request(key)
		return r.scan_request(self.tcs_meta.tnx_proto.tmeta, key)


	def write(self, key, value):
		if self.tcs_meta is None:
			self.tcs_meta = TCS_meta()
			self.tcs_meta.tnx_proto = r.begin_transaction_request(key)
		self.tcs_meta.write_keys.append(key)
		r.put_request(self.tcs_meta.tnx_proto.tmeta, key, value)

	def commit(self):
		r.commit_request(self.tcs_meta.tnx_proto.tmeta, self.tcs_meta.write_keys, self.tcs_meta.read_keys)



r = Replica()



t1 = Transaction()
t1.begin()
t1.write("1","100")
t1.commit()

time.sleep(5)
t2 = Transaction()
t2.begin()
print(t2.read("1"))
t2.commit()