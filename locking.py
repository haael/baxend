#!/usr/bin/python3.11
#-*- coding: utf-8 -*-


from logging import getLogger, basicConfig, DEBUG
log = getLogger(__name__)

if __name__ == '__main__':
	basicConfig(level=DEBUG, format='%(asctime)-8s %(levelname)-8s %(name)-32s %(message)s')
	
	import warnings
	warnings.filterwarnings('ignore')


__all__ = 'locked_ro', 'locked_rw', 'MultiLock', 'GlobalDict', 'Arbitrator', 'Driver', 'Accessor'


from time import time
from ctypes import c_int


def locked_ro(old_method):
	"Lock a method of Accessor for read-only operation."
	
	if hasattr(old_method, '__next__'):
		def new_method(self, *args, **kwargs):
			log.debug(f"locked_ro begin")
			
			with self.write_lock:
				with self.read_condition:
					try:
						self.read_counter.value += self.read_counter.increment
					except AttributeError:
						self.read_counter.value += 1
			
			try:
				yield from old_method(self, *args, **kwargs)
			finally:
				with self.read_condition:
					try:
						self.read_counter.value -= self.read_counter.increment
					except AttributeError:
						self.read_counter.value -= 1
					self.read_condition.notify()
				
				log.debug(f"locked_ro end")
	
	else:
		def new_method(self, *args, **kwargs):
			log.debug(f"locked_ro begin")
			
			with self.write_lock:
				with self.read_condition:
					try:
						self.read_counter.value += self.read_counter.increment
					except AttributeError:
						self.read_counter.value += 1
			
			try:
				return old_method(self, *args, **kwargs)
			finally:
				with self.read_condition:
					try:
						self.read_counter.value -= self.read_counter.increment
					except AttributeError:
						self.read_counter.value -= 1
					self.read_condition.notify()
				
				log.debug(f"locked_ro end")
	
	new_method.__name__ = old_method.__name__
	return new_method


def locked_rw(old_method):
	"Lock a method of Accessor for read-write operation."

	if hasattr(old_method, '__next__'):
		def new_method(self, *args, **kwargs):
			log.debug(f"locked_rw begin")
			
			with self.write_lock:
				with self.read_condition:
					self.read_condition.wait_for(lambda: self.read_counter.value == 0)
				try:
					yield from old_method(self, *args, **kwargs)
				finally:
					log.debug(f"locked_rw end")
	
	else:
		def new_method(self, *args, **kwargs):
			log.debug(f"locked_rw begin")
			
			with self.write_lock:
				with self.read_condition:
					self.read_condition.wait_for(lambda: self.read_counter.value == 0)
				try:
					return old_method(self, *args, **kwargs)
				finally:
					log.debug(f"locked_rw end")
	
	new_method.__name__ = old_method.__name__
	return new_method


class MultiLock:
	"Combine multiple instances of Lock or Condition into one."
	
	def __init__(self, *locks):
		self.locks = []
		for lock in locks:
			try:
				lock_set = lock.locks
			except AttributeError:
				self.locks.append(lock)
			else:
				self.locks.extend(lock_set)
		
		# remove duplicates
		todel = set()
		present = set()
		for n, lock in enumerate(self.locks):
			if id(lock) in present:
				todel.add(id(lock))
			else:
				present.add(id(lock))
		for n in reversed(sorted(todel)):
			del self.locks[n]
	
	def __enter__(self):
		self.acquire()
		return self
	
	def __exit__(self, *args):
		self.release()
	
	def acquire(self, blocking=True):
		locked_up_to = 0
		while True:
			try:
				for lock in self.locks:
					if not lock.acquire(blocking=False):
						for elock in self.locks[:locked_up_to]:
							elock.release()
						
						if blocking:
							locked_up_to = 0
							break # TODO: rate limit
							log.warning(f"MultiLock.acquire retry")
						else:
							return False
					else:
						locked_up_to += 1
				else:
					assert locked_up_to == len(self.locks)
					log.debug(f"MultiLock.acquire success")
					return True
			
			except:
				for lock in self.locks[:locked_up_to]:
					lock.release()
				raise
		
		raise RuntimeError("Why am I here?")
	
	def release(self):
		for lock in self.locks:
			lock.release()
		log.debug(f"MultiLock.release")
	
	#def locked(self):
	#	return all(_lock.locked() for _lock in self.locks)
	
	def notify(self):
		for lock in self.locks:
			lock.notify()
	
	def notify_all(self):
		for lock in self.locks:
			lock.notify_all()
	
	def wait(self, timeout=None):
		start_time = time()
		self.release()
		try:
			cur_time = start_time
			while timeout == None or timeout > cur_time - start_time:
				for lock in self.locks:
					with lock:
						if lock.wait(timeout=(timeout/10 if timeout != None else 0.001)):
							break
				else:
					cur_time = start_time
					continue
				break
			
			if timeout != None and timeout <= cur_time - start_time:
				return False
			else:
				return True
		finally:
			self.acquire()
	
	def wait_for(self, predicate, timeout=None):
		start_time = time()
		self.release()
		try:
			cur_time = start_time
			while timeout == None or timeout > cur_time - start_time:
				for lock in self.locks:
					with lock:
						if lock.wait_for(predicate, timeout=(timeout/10 if timeout != None else 0.001)):
							break
				else:
					cur_time = start_time
					continue
				break
			
			if timeout != None and timeout <= cur_time - start_time:
				return False
			else:
				return True
		finally:
			self.acquire()


class MultiInt:
	def __init__(self, *integers):
		self.integers = integers
	
	@property
	def increment(self):
		return len(self.integers) # TODO: recursive MultiInt
	
	@property
	def value(self):
		return sum(_integer.value for _integer in self.integers)
	
	@value.setter
	def value(self, value):
		if (value - self.value) % self.increment:
			raise ValueError(f"Value {value} - {self.value} not a multiply of {self.increment}")
		
		sv = self.value
		si = self.increment
		for integer in self.integers:
			integer.value += (value - sv) // si


class GlobalDict:
	def __init__(self, backing, constructor, lock):
		# TODO: invalidate cache
		self.cache = {}
		self.backing = backing
		self.constructor = constructor
		self.lock = lock
	
	def __getitem__(self, key):
		try:
			return self.cache[key]
		except KeyError:
			pass
		
		with self.lock:
			try:
				result = self.backing[key]
				self.cache[key] = result
				return result
			except KeyError:
				result = self.constructor()
				self.backing[key] = result
				self.cache[key] = result
				return result
	
	def __setitem__(self, key, value):
		with self.lock:
			self.backing[key] = value
		self.cache[key] = value
	
	def __delitem__(self, key):
		with self.lock:
			del self.backing[key]
		del self.cache[key]


class Arbitrator:
	"Arbitrator class, setting up the global state and managing access to resources. Should be run on the central server."
	
	def __init__(self, manager):
		self.manager = manager
	
	def prepare_namespace(self):
		manager = self.manager
		self.global_ = manager.Namespace()
		self.global_.write_lock_lock = manager.Lock()
		self.global_.write_lock = manager.dict()
		self.global_.read_condition_lock = manager.Lock()
		self.global_.read_condition = manager.dict()
		self.global_.read_counter_lock = manager.Lock()
		self.global_.read_counter = manager.dict()


class Driver:
	"Driver class. Groups resources into logical categories."
	
	def __init__(self, arbitrator):
		global_ = arbitrator.global_
		manager = arbitrator.manager
		self.read_condition = GlobalDict(global_.read_condition, manager.Condition, global_.read_condition_lock)
		self.read_counter = GlobalDict(global_.read_counter, (lambda: manager.Value(c_int, 0)), global_.read_counter_lock)
		self.write_lock = GlobalDict(global_.write_lock, manager.Lock, global_.write_lock_lock)


class Accessor:
	"Dict-like object that belongs to a certain driver. Its methods may be decorated with `locked_ro` or `locked_rw`."
	
	def __init__(self, driver, lock_key, multi=False):
		if not multi:
			self.read_condition = driver.read_condition[lock_key]
			self.read_counter = driver.read_counter[lock_key]
			self.write_lock = driver.write_lock[lock_key]
		else:
			self.read_condition = MultiLock(*[driver.read_condition[_lock_key_item] for _lock_key_item in lock_key])
			self.read_counter = MultiInt(*[driver.read_counter[_lock_key_item] for _lock_key_item in lock_key])
			self.write_lock = MultiLock(*[driver.write_lock[_lock_key_item] for _lock_key_item in lock_key])
	
	@locked_ro
	def __len__(self):
		"Return number of entries in the resource."
		raise NotImplementedError(f"`__len__` method not implemented on {self.__class__.__name__}")
	
	@locked_ro
	def __contains__(self, key):
		"Check if the key is present in the resource."
		raise NotImplementedError(f"`__contains__` method not implemented on {self.__class__.__name__}")
	
	@locked_ro
	def keys(self):
		"Iter over all possible keys of the resource."
		raise NotImplementedError(f"`keys` method not implemented on {self.__class__.__name__}")
	
	@locked_ro
	def values(self):
		"Iter over all values in the resource."
		raise NotImplementedError(f"`values` method not implemented on {self.__class__.__name__}")
	
	@locked_ro
	def items(self):
		"Iter over (key, value) pairs in the resource."
		raise NotImplementedError(f"`items` method not implemented on {self.__class__.__name__}")
	
	@locked_rw
	def clear(self):
		"Remove all content from the resource."
		raise NotImplementedError(f"`clear` method not implemented on {self.__class__.__name__}")
	
	@locked_rw
	def update(self, values):
		"Update the resource with values from the provided dict-like object."
		raise NotImplementedError(f"`update` method not implemented on {self.__class__.__name__}")
	
	@locked_ro
	def __getitem__(self, key):
		"Return the value associated with the given key, or yield values corresponding to the given slice."
		raise NotImplementedError(f"`__getitem__` method not implemented on {self.__class__.__name__}")
	
	@locked_rw
	def __setitem__(self, key, value):
		"Add/update the value associated with the given key, or add/replace values corresponding to the given slice."
		raise NotImplementedError(f"`__setitem__` method not implemented on {self.__class__.__name__}")
	
	@locked_rw
	def __delitem__(self, key):
		"Delete the value associated with the given key, or all values corresponding to the given slice."
		raise NotImplementedError(f"`__delitem__` method not implemented on {self.__class__.__name__}")


if __debug__ and __name__ == '__main__':
	from multiprocessing import Manager, Process
	from time import sleep
	
	manager = Manager()
	arbitrator = Arbitrator(manager)
	arbitrator.prepare_namespace()
	driver = Driver(arbitrator)
	
	class SlowDict(Accessor):
		def __init__(self, driver, name):
			super().__init__(driver, name)
			self.data = {}
		
		@locked_ro
		def __getitem__(self, key):
			print("{ __getitem__", key)
			sleep(0.1)
			result = self.data[key]
			print("} __getitem__")
			return result
		
		@locked_rw
		def __setitem__(self, key, value):
			print("{ __setitem__", key, value)
			sleep(0.2)
			self.data[key] = value
			print("} __setitem__")
	
	class SlowMultiDict(Accessor):
		def __init__(self, driver, names):
			super().__init__(driver, names, True)
			self.data = {}
		
		@locked_ro
		def __getitem__(self, key):
			print("{ __getitem__", key)
			sleep(0.1)
			result = self.data[key]
			print("} __getitem__")
			return result
		
		@locked_rw
		def __setitem__(self, key, value):
			print("{ __setitem__", key, value)
			sleep(0.2)
			self.data[key] = value
			print("} __setitem__")
	
	def test_mp(l):
		x = SlowDict(driver, 'x')
		y = SlowDict(driver, 'y')
		xy = SlowMultiDict(driver, ['x', 'y'])
		
		for n in range(l):
			x[n] = n
			print(x[n])
		for n in range(l):
			y[n] = n
			print(y[n])
		for n in range(l):
			xy[n] = n
			print(xy[n])
		
		for n in range(l):
			x[n] = n + 1
			y[n] = n + 1
			xy[n] = n + 1
			for m in range(l):
				print(x[m])
			for m in range(l):
				print(y[m])
			for m in range(l):
				print(xy[m])
	
	processes = [Process(target=test_mp, args=(_n,)) for _n in range(5)]
	for process in processes:
		process.start()
	for process in processes:
		process.join()





