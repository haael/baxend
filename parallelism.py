#!/usr/bin/python3.11
#-*- coding: utf-8 -*-


from logging import getLogger, basicConfig, DEBUG
log = getLogger(__name__)

if __name__ == '__main__':
	basicConfig(level=DEBUG, format='%(asctime)-8s %(levelname)-8s %(name)-32s %(message)s')
	
	import warnings
	warnings.filterwarnings('ignore')


__all__ = 'task_bg', 'parallel', 'task_fg', 'rt_sum', 'rt_product'


from enum import Enum
from multiprocessing import Process, Event, SimpleQueue
from itertools import product


def task_bg(old_function):
	def new_function(*args, **kwargs):
		return BackgroundTask(old_function, args, kwargs)
	new_function.__name__ = old_function.__name__
	return new_function


class BackgroundTask:	
	def __init__(self, runnable, args, kwargs):
		self.runnable = runnable
		self.args = args
		self.kwargs = kwargs
		self.queue = SimpleQueue()
		self.exit_event = Event()
		self.process = Process(target=self.__producer)
		self.process.start()
	
	def __producer(self):
		try:
			result = self.runnable(*self.args, **self.kwargs)
		except Exception as error:
			log.error(f"Error in background task: {repr(error)}")
			log.error(str(error))
			result = None
			raise
		finally:
			self.queue.put(result)
			self.queue.close()
	
	def __call__(self):
		return self.queue.get()
		self.process.join()
	
	def kill(self):
		self.exit_event.set()
		if not self.queue.empty():
			self.queue.get_nowait()
		self.process.join()


def parallel(*runnables):
	return list(_runnable() for _runnable in runnables)


def task_fg(old_generator):
	def new_generator(*args, **kwargs):
		return ForegroundTask(old_generator, args, kwargs)
	new_generator.__name__ = old_generator.__name__
	return new_generator


class ForegroundTask:
	Sentinel = Enum('ForegroundTask.Sentinel', 'BEGIN END')
	
	def __init__(self, iterable, args, kwargs):
		self.iterable = iterable
		self.args = args
		self.kwargs = kwargs
		self.queue = SimpleQueue()
		self.exit_event = Event()
		self.process = Process(target=self.__producer)
		self.process.start()
	
	def __producer(self):
		try:
			for item in self.iterable(*self.args, **self.kwargs):
				if self.exit_event.is_set():
					break
				self.queue.put(item)
		except Exception as error:
			log.error(f"Error in foreground task: {repr(error)}")
			log.error(str(error))
			raise
		finally:
			self.queue.put(self.Sentinel.END)
			self.queue.close()
	
	def __iter__(self):
		item = self.Sentinel.BEGIN
		while item != self.Sentinel.END:
			try:
				item = self.queue.get()
			except ValueError:
				break
			else:
				if item != self.Sentinel.END:
					yield item
		self.process.join()
	
	def kill(self):
		self.exit_event.set()
		if not self.queue.empty():
			self.queue.get()
		self.process.join()


def rt_sum(*collections):
	"Pull data from all the iterables and yield as one iterable in random order."
	
	iterators = [iter(_v) if not hasattr(_v, '__next__') else _v for _v in collections]
	active = [True for _n in range(len(iterators))]
	
	while any(active):
		for n, iterator in enumerate(iterators):
			if not active[n]: continue
			
			try:
				yield next(iterator)
			except StopIteration:
				active[n] = False


def rt_product(*collections):
	"Pull data from all the iterables and yield tuples of all combinations."
		
	iterators = [iter(_v) if not hasattr(_v, '__next__') else _v for _v in collections]
	values = [[] for _n in range(len(iterators))]
	active = [True for _n in range(len(iterators))]
	
	while any(active):
		for n, iterator in enumerate(iterators):
			if not active[n]: continue
			
			try:
				value = next(iterator)
			except StopIteration:
				active[n] = False
				if not values[n]:
					for m, iterator in enumerate(iterators):
						if active[m]:
							try:
								iterator.kill()
							except AttributeError:
								pass
					return
			else:
				values[n].append(value)
		
		for selection in product(*[[True, False] if a else [False] for a in active]):
			if not any(selection): continue
			
			round_values = []
			for n, value in enumerate(values):
				if selection[n]:
					round_values.append([value[-1]])
				elif active[n]:
					round_values.append(value[:-1])
				else:
					round_values.append(value)
			
			yield from product(*round_values)


if __debug__ and __name__ == '__main__':
	from time import sleep
	
	@task_bg
	def one(n):
		sleep(1)
		if n == 1: raise ValueError("Error thrown in background task.")
		return n
	
	@task_bg
	def two(n):
		return parallel(*[one(m) for m in range(n)])
	
	@task_bg
	def three(n):
		return parallel(*[two(m) for m in range(n)])
	
	print(three(10)())
	
	@task_fg
	def one(n):
		sleep(1)
		if n == 1: raise ValueError("Error thrown in foreground task (sum mode).")
		yield n
	
	@task_fg
	def two(n):
		yield from rt_sum(*[one(m) for m in range(n)])
	
	def three(n):
		yield from rt_sum(*[two(m) for m in range(n)])
	
	print(list(three(10)))
	
	@task_fg
	def one(n):
		sleep(1)
		if n == 1: raise ValueError("Error thrown in foreground task (product mode).")
		yield n
	
	@task_fg
	def two(n):
		yield from rt_sum(*[one(m) for m in range(n)])
	
	def three(n):
		yield from rt_product(*[two(n) for m in range(n)])
	
	print(list(three(3)))




