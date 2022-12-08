#!/usr/bin/python3.11
#-*- coding: utf-8 -*-


from logging import getLogger, basicConfig, DEBUG, INFO, WARNING, ERROR
log = getLogger(__name__)

if __name__ == '__main__':
	basicConfig(level=WARNING, format='%(asctime)-8s %(levelname)-8s %(name)-32s %(message)s')
	
	import warnings
	warnings.filterwarnings('ignore')


__all__ = 'Database', 'Table'


from basex import Session as BaseXSession, BaseXQueryError
from locking import locked_ro, locked_rw, MultiLock, Driver, Accessor
from collections import defaultdict
from itertools import chain
from enum import Enum


class Database(BaseXSession, Driver):
	class Queries(defaultdict):
		def __init__(self, database):
			super().__init__()
			self.database = database
		
		def __missing__(self, query_str):
			query = self.database.query(query_str)
			query.open()
			super().__setitem__(query_str, query)
			return query
		
		__setitem__ = None
		
		def __delitem__(self, query_str):
			if query_str == Ellipsis:
				for qs in self.keys():
					del self[qs]
			elif query_str in self:
				query = super().__getitem__(query_str)
				if query.is_open():
					query.close()
				super().__delitem__(query_str)
	
	def __init__(self, arbitrator, host, port, user, password, database_name, xmlns={}):
		Driver.__init__(self, arbitrator)
		BaseXSession.__init__(self, user, password, (host, port))
		self.database_name = database_name
		self.xmlns = {}
	
	def __enter__(self):
		super().__enter__()
		self.execute.check(self.database_name)
		self.queries = self.Queries(self)
		return self
	
	def __exit__(self, *args):
		if all(_arg == None for _arg in args):
			for query in self.queries.values():
				if query.is_open():
					query.close()
			self.execute.close()
		super().__exit__(*args)
	
	def keys(self):
		return [_line.split(' ')[0] for _line in self.execute.dir_().split('\n')[2:-3]]
	
	def values(self):
		for path in self.keys():
			yield self[path]
	
	def items(self):
		for path in self.keys():
			yield path, self[path]
	
	def __contains__(self, path):
		return path in self.keys()
	
	def __getitem__(self, path):
		try:
			return self.execute.get(path)
		except BaseXCommandError:
			raise KeyError(path)
	
	def __setitem__(self, path, content):
		self.put(path, content)
	
	def __delitem__(self, path):
		try:
			self.execute.delete(path)
		except BaseXCommandError:
			raise KeyError(path)
	
	def doc(self, document, xmlns={}):
		table_xmlns = {}
		table_xmlns.update(self.xmlns)
		table_xmlns.update(xmlns)
		return Table(self, document, (), (), table_xmlns)


class Table(Accessor):
	def __init__(self, database, document, expression_chain, selector_chain, xmlns):
		super().__init__(database, document, isinstance(document, frozenset))
		self.database = database
		self.document = document
		self.expression_chain = expression_chain
		self.selector_chain = selector_chain
		self.xmlns = xmlns
		self.__query_string_cache = {}
	
	def __truediv__(self, path_element):
		"Expression building helper. Extend the path by one element."
		expr_chain = self.expression_chain
		if not expr_chain or expr_chain[-1][1] != None:
			expr_chain = expr_chain + ((path_element, None, None),)
		else:
			expr_chain = expr_chain[:-1] + ((expr_chain[-1][0] + '/' + path_element, None, None),)
		return self.__class__(self.database, self.document, expr_chain, self.selector_chain, self.xmlns)
	
	def __matmul__(self, keys_spec):
		"Expression building helper. Provide keys specification."
		expr_chain = self.expression_chain
		if not expr_chain or expr_chain[-1][1] != None:
			raise ValueError("Provide a path element before specifying keys.")
		else:
			expr_chain = expr_chain[:-1] + ((expr_chain[-1][0], tuple(keys_spec), expr_chain[-1][2]),)
		return self.__class__(self.database, self.document, expr_chain, self.selector_chain, self.xmlns)
	
	def __mod__(self, filter_spec):
		"Expression building helper. Apply a filter on the results."
		expr_chain = self.expression_chain
		if not expr_chain or expr_chain[-1][1] != None:
			raise ValueError("Provide path element first.")
		elif expr_chain[-1][2] == None:
			expr_chain = expr_chain[:-1] + ((expr_chain[-1][0], expr_chain[-1][1], filter_spec),)
		else:
			expr_chain = expr_chain[:-1] + ((expr_chain[-1][0], expr_chain[-1][1], f'{expr_chain[-1][2]} and ({filter_spec})'),)
		return self.__class__(self.database, self.document, expr_chain, self.selector_chain, self.xmlns)
	
	def __mul__(self, other):
		"Expression building helper. Create cartesian product of the expressions."
		
		if self.database is not other.database:
			raise ValueError
		
		self_is_product = self.expression_chain and isinstance(self.expression_chain[-1][0], tuple) and self.expression_chain[-1][1] == None and self.expression_chain[-1][2] == None
		other_is_product = other.expression_chain and isinstance(other.expression_chain[-1][0], tuple) and other.expression_chain[-1][1] == None and other.expression_chain[-1][2] == None
		
		if not self_is_product and not other_is_product:
			expr_chain = (((self[...], other[...]), None, None),)
		elif self_is_product and not other_is_product:
			expr_chain = ((self.expression_chain[-1][0] + (other[...],), None, None),)
		elif not self_is_product and other_is_product:
			expr_chain = (((self[...],) + other.expression_chain[-1][0], None, None),)
		elif self_is_product and other_is_product:
			expr_chain = ((self.expression_chain[-1][0] + other.expression_chain[-1][0], None, None),)
		
		if len(expr_chain[0][0]) > 10:
			raise ValueError("Only up to 10-fold cartesian products are supported.")
		
		if not isinstance(self.document, frozenset) and not isinstance(other.document, frozenset):
			documents = frozenset([self.document, other.document])
		elif not isinstance(self.document, frozenset) and isinstance(other.document, frozenset):
			documents = frozenset([self.document]) | other.document
		elif isinstance(self.document, frozenset) and not isinstance(other.document, frozenset):
			documents = self.document | frozenset([other.document])
		elif isinstance(self.document, frozenset) and isinstance(other.document, frozenset):
			documents = self.document | other.document
		
		return self.__class__(self.database, documents, expr_chain, (), self.xmlns)
	
	def __getitem__(self, keys_values):
		if keys_values == Ellipsis:
			pass
		elif not isinstance(keys_values, tuple):
			keys_values = keys_values,
		else:
			keys_values = tuple(keys_values)
		# TODO: type checks
		
		if len(self.selector_chain) > len(self.expression_chain):
			raise TypeError("End of chain reached.")
		
		sel_chain = self.selector_chain + (keys_values,)
		return self.__class__(self.database, self.document, self.expression_chain, sel_chain, self.xmlns)
	
	def __xmlns_decls(self):
		for prefix, namespace in self.xmlns.items():
			if prefix == '':
				yield f'declare default element namespace "{namespace}";'
			else:
				yield f'declare namespace {prefix} = "{namespace}";'
	
	def __var_decls(self, level=''):
		for m, vals in enumerate(self.selector_chain):
			if vals == Ellipsis:
				continue
			for n, val in enumerate(vals):
				if not isinstance(val, slice):
					yield f'declare variable $key{level}_{m}_{n} external;'
				else:
					if val.start != None:
						yield f'declare variable $key{level}_{m}_{n}_low external;'
					if val.stop != None:
						yield f'declare variable $key{level}_{m}_{n}_high external;'
		
		if self.expression_chain and isinstance(self.expression_chain[0][0], tuple):
			for k, subpath in enumerate(self.expression_chain[0][0]):
				yield from subpath.__var_decls(level=self.__digits[k] + level)
	
	__numerals = 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten'
	__digits = '1234567890'
	
	def __query_expr(self, selector=None, level=''):
		p = f'doc("{self.database.database_name}/{self.document}")'
		s = ''
		l = 0
		for m, ((path, keys, filter_), vals) in enumerate(zip(self.expression_chain, self.selector_chain if selector == None else chain(self.selector_chain, [None]))):
			if vals == None:
				keyspec = selector
			elif isinstance(vals, tuple):
				keyspecs = []
				for n, (key, val) in enumerate(zip(keys, vals)):
					if not isinstance(val, slice):
						keyspecs.append(f'{key}=$key{level}_{m}_{n}')
					else:
						if val.start != None:
							keyspecs.append(f'{key}>=$key{level}_{m}_{n}_low')
						if val.stop != None:
							keyspecs.append(f'{key}<$key{level}_{m}_{n}_high')
				
				keyspec = '[' + ' and '.join(keyspecs) +']'
			elif vals == Ellipsis:
				keyspec = ''
			else:
				raise RuntimeError
			
			if isinstance(path, tuple):
				assert m == 0
				assert len(path) <= 10
				
				sp = []
				for k, subpath in enumerate(path):
					sp.append(f'let ${self.__numerals[k]}{level} :=\n{subpath.__query_expr(selector="", level=self.__digits[k] + level)}\n')
				
				for k in range(len(path)):
					sp.append(f'{("let $this :=" + chr(10) + "for" if k == 0 else "   ")} ${self.__numerals[k]} in ${self.__numerals[k]}{level}{("," if k != len(path) - 1 else "")}\n')
				
				if filter_:
					sp.append(f' where {filter_}\n')
				
				sp.append(f' return <tuple xmlns="https://github.com/haael/baxend">{("".join(f"{{${self.__numerals[_k]}}}" for _k in range(len(path))))}</tuple>\n')
				
				s = ''.join(sp)
				p = f'$this{keyspec}'
			elif not filter_:
				p = f'{p}/{path}{keyspec}'
			else:
				s = f'{s}{" "*l}for $this in {p}/{path}\n{" "*l} where {filter_}\n'
				p = f'$this{keyspec}'
				l += 1
		
		if s:
			return f'{s}{" "*l}return {p}'
		else:
			return p
	
	__Mode = Enum('Table._Table__Mode', 'GETITEM KEYS')
	
	def __query_string(self, mode):
		try:
			return self.__query_string_cache[mode]
		except KeyError:
			pass
		
		if mode == self.__Mode.GETITEM:
			selector = None
		elif mode == self.__Mode.KEYS:
			keys = self.expression_chain[len(self.selector_chain)][1]
			selector = '/(' + ','.join(keys) + ')'
		else:
			raise NotImplementedError(f"Unsupported mode: {mode}.")
		
		result = '\n'.join(chain(self.__xmlns_decls(), self.__var_decls(), [self.__query_expr(selector)]))
		self.__query_string_cache[mode] = result
		return result
	
	def __apply_keys(self, query, level=''):
		for m, ((path, keys, filter_), vals) in enumerate(zip(self.expression_chain, self.selector_chain)):
			if isinstance(vals, tuple):
				for n, (key, val) in enumerate(zip(keys, vals)):
					if not isinstance(val, slice):
						query.bind(f'$key{level}_{m}_{n}', val, 'xs:string')
					else:
						if val.start != None:
							query.bind(f'$key{level}_{m}_{n}_low', val.start, 'xs:string')
						if val.stop != None:
							query.bind(f'$key{level}_{m}_{n}_high', val.stop, 'xs:string')
			elif vals == Ellipsis:
				pass
			else:
				raise RuntimeError
		
		if self.expression_chain and isinstance(self.expression_chain[0][0], tuple):
			for k, subpath in enumerate(self.expression_chain[0][0]):
				subpath.__apply_keys(query, level=self.__digits[k] + level)
	
	@locked_ro
	def __str__(self):
		query_str = self.__query_string(self.__Mode.GETITEM)		
		query = self.database.queries[query_str]
		self.__apply_keys(query)
		return query.execute()
	
	@locked_ro
	def __iter__(self):
		query_str = self.__query_string(self.__Mode.GETITEM)		
		query = self.database.queries[query_str]
		self.__apply_keys(query)
		for typeid, item in query.results():
			yield item
	
	def __len__(self):
		return len(list(self.keys()))
	
	def __contains__(self, key_values):
		return key_values in frozenset(self.keys())
	
	@locked_ro
	def keys(self):
		r = []
		l = len(self.expression_chain[len(self.selector_chain)][1])
		query_str = self.__query_string(self.__Mode.KEYS)
		query = self.database.queries[query_str]
		self.__apply_keys(query)
		for typeid, item in query.results():
			if typeid == 'attribute()':
				item = item.split('=')[1][1:-1]
			r.append(item)
			if len(r) == l:
				if l == 1:
					yield r[0]
				else:
					yield tuple(r)
				r.clear()
		assert not r
	
	def values(self):
		for key in list(self.keys()):
			yield self[key]
	
	def items(self):
		for key in list(self.keys()):
			yield key, self[key]
	
	@locked_rw
	def clear(self):
		raise NotImplementedError
	
	@locked_rw
	def update(self, values):
		raise NotImplementedError
	
	@locked_rw
	def __setitem__(self, key_values, values):
		raise NotImplementedError
	
	@locked_rw
	def __delitem__(self, key_values):
		raise NotImplementedError


if __debug__ and __name__ == '__main__':
	from multiprocessing import Manager
	from locking import Arbitrator
	from time import clock_gettime_ns, CLOCK_MONOTONIC
	#from pycallgraph2 import PyCallGraph
	#from pycallgraph2.output import GraphvizOutput
	
	def get_time():
		return clock_gettime_ns(CLOCK_MONOTONIC)
	
	arbitrator = Arbitrator(Manager())
	arbitrator.prepare_namespace()
	
	database = Database(arbitrator, '::1', 1984, 'db_user', 'wemn2o03289', 'baxend_test')
	database.xmlns['baxend'] = 'https://github.com/haael/baxend'
	table1 = database.doc('one.xml') / 'baxend:root' / 'baxend:one' % 'string-length($this/baxend:descr/text()) < 15' @ ['baxend:title/text()'] / 'baxend:two' % '$this/@x < $this/@y' @ ['@x', '@y']
	table2 = database.doc('two.xml') / 'baxend:root' / 'baxend:one' % 'string-length($this/baxend:descr/text()) < 20' @ ['baxend:title/text()'] / 'baxend:two' @ ['@y', '@z']
	table3 = table1[...] * table2["Fifth"] % '$one/@y = $two/@y' @ ['baxend:two[1]/@x', 'baxend:two[2]/@z']
	
	#profiler = PyCallGraph(output=GraphvizOutput(output_file='database.png'))
	#profiler.start()
	
	with database:
		database['one.xml'] = '''
			<root xmlns="https://github.com/haael/baxend">
				<one>
					<title>First</title>
					<descr>First entry</descr>
					<two x="1" y="2">A</two>
					<two x="1" y="3">B</two>
					<two x="2" y="2">C</two><!-- invisible -->
					<two x="2" y="3">D</two>
				</one>
				<one>
					<title>Second</title>
					<descr>Second entry</descr>
					<two x="10" y="20">E</two>
					<two x="10" y="30">F</two>
					<two x="20" y="20">G</two><!-- invisible -->
					<two x="20" y="30">H</two>
				</one>
				<one>
					<title>Third</title>
					<descr>This entry will not be seen, because the description is too long.</descr>
					<two x="100" y="200">I</two>
					<two x="100" y="300">J</two>
				</one>
			</root>
		'''
		
		database['two.xml'] = '''
			<root xmlns="https://github.com/haael/baxend">
				<one>
					<title>Four</title>
					<descr>Fourth entry</descr>
					<two y="2" z="1">A</two>
					<two y="3" z="2">B</two>
					<two y="2" z="3">C</two><!-- invisible -->
					<two y="3" z="4">D</two>
				</one>
				<one>
					<title>Fifth</title>
					<descr>Fifth entry</descr>
					<two y="20" z="5">E</two>
					<two y="30" z="6">F</two>
					<two y="2" z="7">G</two><!-- invisible -->
					<two y="30" z="8">H</two>
				</one>
				<one>
					<title>Sixth</title>
					<descr>This entry will not be seen, because the description is too long.</descr>
					<two y="200" z="9">I</two>
					<two y="300" z="10">J</two>
				</one>
			</root>
		'''
		
		perf_iters = 1000
		
		try:
			print()
			print(1)
			print(list(table1.keys()))
			print(list(table1["First"].keys()))
			print(table1["First"]['1', '2'])
			print("len:", len(table1["First"]))
			for value in table1["First"][...]:
				print(" ", value)
			
			tb = get_time()
			for n in range(perf_iters):
				x = str(table1["Second"][10, 20])
			te = get_time()
			print("amortized time:", int((te - tb) / 10**6)  / perf_iters, "ms")
		except BaseXQueryError as error:
			print(error)
		
		try:
			print()
			print(2)
			print(table2["Fifth"])
			
			tb = get_time()
			for n in range(perf_iters):
				x = str(table2["Fifth"][20, 50])
			te = get_time()
			print("amortized time:", int((te - tb) / 10**6) / perf_iters, "ms")
		except BaseXQueryError as error:
			print(error)
		
		try:
			print()
			print(3)
			print(list(table3.keys()))
			for item in table3[...]:
				print(" ", item)
			print(table3[10, 5])
			
			tb = get_time()
			for n in range(perf_iters):
				x = str(table3[20, 6])
			te = get_time()
			print("amortized time:", int((te - tb) / 10**6) / perf_iters, "ms")
		except BaseXQueryError as error:
			print(error)
		
		del database['one.xml']
		del database['two.xml']
	
	#profiler.done()
	
