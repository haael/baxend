#!/usr/bin/python3.11
#-*- coding: utf-8 -*-


from logging import getLogger, basicConfig, DEBUG, INFO, WARNING, ERROR
log = getLogger(__name__)

if __name__ == '__main__':
	basicConfig(level=DEBUG, format='%(asctime)-8s %(levelname)-8s %(name)-32s %(message)s')
	
	import warnings
	warnings.filterwarnings('ignore')


__all__ = 'Database', 'Table'


from collections import defaultdict
from itertools import chain
from enum import Enum

if __name__ == '__main__':
	from basex import Session as BaseXSession, BaseXQueryError
	from locking import locked_ro, locked_rw, MultiLock, Driver, Accessor
	from xmltype import XMLType, XMLText, XMLAttribute
else:
	from .basex import Session as BaseXSession, BaseXQueryError
	from .locking import locked_ro, locked_rw, MultiLock, Driver, Accessor
	from .xmltype import XMLType, XMLText, XMLAttribute


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
	
	def __init__(self, arbitrator, host, port, user, password, database_name, xmlns={}, xml_pfx={}):
		Driver.__init__(self, arbitrator)
		BaseXSession.__init__(self, user, password, (host, port))
		self.database_name = database_name
		self.xmlns = dict(xmlns)
		self.xml_pfx = dict(xml_pfx)
		try:
			del self.xmlns['xml']
		except KeyError:
			pass
	
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
		path_lines = self.execute.list_(self.database_name).split('\n')[:-3]
		lim = path_lines[0].index('Type')
		result = []
		for line in path_lines[2:]:
			result.append(line[:lim].rstrip())
		return result
	
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
	
	def doc(self, document, xmlns={}, xml_pfx={}):
		table_xmlns = {}
		table_xmlns.update(self.xmlns)
		table_xmlns.update(xmlns)
		
		table_xml_pfx = {}
		table_xml_pfx.update(self.xml_pfx)
		table_xml_pfx.update(xml_pfx)
		
		return Table(self, document, (), (), {}, table_xmlns, table_xml_pfx)
	
	@staticmethod
	def xml_convert(py_value, xml_pfx):
		if isinstance(py_value, bool):
			return ('true' if py_value else 'false'), 'xs:boolean'
		elif isinstance(py_value, int):
			return str(py_value), 'xs:int'
		elif isinstance(py_value, float):
			return str(py_value), 'xs:double'
		elif isinstance(py_value, str):
			return py_value, 'xs:string'
		elif isinstance(py_value, XMLType):
			return py_value.render(xml_pfx=xml_pfx), 'element()'
		elif isinstance(py_value, XMLText):
			return str(py_value), 'text()'
		else:
			raise TypeError(f"Can't convert Python value {repr(py_value)} of type {type(py_value).__name__} to XML type.")
	
	@staticmethod
	def py_convert(type_name, xml_value):
		if type_name in ['xs:integer', 'xs:long', 'xs:int', 'xs:short', 'xs:byte', 'xs:nonNegativeInteger', 'xs:unsignedLong', 'xs:unsignedInt', 'xs:unsignedShort', 'xs:unsignedByte', 'xs:positiveInteger']:
			return int(xml_value)
		elif type_name in ['xs:float', 'xs:double', 'xs:decimal', 'xs:precisionDecimal']:
			return float(xml_value)
		elif type_name == 'xs:string':
			return xml_value
		elif type_name == 'element()' or type_name == 'document-node(element())':
			return XMLType(xml_value, None)
		elif type_name == 'text()':
			return XMLText(xml_value)
		elif type_name == 'xs:boolean':
			return True if (xml_value in ['true', '1']) else False
		else:
			raise TypeError(f"Can't convert XML type {type_name} to Python type.")


class Table(Accessor):
	def __init__(self, database, document, expression_chain, selector_chain, bound_variables, xmlns, xml_pfx):
		super().__init__(database, document, isinstance(document, frozenset))
		self.database = database
		self.document = document
		self.expression_chain = expression_chain
		self.selector_chain = selector_chain
		self.bound_variables = bound_variables
		self.xmlns = xmlns
		self.xml_pfx = xml_pfx
		self.__query_string_cache = {}
	
	def __truediv__(self, path_element):
		"Expression building helper. Extend the path by one element."
		expr_chain = self.expression_chain
		if not expr_chain or expr_chain[-1][1] != None:
			expr_chain = expr_chain + (((path_element,), None, None),)
		else:
			expr_chain = expr_chain[:-1] + ((expr_chain[-1][0] + (path_element,), None, None),)
		return self.__class__(self.database, self.document, expr_chain, self.selector_chain, self.bound_variables, self.xmlns, self.xml_pfx)
	
	def __matmul__(self, keys_spec):
		"Expression building helper. Provide keys specification."
		expr_chain = self.expression_chain
		if not expr_chain or expr_chain[-1][1] != None:
			raise ValueError("Provide a path element before specifying keys.")
		else:
			if not keys_spec:
				keys_spec = [None]
			expr_chain = expr_chain[:-1] + ((expr_chain[-1][0], tuple(keys_spec), expr_chain[-1][2]),)
		return self.__class__(self.database, self.document, expr_chain, self.selector_chain, self.bound_variables, self.xmlns, self.xml_pfx)
	
	def __floordiv__(self, values):
		bound_variables = dict(self.bound_variables)
		bound_variables.update(values)
		return self.__class__(self.database, self.document, self.expression_chain, self.selector_chain, bound_variables, self.xmlns, self.xml_pfx)
	
	def __mod__(self, filter_spec):
		"Expression building helper. Apply a filter on the results."
		expr_chain = self.expression_chain
		if not expr_chain or expr_chain[-1][1] != None:
			raise ValueError("Provide path element first.")
		elif expr_chain[-1][2] == None:
			expr_chain = expr_chain[:-1] + ((expr_chain[-1][0], expr_chain[-1][1], (filter_spec,)),)
		else:
			expr_chain = expr_chain[:-1] + ((expr_chain[-1][0], expr_chain[-1][1], expr_chain[-1][2] + (filter_spec,)),)
		return self.__class__(self.database, self.document, expr_chain, self.selector_chain, self.bound_variables, self.xmlns, self.xml_pfx)
	
	def __mul__(self, other):
		"Expression building helper. Create cartesian product of the expressions."
		
		if self.database is not other.database:
			raise ValueError
		
		self_is_product = self.expression_chain and self.expression_chain[-1][1] == None and self.expression_chain[-1][2] == None and all(isinstance(_subpath, Table) for _subpath in self.expression_chain[-1][0])
		other_is_product = other.expression_chain and other.expression_chain[-1][1] == None and other.expression_chain[-1][2] == None and all(isinstance(_subpath, Table) for _subpath in other.expression_chain[-1][0])
		
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
		
		bound_variables = dict()
		bound_variables.update(self.bound_variables)
		bound_variables.update(other.bound_variables)
		return self.__class__(self.database, documents, expr_chain, (), bound_variables, self.xmlns, self.xml_pfx)
	
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
		
		if self.expression_chain and all(isinstance(_subpath, Table) for _subpath in self.expression_chain[0][0]):
			for k, subpath in enumerate(self.expression_chain[0][0]):
				yield from subpath.__var_decls(level=self.__digits[k] + level)
	
	def __bound_var_decls(self):
		for varname in self.bound_variables.keys():
			yield f'declare variable ${varname} external;'
	
	def __apply_bound_variables(self, query):
		for varname, value in self.bound_variables.items():
			query.bind('$' + varname, *self.database.xml_convert(value, self.xml_pfx))
	
	__numerals = 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten'
	__digits = '1234567890'
	
	def __query_expr(self, modifier=None, level=''):
		p = f'doc("{self.database.database_name}/{self.document}")'
		s = ''
		l = 0
		for m, ((path, keys, filter_), vals) in enumerate(zip(self.expression_chain, self.selector_chain)):			
			if isinstance(vals, tuple):
				keyspecs = []
				for n, (key, val) in enumerate(zip(keys, vals)):
					if not isinstance(val, slice):
						if key != None:
							keyspecs.append(f'{key}=$key{level}_{m}_{n}')
						else:
							keyspecs.append(f'$key{level}_{m}_{n}')
					else:
						if key != None:
							if val.start != None:
								keyspecs.append(f'{key}>=$key{level}_{m}_{n}_low')
							if val.stop != None:
								keyspecs.append(f'{key}<$key{level}_{m}_{n}_high')
						else:
							if val.start != None:
								keyspecs.append(f'position()>=$key{level}_{m}_{n}_low')
							if val.stop != None:
								keyspecs.append(f'position()<$key{level}_{m}_{n}_high')
				
				keyspec = '[' + ' and '.join(keyspecs) +']'
			elif vals == Ellipsis:
				keyspec = ''
			else:
				raise RuntimeError
			
			filter_ = ' and '.join([f'({_filter_spec})' for _filter_spec in filter_]) if filter_ != None else ''
			
			if all(isinstance(_subpath, Table) for _subpath in path):
				assert m == 0
				assert len(path) <= 10
				
				sp = []
				for k, subpath in enumerate(path):
					sp.append(f'let ${self.__numerals[k]}{level} :=\n{subpath.__query_expr(modifier=None, level=self.__digits[k] + level)}\n')
				
				for k in range(len(path)):
					sp.append(f'{("let $this :=" + chr(10) + "for" if k == 0 else "   ")} ${self.__numerals[k]} in ${self.__numerals[k]}{level}{("," if k != len(path) - 1 else "")}\n')
				
				if filter_:
					sp.append(f' where {filter_}\n')
				
				ts = "".join(f"{{${self.__numerals[_k]}}}" for _k in range(len(path)))
				sp.append(f' return <tuple xmlns="https://github.com/haael/baxend">{ts}</tuple>\n')
				
				s = ''.join(sp)
				p = f'$this{keyspec}'
			elif not filter_:
				path = '/'.join(path)
				p = f'{p}/{path}{keyspec}'
			else:
				path = '/'.join(path)
				s = f'{s}{" "*l}for $this in {p}/{path}\n{" "*l} where {filter_}\n'
				p = f'$this{keyspec}'
				l += 1
		
		if modifier:
			p = modifier(p)
		
		if s:
			return f'{s}{" "*l}return {p}'
		else:
			return p
	
	__Mode = Enum('Table._Table__Mode', 'GET COUNT INSERT DELETE KEYS GETATTR SETATTR')
	
	def __query_string(self, mode):
		try:
			return self.__query_string_cache[mode]
		except KeyError:
			pass
		
		if mode == self.__Mode.GET:
			modifier = None
		elif mode == self.__Mode.GETATTR:
			modifier = lambda p: f'( let $element := {p} return if(empty($element)) then () else element {{fn:node-name($element)}} {{$element/@*}} )'
		elif mode == self.__Mode.SETATTR:
			#raise NotImplementedError
			modifier = lambda p: f'( let $element := {p} return if(empty($element)) then () else replace node $element with element {{fn:node-name($inserted)}} {{ $inserted/@*, $element/* }} )'
		elif mode == self.__Mode.COUNT:
			modifier = lambda p: f'count({p})'
		elif mode == self.__Mode.KEYS:
			keys = ', '.join(self.expression_chain[len(self.selector_chain) - 1][1])
			modifier = lambda p: f'{p}/({keys})'
		elif mode == self.__Mode.DELETE:
			modifier = lambda p: f'{p}/(delete node ., update:output("deleted"))'
		elif mode == self.__Mode.INSERT:
			path_stem = '/'.join(self.expression_chain[len(self.selector_chain)][0][:-1])
			if path_stem: path_stem = '/' + path_stem
			modifier = lambda p: f'insert node $inserted into {p}{path_stem}'
		else:
			raise NotImplementedError(f"Unsupported mode: {mode}.")
		
		result = '\n'.join(chain(self.__xmlns_decls(), self.__var_decls(), self.__bound_var_decls(), (['declare variable $inserted external;'] if mode in (self.__Mode.INSERT, self.__Mode.SETATTR) else []) + [self.__query_expr(modifier)]))
		self.__query_string_cache[mode] = result
		return result
	
	def __apply_keys(self, query, level=''):
		for m, ((path, keys, filter_), vals) in enumerate(zip(self.expression_chain, self.selector_chain)):
			if isinstance(vals, tuple):
				for n, (key, val) in enumerate(zip(keys, vals)):
					if not isinstance(val, slice):
						query.bind(f'$key{level}_{m}_{n}', *self.database.xml_convert(val, self.xml_pfx))
					else:
						if val.start != None:
							query.bind(f'$key{level}_{m}_{n}_low', *self.database.xml_convert(val.start, self.xml_pfx))
						if val.stop != None:
							query.bind(f'$key{level}_{m}_{n}_high', *self.database.xml_convert(val.stop, self.xml_pfx))
			elif vals == Ellipsis:
				pass
			else:
				raise RuntimeError
		
		if self.expression_chain and all(isinstance(_subpath, Table) for _subpath in self.expression_chain[0][0]):
			for k, subpath in enumerate(self.expression_chain[0][0]):
				subpath.__apply_keys(query, level=self.__digits[k] + level)
	
	def __apply_value(self, query, py_value):
		query.bind('$inserted', *self.database.xml_convert(py_value, self.xml_pfx))
	
	def __is_slice(self):
		for m, ((path, keys, filter_), vals) in enumerate(zip(self.expression_chain, self.selector_chain)):
			if isinstance(vals, tuple):
				for n, (key, val) in enumerate(zip(keys, vals)):
					if isinstance(val, slice):
						return True
			elif vals == Ellipsis:
				return True
			else:
				raise RuntimeError
		
		if self.expression_chain and all(isinstance(_subpath, Table) for _subpath in self.expression_chain[0][0]):
			for subpath in self.expression_chain[0][0]:
				if subpath.__is_slice():
					return True
		
		return False
	
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
		return self.__class__(self.database, self.document, self.expression_chain, sel_chain, self.bound_variables, self.xmlns, self.xml_pfx)
	
	@locked_ro
	def __str__(self):
		query_str = self.__query_string(self.__Mode.GET)
		query = self.database.queries[query_str]
		self.__apply_keys(query)
		self.__apply_bound_variables(query)
		result = query.execute()
		if not result and not self.__is_slice():
			raise KeyError("Query returned no result.")
		return result
	
	@locked_ro
	def get_tags(self):
		query_str = self.__query_string(self.__Mode.GETATTR)
		query = self.database.queries[query_str]
		self.__apply_keys(query)
		self.__apply_bound_variables(query)
		for typeid, item in query.results():
			yield self.database.py_convert(typeid, item)
	
	@property
	def tag(self):
		if self.__is_slice():
			raise ValueError("The query must refer to 1 element.")
		
		over = self.get_tags()
		finished = False
		try:		
			try:
				result = next(over)
			except StopIteration:
				finished = True
				raise KeyError("Empty result (tag).")
			
			try:
				next(over)
			except StopIteration:
				finished = True
			else:
				raise ValueError("Result has more than 1 element (tag).")
			
			return result
		finally:
			if not finished:
				while True:
					try:
						next(over)
					except StopIteration:
						break
	
	@locked_rw
	def set_tags(self, value):
		query_str = self.__query_string(self.__Mode.SETATTR)
		query = self.database.queries[query_str]
		self.__apply_keys(query)
		self.__apply_bound_variables(query)
		self.__apply_value(query, value)
		query.execute()
	
	@tag.setter
	def tag(self, value):
		if self.__is_slice():
			raise ValueError("The query must refer to 1 element.")		
		self.set_tags(value)
	
	@locked_ro
	def __iter__(self):
		query_str = self.__query_string(self.__Mode.GET)		
		query = self.database.queries[query_str]
		self.__apply_keys(query)
		self.__apply_bound_variables(query)
		for typeid, item in query.results():
			yield self.database.py_convert(typeid, item)
	
	def __call__(self):
		over = iter(self)
		finished = False
		try:
			try:
				result = next(over)
			except StopIteration:
				finished = True
				raise KeyError("Empty result (call).")
			
			try:
				next(over)
			except StopIteration:
				finished = True
			else:
				raise ValueError("Result has more than 1 element (call).")
			
			return result
		finally:
			if not finished:
				while True:
					try:
						next(over)
					except StopIteration:
						break
	
	@locked_ro
	def __len__(self):
		if self.__is_slice():
			query_str = self.__query_string(self.__Mode.COUNT)
			query = self.database.queries[query_str]
			self.__apply_keys(query)
			self.__apply_bound_variables(query)
			return sum(int(_l) for _l in query.execute().split('\n') if _l)
		else:
			final = self[...]
			query_str = final.__query_string(self.__Mode.COUNT)
			query = final.database.queries[query_str]
			final.__apply_keys(query)
			final.__apply_bound_variables(query)
			return sum(int(_l) for _l in query.execute().split('\n') if _l)
	
	@locked_ro
	def __contains__(self, keys_values):
		final = self[keys_values]
		query_str = final.__query_string(self.__Mode.COUNT)
		query = final.database.queries[query_str]
		final.__apply_keys(query)
		final.__apply_bound_variables(query)
		return bool(sum(int(_l) for _l in query.execute().split('\n') if _l))
	
	@locked_ro
	def keys(self):
		r = []
		l = len(self.expression_chain[len(self.selector_chain)][1])
		final = self[...]
		query_str = final.__query_string(self.__Mode.KEYS)
		query = final.database.queries[query_str]
		final.__apply_keys(query)
		final.__apply_bound_variables(query)
		for typeid, item in query.results():
			r.append(self.database.py_convert(typeid, item))
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
	
	def clear(self):
		del self[...]
	
	@locked_rw
	def __setitem__(self, keys_values, values):
		final = self[keys_values]
		query_str = final.__query_string(self.__Mode.DELETE)
		query = final.database.queries[query_str]
		final.__apply_keys(query)
		final.__apply_bound_variables(query)
		query.execute()
		
		query_str = self.__query_string(self.__Mode.INSERT)
		query = self.database.queries[query_str]
		if final.__is_slice():
			for value in values:
				self.__apply_keys(query)
				self.__apply_bound_variables(query)
				self.__apply_value(query, value)
				query.execute()
		else:
			self.__apply_keys(query)
			self.__apply_bound_variables(query)
			self.__apply_value(query, values)
			query.execute()
	
	@locked_rw
	def __delitem__(self, keys_values):
		final = self[keys_values]
		query_str = final.__query_string(self.__Mode.DELETE)
		query = final.database.queries[query_str]
		final.__apply_keys(query)
		final.__apply_bound_variables(query)
		result = query.execute() # TODO: check result
		if not result and not self.__is_slice():
			raise KeyError("Query returned no result.")
	
	def get_attr(self, attr, type_):
		raise NotImplementedError(f"get_attr({attr}, {type_})")
	
	def set_attr(self, attr, value):
		raise NotImplementedError(f"set_attr({attr}, {value})")


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
	
	XMLType.xmlns['baxend'] = 'https://github.com/haael/baxend'
	XMLType.xml_pfx['https://github.com/haael/baxend'] = ''
	XMLType.xml_pfx['other'] = 'other'
	
	database = Database(arbitrator, '::1', 1984, 'db_user', 'wemn2o03289', 'baxend_test')
	database.xmlns['baxend'] = 'https://github.com/haael/baxend'
	database.xml_pfx['https://github.com/haael/baxend'] = ''
	
	table1 = database.doc('one.xml') / 'baxend:root' / 'baxend:one' % 'string-length($this/baxend:descr/text()) < 15' @ ['baxend:title/text()'] / 'baxend:two' % 'xs:int($this/@x) < xs:int($this/@y)' @ ['xs:int(@x)', 'xs:int(@y)']
	table2 = database.doc('two.xml') / 'baxend:root' / 'baxend:one' % 'string-length($this/baxend:descr/text()) < 20' @ ['baxend:title/text()'] / 'baxend:two' @ ['xs:int(@y)', 'xs:int(@z)']
	table3 = table1[...] * table2["Fifth"] % 'xs:int($one/@y) = xs:int($two/@y)' @ ['xs:int(baxend:two[1]/@x)', 'xs:int(baxend:two[2]/@z)']
	
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
				<one k="kkk" l="lll">
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

		perf_iters = 1
		
		try:
			print()
			print(1)
			print("table1.keys():", list(table1.keys()))
			print("table1[\"First\"].keys():", list(table1["First"].keys()))
			print(table1["First"][1, 2])
			print("len:", len(table1), len(table1["First"]), len(table1["First":"ZZZ"]))
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
				x = list(table2["Fifth"][20, 50])
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
		
		
		print("1?:", "First" in table1)
		print("2?:", "Second" in table1)
		print("3?:", "Third" in table1)
		try:
			print("3:", table1["Third"])
		except KeyError:
			print("Third not found (correct).")
		
		try:
			print(database['one.xml'])
			print()
			
			print('table1["First"].clear()')
			table1["First"].clear()
			print(database['one.xml'])
			print()
			
			print('table1["Second"].tag')
			second_tag = table1["Second"].tag
			print(second_tag)
			print()
			
			second_tag['@k'] = "ooo"
			print('table1["Second"].tag = ...')
			table1["Second"].tag = second_tag
			print(second_tag)
			print()
			
			print(database['one.xml'])
			
			print('table1["Second"].tag')
			print(table1["Second"].tag)
			print()
			
			print('del table1["Second"][10,3 0]')
			del table1["Second"][10, 30]
			print(database['one.xml'])
			print()
			
			print('del table1["Second"][...]')
			del table1["Second"][...]
			print(database['one.xml'])
			print()
			
			print('del table1["Second"]')
			del table1["Second"]
			print(database['one.xml'])
			print()
			
			print('table1["First"][11, 31] = \'<two xmlns="https://github.com/haael/baxend" x="11" y="31">newly</two>\'')
			table1["First"][11, 31] = XMLType(xml='<two xmlns="https://github.com/haael/baxend" x="11" y="31">newly</two>', default_tag=None)
			print(database['one.xml'])
			print()
		except BaseXQueryError as error:
			print(error)
		
		del database['one.xml']
		del database['two.xml']
	
	#profiler.done()
	
