#!/usr/bin/python3.11
#-*- coding: utf-8 -*-

"""
Python 3.6+ client for BaseX.
Works with BaseX 8.2 and later

LIMITATIONS:

* binary content would corrupt, maybe. (I didn't test it)
* also, will fail to extract stored binary content, maybe.
  (both my code, and original don't care escaped 0xff.)

Documentation: https://docs.basex.org/wiki/Clients

(C) 2012, Hiroaki Itoh. BSD License
	updated 2014 by Marc van Grootel
    updated 2021-2022 by haael

"""


from logging import getLogger, basicConfig, DEBUG
log = getLogger(__name__)

if __name__ == '__main__':
	basicConfig(level=DEBUG, format='%(asctime)-8s %(levelname)-8s %(name)-32s %(message)s')
	
	import warnings
	warnings.filterwarnings('ignore')


__all__ = 'BaseXError', 'BaseXAuthError', 'BaseXQueryError', 'BaseXCommandError', 'BaseXProtocolError', 'Session', 'Query'


import socket
from collections import deque
from hashlib import md5
from multiprocessing import Lock
from inspect import isgeneratorfunction


def locked(old_method):
	if isgeneratorfunction(old_method):
		def new_method(self, *args, **kwargs):
			with self.lock:
				yield from old_method(self, *args, **kwargs)
	else:
		def new_method(self, *args, **kwargs):
			with self.lock:
				return old_method(self, *args, **kwargs)
	new_method.__name__ = old_method.__name__
	return new_method


class BaseXError(Exception):
	"Error thrown in one of the BaseXClient methods."


class BaseXAuthError(BaseXError):
	"Error related to authentication."


class BaseXQueryError(BaseXError):
	"Error related to a query."


class BaseXCommandError(BaseXError):
	"Error related to a command."


class BaseXProtocolError(BaseXError):
	"Error related to network protocol."


class Session:
	"""
	BaseX session. Maintains connection to the server.
	
	https://docs.basex.org/wiki/Server_Protocol
	
	In order to interact with the server, perform open/login and then logout/close at exit.
	Alternatively use the context manager.
	
	User API consists of the methods:
		execute.COMMAND(*arguments) - execute a server command
		create(name, input_='') - create a database, and optionally create the default data
		add(name, path, input_) - add new data to database at the specified path
		put(path, input_) - add or replace data in previously opened database
		put_binary(path, input_) - upload binary data to previously opened database
		query(query) - perform XQuery
	"""
	
	terminator = bytes([0])
	
	class SocketWrapper:
		class Buffer:
			def __init__(self):
				self.data = deque()
				self.length = 0
			
			def __len__(self):
				assert self.length == sum(len(_s) for _s in self.data)
				return self.length
			
			def __contains__(self, ch):
				return any(ch in _s for _s in self.data)
			
			def __bytes__(self):
				return bytes().join(self.data)
			
			def index(self, ch):
				pos = 0
				for s in self.data:
					try:
						pos += s.index(ch)
						return pos
					except ValueError:
						pos += len(s)
				else:
					raise IndexError
			
			def get(self, n):
				if len(self) < n:
					raise ValueError("Not enough data")
				
				if __debug__: orig_length = len(self)
				
				result = []
				result_length = 0
				while result_length < n:
					s = self.data[0]
					needed = n - result_length
					if len(s) <= needed:
						result.append(s)
						del self.data[0]
						result_length += len(s)
					else:
						result.append(s[:needed])
						self.data[0] = s[needed:]
						result_length += needed
				
				self.length -= result_length
				assert result_length == n
				assert len(self) == orig_length - result_length
				
				return bytes().join(result)
			
			def put(self, b):
				self.data.append(b)
				self.length += len(b)
		
		def __init__(self, address, family=socket.AF_INET6, tls_context=None):
			self.address = address
			self.family = family
			self.tls_context = tls_context
			
			self.in_buffer = self.Buffer()
			self.out_buffer = self.Buffer()
		
		def open(self):
			self.__sock = socket.socket(self.family, socket.SOCK_STREAM | socket.SOCK_CLOEXEC)
			if self.tls_context:
				self.__sock = self.tls_context.wrap_socket(self.__sock)
			self.__sock.connect(self.address)
		
		def recv(self, n):
			while len(self.in_buffer) < n:
				data = self.__sock.recv(4096)
				log.debug(f"recv: {data}")
				self.in_buffer.put(data)
			return self.in_buffer.get(n)
		
		def recv_until(self, t):
			while not t in self.in_buffer:
				data = self.__sock.recv(4096)
				log.debug(f"recv: {data}")
				self.in_buffer.put(data)
			return self.in_buffer.get(self.in_buffer.index(t))
		
		def send(self, b):
			self.out_buffer.put(b)
		
		def flush(self):
			tosend = self.out_buffer.get(len(self.out_buffer))
			log.debug(f"send: {tosend}")
			self.__sock.sendall(tosend)
		
		def close(self):
			self.__sock.close()
			del self.__sock
		
		def are_buffers_empty(self):
			if len(self.in_buffer): log.error(f"Input protocol buffer corrupted: {bytes(self.in_buffer)}")
			if len(self.out_buffer): log.error(f"Output protocol buffer corrupted: {bytes(self.out_buffer)}")
			return len(self.in_buffer) == len(self.out_buffer) == 0
	
	def __init__(self, user, password, address, family=socket.AF_INET6, tls_context=None):
		"Setup session parameters with host, port, user name and password."
		self.user = user
		self.password = password
		self.address = address
		self.family = family
		self.tls_context = tls_context
	
	def open(self):
		"Open network connection to the server."
		self.__swrapper = self.SocketWrapper(self.address, self.family, self.tls_context)
		self.__swrapper.open()
		self.lock = Lock()
	
	@staticmethod
	def md5(s):
		return md5(s.encode('utf-8')).hexdigest()
	
	@locked
	def login(self):
		"Log in. Must be the first method called after open."
		try:
			realm, nonce = self.recv_str().split(':')
		except TypeError:
			raise BaseXProtocolError("Error in login protocol.")
		
		self.send_str(self.user)
		self.send_str(self.md5(self.md5(':'.join([self.user, realm, self.password])) + nonce))
		self.flush()
		
		status = self.recv_byte()
		if not self.are_buffers_empty():
			raise BaseXProtocolError(f"Garbage left in protocol buffers (login).")
		
		if status == 0x0:
			return
		elif status == 0x1:
			raise BaseXAuthError(f"Access denied for user {self.user}", self.user)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	def logout(self):
		"Inform the server that the session ended. The server will close the connection at its side."
		self._COMMAND('EXIT')
	
	def close(self):
		"Close network connection to the server."
		self.__swrapper.close()
		del self.__swrapper
		del self.lock
	
	def send_byte(self, b):
		"Buffer one byte for sending. The argument must be 0 <= b < 256."
		if not 0 <= b < 256:
			raise ValueError("Argument outside byte range.")
		self.__swrapper.send(bytes([b]))
	
	def send_str(self, s):
		"Buffer a string for sending."
		self.__swrapper.send(s.encode('utf-8'))
		self.__swrapper.send(self.terminator)
	
	def flush(self):
		"Flush data from the output buffer."
		self.__swrapper.flush()
	
	def recv_byte(self):
		"Receive one byte."
		return self.__swrapper.recv(1)[0]
	
	def recv_str(self):
		"Receive a string."
		result = self.__swrapper.recv_until(self.terminator)
		zero = self.__swrapper.recv(1)
		assert len(zero) == 1 and zero[0] == 0
		return result.decode('utf-8')
	
	def are_buffers_empty(self):
		"Check if input and output buffers are empty."
		return self.__swrapper.are_buffers_empty()
	
	def __enter__(self):
		"Enter context manager. Open connection to the server, log in, return an active session."
		self.open()
		try:
			self.login()
		except:
			self.close()
			raise
		return self
	
	def __exit__(self, e_type, e_val, e_stack):
		"Exit context manager. If the exit was clean, log out. Close the connection."
		if e_type == None and e_val == None and e_stack == None:
			self.logout()
		self.close()
	
	@locked
	def _COMMAND(self, command):
		"Executes a database command."
		
		log.info(f"BaseX command: {command}")
		self.send_str(command)
		self.flush()
		
		result = self.recv_str()
		info = self.recv_str()
		status = self.recv_byte()		
		if not self.are_buffers_empty():
			raise BaseXProtocolError("Garbage left in protocol buffers (_COMMAND).")
		
		if status == 0x0:
			return result
		elif status == 0x1:
			raise BaseXCommandError(info, "COMMAND", command)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _QUERY(self, query):
		"Creates a new query instance and returns its id."
		
		log.info(f"BaseX create xquery.")
		log.debug('\n\t'.join(query.split('\n')) + '\n')
		self.send_byte(0x0)
		self.send_str(query)
		self.flush()
		
		id_ = self.recv_str()
		status = self.recv_byte()
		if not self.are_buffers_empty():
			raise BaseXProtocolError("Garbage left in protocol buffers (_QUERY).")
		
		if status == 0x0:
			return id_
		elif status == 0x1:
			raise BaseXQueryError("Error creating XQuery", query)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _CREATE(self, name, input_=''):
		"Creates a new database with the specified input (may be empty)."
		
		log.info(f"BaseX create database: {name}, input length: {len(input_)}")
		self.send_byte(0x8)
		self.send_str(name)
		self.send_str(input_)
		self.flush()
		
		info = self.recv_str()
		status = self.recv_byte()
		if not self.are_buffers_empty():
			raise BaseXProtocolError("Garbage left in protocol buffers (_CREATE).")
		
		if status == 0x0:
			return
		elif status == 0x1:
			raise BaseXCommandError(info, "CREATE", name, input_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _ADD(self, name, path, input_):
		"Adds a new document to the opened database."
		
		log.info(f"BaseX add XML file: database: {name}, path: {path}, input length: {len(input_)}")
		self.send_byte(0x9)
		self.send_str(name)
		self.send_str(path)
		self.send_str(input_)
		self.flush()
		
		info = self.recv_str()
		status = self.recv_byte()
		if not self.are_buffers_empty():
			raise BaseXProtocolError("Garbage left in protocol buffers (_ADD).")
		
		if status == 0x0:
			return
		elif status == 0x1:
			raise BaseXCommandError(info, "ADD", name, path, input_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _PUT(self, path, input_):
		"Puts (adds or replaces) an XML document resource in the opened database."
		
		log.info(f"BaseX put XML file: {path}, input length: {len(input_)}")
		self.send_byte(0xc)
		self.send_str(path)
		self.send_str(input_)
		self.flush()
		
		info = self.recv_str()
		status = self.recv_byte()
		if not self.are_buffers_empty():
			raise BaseXProtocolError("Garbage left in protocol buffers (_PUT).")
		
		if status == 0x0:
			return
		elif status == 0x1:
			raise BaseXCommandError(info, "PUT", path, input_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _PUTBINARY(self, path, input_):
		"Puts (adds or replaces) a binary resource in the opened database. "
		
		log.info(f"BaseX put binary file: {path}, input length: {len(input_)}")
		self.send_byte(0x9)
		self.send_str(path)
		self.send(input_) # TODO: escape
		self.send(bytes([0]))
		self.flush()
		
		info = self.recv_str()
		status = self.recv_byte()
		if not self.are_buffers_empty():
			raise BaseXProtocolError("Garbage left in protocol buffers (_PUTBINARY).")
		
		if status == 0x0:
			return
		elif status == 0x1:
			raise BaseXCommandError(info, "PUTBINARY", path, input_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _CLOSE(self, id_):
		"Closes and unregisters the query with the specified id."
		
		log.info(f"BaseX close xquery {id_}")
		self.send_byte(0x2)
		self.send_str(str(id_))
		self.flush()
		
		info = self.recv_str()
		status = self.recv_byte()
		if not self.are_buffers_empty():
			raise BaseXProtocolError("Garbage left in protocol buffers (_CLOSE).")
		
		if status == 0x0:
			return
		elif status == 0x1:
			raise BaseXCommandError(info, "CLOSE", id_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} ('{chr(status)}') instead.")
	
	@locked
	def _BIND(self, id_, name, value, type_):
		"Binds a value to a variable. The type will be ignored if the string is empty."
		
		log.info(f"BaseX bind xquery variable: id: {id_}, {name} := {value}, type:{type_}")
		self.send_byte(0x3)
		self.send_str(str(id_))
		self.send_str(name)
		self.send_str(value)
		self.send_str(type_)
		self.flush()
		
		zero = self.recv_byte()
		if zero != 0x0:
			raise BaseXProtocolError(f"Expected zero byte, got {hex(zero)} instead.")
		
		status = self.recv_byte()
		
		if status == 0x0:
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_BIND).")
		elif status == 0x1:
			info = self.recv_str()
			raise BaseXQueryError(info, "BIND", id_, name, value, type_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _RESULTS(self, id_):
		"Returns all resulting items as strings, prefixed by a single byte that represents the Type ID."
		
		log.info(f"BaseX iterate through xquery results {id_}")
		self.send_byte(0x4)
		self.send_str(str(id_))
		self.flush()
		
		typeid = self.recv_byte()
		while typeid != 0x0:
			item = self.recv_str()
			yield typeid, item
			typeid = self.recv_byte()
		
		status = self.recv_byte()
		
		if status == 0x0:
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_RESULTS, no error).")
			return
		elif status == 0x1:
			info = self.recv_str()
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_RESULTS, error).")
			raise BaseXQueryError(info, "RESULTS", id_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _EXECUTE(self, id_):
		"Executes the query and returns the result as a single string."
		
		log.info(f"BaseX execute xquery {id_}")
		self.send_byte(0x5)
		self.send_str(str(id_))
		self.flush()
		
		result = self.recv_str()
		status = self.recv_byte()
		
		if status == 0x0:
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_EXECUTE, no error).")
			return result
		elif status == 0x1:
			info = self.recv_str()
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_EXECUTE, error).")
			raise BaseXQueryError(info, "EXECUTE", id_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _INFO(self, id_):
		"Returns a string with query compilation and profiling info."

		self.send_byte(0x6)
		self.send_str(str(id_))
		self.flush()
		
		result = self.recv_str()
		status = self.recv_byte()
		
		if status == 0x0:
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_INFO, no error).")
			return result
		elif status == 0x1:
			info = self.recv_str()
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_INFO, error).")
			raise BaseXQueryError(info, "INFO", id_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _OPTIONS(self, id_):
		"Returns a string with all query serialization parameters, which can e.g. be assigned to the SERIALIZER option."
		
		self.send_byte(0x7)
		self.send_str(str(id_))
		self.flush()
		
		result = self.recv_str()
		status = self.recv_byte()
		
		if status == 0x0:
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_OPTIONS, no error).")
			return result
		elif status == 0x1:
			info = self.recv_str()
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_OPTIONS, error).")
			raise BaseXQueryError(info, "OPTIONS", id_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _CONTEXT(self, id_, value, type_):
		"Binds a value to the context. The type will be ignored if the string is empty."

		self.send_bytes(0xe)
		self.send_str(str(id_))
		self.send_str(value)
		self.send_str(type_)
		self.flush()
		
		zero = self.recv_byte()
		if zero != 0x0:
			raise BaseXProtocolError(f"Expected zero byte, got {hex(zero)} instead.")
		
		status = self.recv_byte()
		
		if status == 0x0:
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_CONTEXT, no error).")
			return
		elif status == 0x1:
			info = self.recv_str()
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_CONTEXT, error).")
			raise BaseXQueryError(info, "CONTEXT", id_, value, type_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _UPDATING(self, id_):
		"Returns true if the query contains updating expressions; false otherwise."
		
		self.send_byte(0x1e)
		self.send_str(str(id_))
		self.flush()
		
		result = self.recv_str()
		status = self.recv_byte()
		
		if status == 0x0:
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_UPDATING, no error).")
			return result
		elif status == 0x1:
			info = self.recv_str()
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_UPDATING, error).")
			raise BaseXQueryError(info, "UPDATING", id_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	@locked
	def _FULL(self, id_):
		"Returns all resulting items as strings, prefixed by the XDM Metadata."
		
		self.send_byte(0x1f)
		self.send_str(str(id_))
		self.flush()
		
		typeid = self.recv_byte()
		while typeid != 0x0:
			if typeid in (12, 14, 82):
				xdm = self.recv_str()
			else:
				xdm = None
			item = self.recv_str()
			yield typeid, xdm, item
			typeid = self.recv_byte()
		
		status = self.recv_byte()
		
		if status == 0x0:
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_FULL, no error).")
			return
		elif status == 0x1:
			info = self.recv_str()
			if not self.are_buffers_empty():
				raise BaseXProtocolError("Garbage left in protocol buffers (_FULL, error).")
			raise BaseXQueryError(info, "FULL", id_)
		else:
			raise BaseXProtocolError(f"Expected status byte 0 or 1, got {hex(status)} instead.")
	
	create = _CREATE
	add = _ADD
	put = _PUT
	put_binary = _PUTBINARY
	
	def query(self, query):
		"Return an XQuery helper."
		return Query(self, query)
	
	@property
	def execute(self):
		"Return command helper."
		return Command(self)


class Command:
	"""
	Helper class for making server commands. The preferred way of obtaining it is through `Session.execute` method.
	
	BaseX command list: https://docs.basex.org/wiki/Commands
	
	Fetch an attribute as the command name with spaces replaced by underscores, then call it with the right arguments.
	If a command should happen to be a reserved Python keyword, add an underscore at the end.
	If a command expects parenthesized arguments, provide them explicitly.
	
	```
		session.execute.open_('my_database')	# OPEN my_database
		session.execute.info_storage(10, 50)	# INFO STORAGE 10 50
		session.execute.close()					# CLOSE
	```
	"""
	
	def __init__(self, session):
		self.__session = session
	
	def __getattr__(self, attr):
		return lambda *args: self.__session._COMMAND(' '.join([attr.upper().replace('_', ' ').strip()] + [str(_arg) for _arg in args]))


class Query:
	"""
	XQuery manager class. The preferred way of obtaining it is through `Session.query` method.
	
	More about XQuery syntax: https://www.w3schools.com/xml/xquery_intro.asp
	
	Use through explicit calls:
	```
		query = session.query("XQUERY...")
		query.open()
		print(query.info())
		
		# retrieve result as one big string
		result = query.execute()
		process_all(result)
		
		# iterate through results one by one
		for typeid, item in query.results(): 
			process_one(item)
		
		query.close() # make sure to close the query if an error happens
	```
	
	Use through context manager:
	```
		with session.query("XQUERY...") as query:
			print(query.info())
			
			# retrieve result as one big string
			result = query.execute()
			process_all(result)
			
			# iterate through results one by one
			for typeid, item in query.results(): 
				process_one(item)
	```
	
	Use all-in-one helpers:
	```
		# retrieve result as one big string
		result = session.query("XQUERY...")() # call the object
		process_all(result)
		
		# iterate through results one by one
		for typeid, item in session.query("XQUERY..."): # use the object as iterator
			process_one(item)
	```
	"""
	
	typeids = [
		None, None, None, None, None, None, None, 'function', 'node()', 'text()',
		'processing-instruction()', 'element()', 'document-node()', 'document-node(element())', 'attribute()', 'comment()', None, None, None, None,
		None, None, None, None, None, None, None, None, None, None,
		None, None, 'item()', 'xs:untyped', 'xs:anyType', 'xs:anySimpleType', 'xs:anyAtomicType', 'xs:untypedAtomic', 'xs:string', 'xs:normalizedString',
		'xs:token', 'xs:language', 'xs:NMTOKEN', 'xs:Name', 'xs:NCName', 'xs:ID', 'xs:IDREF', 'xs:ENTITY', 'xs:float', 'xs:double',
		'xs:decimal', 'xs:precisionDecimal', 'xs:integer', 'xs:nonPositiveInteger', 'xs:negativeInteger', 'xs:long', 'xs:int', 'xs:short', 'xs:byte', 'xs:nonNegativeInteger',
		'xs:unsignedLong', 'xs:unsignedInt', 'xs:unsignedShort', 'xs:unsignedByte', 'xs:positiveInteger', 'xs:duration', 'xs:yearMonthDuration', 'xs:dayTimeDuration', 'xs:dateTime', 'xs:dateTimeStamp',
		'xs:date', 'xs:time', 'xs:gYearMonth', 'xs:gYear', 'xs:gMonthDay', 'xs:gDay', 'xs:gMonth', 'xs:boolean', 'basex:binary', 'xs:base64Binary',
		'xs:hexBinary', 'xs:anyURI', 'xs:QName', 'xs:NOTATION'
	]
	
	def __init__(self, session, query):
		"Initialize the Query object with a session and query string. The session does not have to be active, but it must be activated before opening the query."
		self.session = session
		self.query = query
	
	def is_open(self):
		return hasattr(self, 'id_')
	
	def open(self):
		"Sends the query and creates the underlying resources on the server. Must be called before any data method. Returns the query id created by the server."
		if hasattr(self, 'id_'):
			raise ValueError("Query already active.")
		self.id_ = int(self.session._QUERY(self.query))
		log.info(f"Opened xquery id {self.id_}")
		return self.id_
	
	def close(self):
		"Closes the query and frees the resources on the server. Might be open again later."
		if not hasattr(self, 'id_'):
			raise ValueError("Query not in active state.")
		self.session._CLOSE(self.id_)
		del self.id_
	
	def execute(self):
		"Return the result of the query as one big string."
		return self.session._EXECUTE(self.id_)
	
	def results(self):
		"Yield results one by one as (typeid, value)."
		for typeid, value in self.session._RESULTS(self.id_):
			try:
				typestr = self.typeids[typeid]
			except KeyError:
				typestr = None
			yield typestr, value
	
	def full(self):
		"Yield results one by one as (typeid, XDM, value). XDM is an URL for typeids: document-node(), attribute(), xs:QName otherwise is None."
		for typeid, xdm, value in self.session._FULL(self.id_):
			try:
				typestr = self.typeids[typeid]
			except KeyError:
				typestr = None
			yield typestr, xdm, value
	
	def info(self):
		"Return compilation and profiling info of this query."
		return self.session._INFO(self.id_)
	
	def options(self):
		"Return query serialization parameters."
		return self.session._OPTIONS(self.id_)
	
	def updating(self):
		"Check if this query is updating (writing) resources, or is read-only."
		return self.session._UPDATING(self.id_) == 'true'
	
	def bind(self, name, value, type_=''):
		"Bind a value to an external variable declared in the query."
		return self.session._BIND(self.id_, name, str(value), type_)
	
	def context(self, value, type_=''):
		return self.session._CONTEXT(self.id_, value, type_)
	
	def __enter__(self):
		"Enter context manager. Use as alternative to open/close."
		self.open()
		return self
	
	def __exit__(self, *args):
		"Exit context manager. Use as alternative to open/close."
		self.close()
	
	def __call__(self):
		"All-in-one convenience function. Open the query, execute, return all results as one big string, close."
		self.open()
		try:
			return self.execute()
		finally:
			self.close()
	
	def __iter__(self):
		"All-in-one convenience function. Open the query, execute, yield results one by one prefixed by typeid, close."
		self.open()
		try:
			yield from self.results()
		finally:
			self.close()


if __debug__ and __name__ == '__main__':
	with Session('db_user', 'wemn2o03289', ('::1', 1984)) as session:
		print(session.execute.info())
		databases = [_line.split(' ')[0] for _line in session.execute.list_().split('\n')[2:-3]]
		print(databases)
		session.execute.open_(databases[0])
		files = [_line.split(' ')[0] for _line in session.execute.dir_().split('\n')[2:-3]]
		print(files)
		print(session.execute.get(files[1]))
		session.execute.close()
		
		try:
			print(session.query(f'doc("nonexistent")/*[1]')())
		except BaseXQueryError as error:
			print("expected error:", error)
		
		try:
			print(list(session.query(f'doc("nonexistent")/*[1]')))
		except BaseXQueryError as error:
			print("expected error:", error)
		
		print(session.query(f'doc("{databases[0]}/{files[1]}")/*[1]')())
		print(list(session.query(f'doc("{databases[0]}/{files[1]}")/*[1]')))
		
		with session.query(f'doc("{databases[0]}/{files[1]}")/*[1]') as query:
			print(query.info())
			print(query.updating())
			print(list(query.full()))
		
		try:
			with session.query(f'doc("{databases[0]}/nonexistent")/*[1]') as query:
				print(query.options())
				print(query.updating())
				print(list(query.full()))
		except BaseXQueryError as error:
			print("expected error:", error)
		
		try:
			session.execute.burp()
		except BaseXCommandError as error:
			print("expected error:", error)



