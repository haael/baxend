#!/usr/bin/python3.11
#-*- coding:utf-8 -*-


from logging import getLogger, basicConfig, DEBUG
log = getLogger(__name__)

if __name__ == '__main__':
	basicConfig(level=DEBUG, format='%(asctime)-8s %(levelname)-8s %(name)-32s %(message)s')
	
	import warnings
	warnings.filterwarnings('ignore')


__all__ = 'XMLType',


from xml.etree.ElementTree import ElementTree, Element, tostring, canonicalize, ParseError
from defusedxml.ElementTree import fromstring
from itertools import chain


class XMLType:
	"Base class for object that keep their data as XML."
	
	__slots__ = 'xml', 'xml_attribute', 'xml_element_type', 'xml_tag'
	
	xmlns = {'xml':'http://www.w3.org/XML/1998/namespace'}
	
	def __init__(self, xml, default_tag):
		if xml != None:
			if isinstance(xml, str):
				try:
					self.xml = fromstring(xml.strip())
				except ParseError as error:
					log.error(f"Error while parsing XML text into document: {error}")
					log.debug("\n" + xml.strip())
					raise
			else:
				self.xml = xml
		else:
			try:
				pfx, lname = default_tag.split(':')
			except ValueError:
				pfx = ''
				lname = default_tag
			ns = self.xmlns[pfx]
			self.xml = Element(f'{{{ns}}}{lname}')
		self.xml_attribute = {}
		self.xml_element_type = {}
		self.xml_tag = default_tag
	
	def __getattr__(self, py_attr):
		"Get XML attribute from the element (if registered)."
		
		if py_attr in self.__slots__:
			return super().__getattribute__(py_attr)
		
		try:
			xml_attr, convert, _ = self.xml_attribute[py_attr]
		except KeyError:
			return super().__getattribute__(py_attr)
		else:
			try:
				pfx, lname = xml_attr.split(':')
			except ValueError:
				et_attr = xml_attr
			else:
				ns = self.xmlns[pfx]
				et_attr = f'{{{ns}}}{lname}'
			
			try:
				return convert(self.xml.attrib[et_attr]) if convert != None else self.xml.attrib[et_attr]
			except KeyError:
				raise AttributeError
	
	def __setattr__(self, py_attr, value):
		"Set value of the XML attribute on the element (if registered)."
		
		if py_attr in self.__slots__:
			super().__setattr__(py_attr, value)
			return
		
		try:
			xml_attr, _, convert = self.xml_attribute[py_attr]
		except KeyError:
			super().__setattr__(py_attr, value)
		else:
			try:
				pfx, lname = xml_attr.split(':')
			except ValueError:
				et_attr = xml_attr
			else:
				ns = self.xmlns[pfx]
				et_attr = f'{{{ns}}}{lname}'
			
			self.xml.attrib[et_attr] = convert(value) if convert != None else value
	
	def __delattr__(self, py_attr):
		"Delete XML attribute from the element (if registered)."
		
		if py_attr in self.__slots__:
			super().__delattr__(py_attr)
			return
		
		try:
			xml_attr, _, _ = self.xml_attribute[py_attr]
		except KeyError:
			super().__delattr__(py_attr)
		else:
			try:
				pfx, lname = xml_attr.split(':')
			except ValueError:
				et_attr = xml_attr
			else:
				ns = self.xmlns[pfx]
				et_attr = f'{{{ns}}}{lname}'
			
			try:
				del self.xml.attrib[et_attr]
			except KeyError:
				raise AttributeError
	
	def __getitem__(self, index):
		if isinstance(index, str):
			if index[0] == '#':
				id_val = index[1:]
				xml_element = self.xml.find(f'.//*[@xml:id="{id_val}"]', namespaces=self.xmlns)
				if xml_element == None:
					raise KeyError(f"Element with id \"{id_val}\" not found.")
			
			elif index[0] == '@':
				if ':' in index:
					pfx, lname = index[1:].split(':')
					ns = self.xmlns[pfx]
					attr = f'{{{ns}}}{lname}'
				else:
					attr = index[1:]
				
				try:
					return self.xml.attrib[attr]
				except KeyError:
					raise KeyError(f"Attribute \"{attr}\" not found.")
			
			else:
				if ':' in index:
					pfx, lname = index.split(':')
				else:
					lname = index
					pfx = ''
				
				try:
					ns = self.xmlns[pfx]
				except KeyError:
					raise ValueError(f"Namespace prefix not found: \"{pfx}\"")
				
				if ns:
					tagname = f'{{{ns}}}{lname}'
				else:
					tagname = lname
				
				xml_element = self.xml.find(f'./{tagname}', namespaces=self.xmlns)
				if xml_element == None:
					raise KeyError(f"Element {tagname} not found.")
		else:
			try:
				xml_element = self.xml[index]
			except IndexError:
				raise KeyError(f"Element at position {index} not found. Number of children: {len(self.xml)}.")
		
		if isinstance(xml_element, list):
			result = []
			for item in xml_element:
				try:
					if item.tag[0] == '{':
						ns, lname = item.tag.split('}')
						ns = ns[1:]
					else:
						ns = ''
						lname = item.tag
					
					for x_pfx, n_ns in self.xmlns.items():
						if n_ns == ns:
							if x_pfx:
								pfx = x_pfx + ':'
							else:
								pfx = ''
							break
					else:
						raise ValueError(f"Prefix not found for namespace `{ns}`. Add it to the `XMLType.xmlns` dictionary.")
					
					xml_element_type = self.xml_element_type[pfx + lname]
				except KeyError:
					try:
						xml_element_type = self.xml_element_type['*']
					except KeyError:
						xml_element_type = lambda xml: XMLType(xml=xml, default_tag=None)
				result.append(xml_element_type(xml=item))
			return result
		else:
			tagname = xml_element.tag
			if tagname[0] == '{':
				ns, lname = tagname.split('}')
				ns = ns[1:]
			else:
				ns = ''
				lname = tagname
			
			for x_pfx, x_ns in self.xmlns.items():
				if x_ns == ns:
					if x_pfx:
						pfx = x_pfx + ':'
					else:
						pfx = ''
					break
			else:
				raise ValueError(f"Prefix not found for namespace `{ns}`. Add it to the `XMLType.xmlns` dictionary.")
			
			try:
				xml_element_type = self.xml_element_type[pfx + lname]
			except KeyError:
				try:
					xml_element_type = self.xml_element_type['*']
				except KeyError:
					xml_element_type = lambda xml: XMLType(xml=xml, default_tag=None)
			return xml_element_type(xml=xml_element)
	
	def __setitem__(self, index, element):
		if isinstance(index, str):
			xml_element = self.xml.find(f'.//*[@xml:id="{index}"]', namespaces=self.xmlns)
			if xml_element == None:
				raise KeyError(f"Element with id \"{index}\" not found.")
			if xml_element is element.xml: return # idempotent assignment
			xml_element.clear()
			xml_element.tag = element.xml.tag
			xml_element.attrib.update(element.xml.attrib)
			xml_element.extend(element.xml)
			xml_element.text = element.xml.text
			xml_element.tail = element.xml.tail
		elif isisntance(index, slice):
			self.xml[index] = [_element.xml for _element in element]
		else:
			try:
				self.xml[index] = element.xml
			except IndexError:
				raise KeyError(f"Element at position {index} not found. Number of children: {len(self.xml)}.")
	
	def __delitem__(self, index):
		if isinstance(index, str):
			xml_element = self.xml.find(f'.//*[@xml:id="{index}"]', namespaces=self.xmlns)
			if xml_element == None:
				raise KeyError(f"Element with id \"{index}\" not found.")
			self.xml.remove(xml_element)
		else:
			try:
				del self.xml[index]
			except IndexError:
				raise KeyError(f"Element at position {index} not found. Number of children: {len(self.xml)}.")
	
	def append(self, element_or_text):
		try:
			self.xml.append(element_or_text.xml)
		except AttributeError:
			if not len(self):
				self.xml.text = element_or_text
			else:
				self.xml[-1].tail = element_or_text
	
	def extend(self, elements):
		self.xml.extend([_element.xml for _element in elements])
	
	def insert(self, index, element):
		self.xml.insert(index, element.xml)
	
	def __len__(self):
		return len(self.xml)
	
	def __iter__(self):
		for index in range(len(self)):
			yield self[index]
	
	def __iadd__(self, element):
		self.append(element)
		return self
	
	def __bytes__(self):
		return str(self).encode('utf-8')
	
	def __str__(self):
		try:
			return canonicalize('\n'.join(self.lines()))
		except ParseError as error:
			log.error(f"Error while rendering XML document into text: {error}")
			log.debug("\n" + '\n'.join(self.lines()))
			raise
	
	def __hash__(self):
		return hash(str(self))
	
	def __eq__(self, other):
		if hasattr(self, 'xml') and hasattr(other, 'xml'):
			return str(self) == str(other)
		else:
			return NotImplemented
	
	@staticmethod
	def __escape(text):
		text = text.replace('&', '&amp;')
		text = text.replace('<', '&lt;')
		text = text.replace('>', '&gt;')
		return text
	
	def lines(self, indent=0, context_ns=None, preserve_whitespace=False, xmlns={}, include_xmlns=True):
		"Yield lines of the XML tree one by one. Honours `xml:space`."
		
		spaces = " " * indent
		
		xml_space = self.xml.attrib.get(f'{{{self.xmlns["xml"]}}}space', None)
		preserve_whitespace |= (xml_space == 'preserve')
		preserve_whitespace &= (xml_space != 'default')
		
		try:
			tag = self.xml.tag.split('}')[1]
			ns =  self.xml.tag.split('}')[0][1:]
			if ns == context_ns or context_ns == Ellipsis:
				ns = None
		except IndexError:
			tag = self.xml.tag
			ns = None
		
		pfx = ''
		if ns != None:
			nopfx = set()
			for x_pfx, x_ns in chain(xmlns.items(), self.xmlns.items()):
				if x_pfx in nopfx: continue
				if x_ns == None:
					nopfx.add(x_pfx)
					continue
				if x_ns == ns:
					if x_pfx:
						pfx = x_pfx + ':'
					ns = None
					break
		
		attr_list = []
		
		if ns != None:
			attr_list.append(f' xmlns="{self.__escape(ns)}"')
			xmlns = xmlns.copy()
			xmlns[''] = ns
		
		if include_xmlns:
			effective_xmlns = {}
			effective_xmlns.update(self.xmlns)
			effective_xmlns.update(xmlns)
			for x_pfx, x_ns in effective_xmlns.items():
				if x_pfx.startswith('xml'): continue
				if x_ns == None: continue
				if x_pfx:
					attr_list.append(f' xmlns:{x_pfx}="{self.__escape(x_ns)}"')
				elif ns == None:
					attr_list.append(f' xmlns="{self.__escape(x_ns)}"')
		
		for attr_name in sorted(self.xml.attrib.keys()):
			attr_value = self.xml.attrib[attr_name]

			try:
				attr_ns = attr_name.split('}')[0][1:]
				attr_lname = attr_name.split('}')[1]
				
				if attr_ns:
					for x_pfx, x_ns in chain(xmlns.items(), self.xmlns.items()):
						if x_ns == None: continue
						if x_ns == attr_ns:
							attr_pfx = x_pfx + ':'
							break
					else:
						raise ValueError(f"Namespace prefix not found for: \"{attr_ns}\". Add it to `XMLType.xmlns` dictionary.")
				else:
					attr_pfs = ''
			except IndexError:
				attr_pfx = ''
				attr_lname = attr_name
			
			attr_list.append(f' {attr_pfx}{attr_lname}="{self.__escape(attr_value)}"')
		
		attrs = ''.join(attr_list)
		
		if not len(self) and not self.xml.text:
			if context_ns == Ellipsis:
				yield f'{spaces}<{pfx}{tag}{attrs}></{pfx}{tag}>'
			else:
				yield f'{spaces}<{pfx}{tag}{attrs}/>'
		
		elif not len(self) and preserve_whitespace:
			yield f'{spaces}<{pfx}{tag}{attrs}>{self.__escape(self.xml.text)}</{pfx}{tag}>'
		
		elif preserve_whitespace: # FIXME
			
			opening = f'{spaces}<{pfx}{tag}{attrs}>'
			if self.xml.text:
				lines = self.xml.text.split('\n')
				if len(lines) > 1:
					yield opening + self.__escape(lines[0])
					for line in lines[1:-1]:
						yield self.__escape(line)
					prev = self.__escape(lines[-1])
				else:
					prev = opening + self.__escape(lines[0])
			else:
				prev = opening
			
			nxt = None
			for child in self:
				for line in child.lines(0, ns if (ns != None and context_ns != Ellipsis) else context_ns, True, xmlns, False):
					if nxt:
						yield nxt
					
					if prev:
						nxt = prev + line
						prev = None
					else:
						nxt = line
				
				if child.xml.tail:
					lines = child.xml.tail.split('\n')
					if len(lines) > 1:
						yield nxt + self.__escape(lines[0])
						for line in lines[1:-1]:
							yield self.__escape(line)
						prev = self.__escape(lines[-1])
					else:
						prev = nxt + self.__escape(lines[0])
				else:
					prev = nxt
				nxt = None
			
			if self.xml.tail:
				lines = self.xml.tail.split('\n')
				if len(lines) > 1:
					yield prev + self.__escape(lines[0])
					for line in lines[1:-1]:
						yield self.__escape(line)
					prev = self.__escape(lines[-1])
				else:
					prev = prev + self.__escape(lines[0])
			
			closing = f'</{pfx}{tag}>'
			yield prev + closing
		
		else:
			yield f'{spaces}<{pfx}{tag}{attrs}>'

			if self.xml.text and self.xml.text.strip():
				yield spaces + " " + self.__escape(self.xml.text.strip())
			for child in self:
				yield from child.lines(indent + 1, ns if (ns != None and context_ns != Ellipsis) else context_ns, False, xmlns, False)
				if child.xml.tail and child.xml.tail.strip():
					yield spaces + " " + self.__escape(child.xml.tail.strip())

			yield f'{spaces}</{pfx}{tag}>'


if __debug__ and __name__ == '__main__':
	root_xml = '''
		<b:root xmlns:b="https://github.com/haael/baxend">
			<b:one a="1">
				<b:two b="1">A</b:two>


				<c:two xmlns:c="other" b="2">B</c:two>
				<b:two b="3">C &amp; C</b:two>
			</b:one>
			<b:one xml:id="first" a="2" xml:space="preserve">
				<b:two b="1">D</b:two>


				<b:two b="2" xml:space="default">E</b:two>
				<b:two b="3">F</b:two>
			</b:one>
			<b:one xml:id="second" a="3">
				<b:two b="1" xml:space="preserve">G</b:two>
				<b:two xml:id="third" b="2">H</b:two>
				<b:two b="3">I</b:two>
			</b:one>
		</b:root>
	'''
	
	fourth_xml = '''
		<baxend:one xml:id="fourth" xmlns:baxend="https://github.com/haael/baxend">
			<baxend:two b="1">J</baxend:two>
			<baxend:two b="2">K</baxend:two>
			<baxend:two b="3">L</baxend:two>
		</baxend:one>
	'''
	
	XMLType.xmlns['baxend'] = 'https://github.com/haael/baxend'
	XMLType.xmlns['other'] = 'other'
	
	root = XMLType(root_xml, 'baxend:root')
	print(root.xml_tag)
	fourth = XMLType(fourth_xml, 'baxend:one')
	root += fourth
	fifth = XMLType(None, 'baxend:one')
	root += fifth
	print(fifth.xml_tag)
	
	xmlns = {'':'https://github.com/haael/baxend', 'baxend':None, 'other':None}
	
	print()
	for line in root.lines(xmlns=xmlns):
		print(line)
	print("----")
	
	print()
	for line in root[1].lines(xmlns=xmlns):
		print(line)
	print("----")

	print()
	for line in root[2].lines(xmlns=xmlns):
		print(line)
	print("----")

	print()
	for line in root['#first'].lines(xmlns=xmlns):
		print(line)
	print("----")

	print()
	for line in root['#second'].lines(xmlns=xmlns):
		print(line)
	print("----")

	print()
	for line in root['#third'].lines(indent=1, xmlns=xmlns):
		print(line)
	print("----")
	
	print()
	for line in root['#fourth'].lines(xmlns=xmlns):
		print(line)
	print("----")
	
	print()
	for line in root['baxend:one'].lines(xmlns=xmlns):
		print(line)
	print("----")
	
	print(root['baxend:one']['@a'])
	
	print(root[0][2].xml.text)


