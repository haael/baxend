#!/usr/bin/python3.11
#-*- coding:utf-8 -*-


from logging import getLogger, basicConfig, DEBUG
log = getLogger(__name__)

if __name__ == '__main__':
	basicConfig(level=DEBUG, format='%(asctime)-8s %(levelname)-8s %(name)-32s %(message)s')
	
	import warnings
	warnings.filterwarnings('ignore')


