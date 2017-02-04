#-*- coding:utf8 -*-
import requests
import json
import MySQLdb
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import time
import re
import multiprocessing
from peewee import *
import math
import pika
import datetime
def new_task(queue,message):
    # credentials = pika.PlainCredentials('chenxl', '123456')# 连接远程IP
    #parameters = pika.ConnectionParameters('192.168.10.123', 5672, '/', credentials)#修改成服务器对应即可
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)  # 队列持久化
    channel.basic_publish(exchange='',
                          routing_key=queue,
                          body=message,
                          properties=pika.BasicProperties(delivery_mode=2))  # 消息持久化

    print " [x] Sent %r" % message
    connection.close()


class allDianRongProduct:
	def __init__(self,hello):
		self.hello = hello
	def getProduct185001A(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/185001/loans?grade=A&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/185001/loans?grade=A&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct185001B(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/185001/loans?grade=B&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/185001/loans?grade=B&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct185001C(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/185001/loans?grade=C&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/185001/loans?grade=C&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct185001D(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/185001/loans?grade=D&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/185001/loans?grade=D&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct185001E(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/185001/loans?grade=E&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/185001/loans?grade=E&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct185001F(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/185001/loans?grade=F&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/185001/loans?grade=F&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e

	def getProduct340401A(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/340401/loans?grade=A&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/340401/loans?grade=A&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct340401B(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/340401/loans?grade=B&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/340401/loans?grade=B&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct340401C(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/340401/loans?grade=C&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/340401/loans?grade=C&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct340401D(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/340401/loans?grade=D&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/340401/loans?grade=D&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct340401E(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/340401/loans?grade=E&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/340401/loans?grade=E&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct340401F(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/340401/loans?grade=F&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/340401/loans?grade=F&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e

	def getProduct68201A(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/68201/loans?grade=A&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/68201/loans?grade=A&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct68201B(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/68201/loans?grade=B&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/68201/loans?grade=B&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct68201C(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/68201/loans?grade=C&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/68201/loans?grade=C&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct68201D(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/68201/loans?grade=D&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/68201/loans?grade=D&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct68201E(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/68201/loans?grade=E&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/68201/loans?grade=E&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct68201F(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/68201/loans?grade=F&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/68201/loans?grade=F&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e

	def getProduct267601A(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/267601/loans?grade=A&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/267601/loans?grade=A&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct267601B(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/267601/loans?grade=B&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/267601/loans?grade=B&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct267601C(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/267601/loans?grade=C&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/267601/loans?grade=C&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct267601D(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/267601/loans?grade=D&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/267601/loans?grade=D&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct267601E(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/267601/loans?grade=E&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/267601/loans?grade=E&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct267601F(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/267601/loans?grade=F&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/267601/loans?grade=F&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e

	def getProduct402458A(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/402458/loans?grade=A&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/402458/loans?grade=A&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct402458B(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/402458/loans?grade=B&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/402458/loans?grade=B&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct402458C(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/402458/loans?grade=C&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/402458/loans?grade=C&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct402458D(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/402458/loans?grade=D&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/402458/loans?grade=D&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct402458E(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/402458/loans?grade=E&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/402458/loans?grade=E&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e
	def getProduct402458F(self):
		try:
			url ="https://www.dianrong.com/api/v2/plans/402458/loans?grade=F&page=1&pageSize=10"
			header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			  'Accept-Encoding': 'gzip, deflate, sdch',
			  'Accept-Language': 'zh-CN,zh;q=0.8',
			  'Connection': 'keep-alive',
			  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
			  }
			r = requests.get(url, headers=header)
			html = json.loads(r.content)
			totalRecords = html.get("content").get("totalRecords")
			total = int (math.ceil(totalRecords/10.0))
			requestPage = 1
			while requestPage < total+1:
				url = "https://www.dianrong.com/api/v2/plans/402458/loans?grade=F&page=" + str(requestPage) + "&pageSize=10"
				r = requests.get(url, headers=header)
				html = json.loads(r.content)
				LoanItems = html.get("content").get("loanItems")
				for loanItem in LoanItems:
					loanId = loanItem.get("loanId")
					try:
						conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',charset='utf8')
						cur = conn.cursor()
						sql = 'insert into all_ori_product_url values (%s,%s,%s,%s)'
						url_source = u"点融网"
						url_category = "P2P"
						statues = "NEW"
						cur.execute(sql, (loanId,url_category , statues,url_source))
						print "success %s ori" % loanId
						conn.commit()
						cur.close()
						ms = {}
						ms["url"] = loanId
						ms['category'] = url_category
						ms['url_source'] = url_source
						ms['push_time'] = str(datetime.datetime.today())
						message = json.dumps(ms)
						new_task("p2p_dianRong", message)
					except Exception,e:
						print e
				requestPage += 1
				time.sleep(0.1)
		except Exception,e:
			print e

def soMuchProduct():
	panda = allDianRongProduct("hello")
	panda.getProduct68201A()
	panda.getProduct68201B()
	panda.getProduct68201C()
	panda.getProduct68201D()
	panda.getProduct68201E()
	panda.getProduct68201F()
	panda.getProduct185001A()
	panda.getProduct185001B()
	panda.getProduct185001C()
	panda.getProduct185001D()
	panda.getProduct185001E()
	panda.getProduct185001F()
	panda.getProduct267601A()
	panda.getProduct267601B()
	panda.getProduct267601C()
	panda.getProduct267601D()
	panda.getProduct267601E()
	panda.getProduct267601F()
	panda.getProduct402458A()
	panda.getProduct402458B()
	panda.getProduct402458C()
	panda.getProduct402458D()
	panda.getProduct402458E()
	panda.getProduct402458F()
	panda.getProduct340401A()
	panda.getProduct340401B()
	panda.getProduct340401C()
	panda.getProduct340401D()
	panda.getProduct340401E()
	panda.getProduct340401F()
	print "that's ok"

if __name__ =="__main__":
	soMuchProduct()