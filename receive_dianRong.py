# -*- coding:utf8 -*-
import requests
from bs4 import BeautifulSoup
import re
import warnings
warnings.filterwarnings("ignore")
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import time
import json
import pika
import datetime
import pandas as pd

from peewee import *
database = MySQLDatabase('original_data', **{'host': '192.168.10.203', 'password': 'caiji@123', 'port': 3306, 'user': 'caiji'})
class UnknownField(object):
    def __init__(self, *_, **__): pass
class BaseModel(Model):
    class Meta:
        database = database
class P2PDianrong(BaseModel):
    age = TextField(null=True)
    amount = TextField(null=True)
    borrowerdescription = TextField(db_column='borrowerDescription', null=True)
    borrowergender = TextField(db_column='borrowerGender', null=True)
    cashguaranteed = TextField(db_column='cashGuaranteed', null=True)
    childrenstatus = TextField(db_column='childrenStatus', null=True)
    collateralized = TextField(null=True)
    companycity = TextField(db_column='companyCity', null=True)
    companyindustry = TextField(db_column='companyIndustry', null=True)
    companysize = TextField(db_column='companySize', null=True)
    companytype = TextField(db_column='companyType', null=True)
    dailypaid = TextField(db_column='dailyPaid', null=True)
    description = TextField(null=True)
    educationleve = TextField(db_column='educationLeve', null=True)
    expirationdate = TextField(db_column='expirationDate', null=True)
    expirationtime = TextField(db_column='expirationTime', null=True)
    expired = TextField(null=True)
    fundingpercentage = TextField(db_column='fundingPercentage', null=True)
    fundingreceived = TextField(db_column='fundingReceived', null=True)
    grade = TextField(null=True)
    guaranteed = TextField(null=True)
    hascredithistory = TextField(db_column='hasCreditHistory', null=True)
    hassubmissiondate = TextField(db_column='hasSubmissionDate', null=True)
    infunding = TextField(db_column='inFunding', null=True)
    investorcount = TextField(db_column='investorCount', null=True)
    issued = TextField(null=True)
    jobtime = TextField(db_column='jobTime', null=True)
    jobtitle = TextField(db_column='jobTitle', null=True)
    loanid = CharField(db_column='loanId', primary_key=True)
    loanlength = TextField(db_column='loanLength', null=True)
    loanlengthyear = TextField(db_column='loanLengthYear', null=True)
    loanstatus = TextField(db_column='loanStatus', null=True)
    loantimeremaining = TextField(db_column='loanTimeRemaining', null=True)
    loantype = TextField(db_column='loanType', null=True)
    loantypetext = TextField(db_column='loanTypeText', null=True)
    loanunfundedamount = TextField(db_column='loanUnfundedAmount', null=True)
    marital = TextField(null=True)
    maturityfull = TextField(db_column='maturityFull', null=True)
    maxinvestamount = TextField(db_column='maxInvestAmount', null=True)
    mininvestamount = TextField(db_column='minInvestAmount', null=True)
    monthlyincome = TextField(db_column='monthlyIncome', null=True)
    monthlyincomeinterval = TextField(db_column='monthlyIncomeInterval', null=True)
    nonprofit = TextField(db_column='nonProfit', null=True)
    notinfunding = TextField(db_column='notInFunding', null=True)
    pendingreview = TextField(db_column='pendingReview', null=True)
    purpose = TextField(null=True)
    purposetext = TextField(db_column='purposeText', null=True)
    rate = TextField(null=True)
    remainingpaymentscount = TextField(db_column='remainingPaymentsCount', null=True)
    repaymentmethod = TextField(db_column='repaymentMethod', null=True)
    reviewstatus = TextField(db_column='reviewStatus', null=True)
    status = TextField(null=True)
    subtype = TextField(db_column='subType', null=True)
    subtypetext = TextField(db_column='subTypeText', null=True)
    submissiondate = TextField(db_column='submissionDate', null=True)
    submissiontime = TextField(db_column='submissionTime', null=True)
    title = TextField(null=True)
    transferable = TextField(null=True)
    unverified = TextField(null=True)
    username = TextField(null=True)
    verified = TextField(null=True)
    verifiedworkemail = TextField(db_column='verifiedWorkEmail', null=True)
    verifiedworkphone = TextField(db_column='verifiedWorkPhone', null=True)
    verifybankaccount = TextField(db_column='verifyBankAccount', null=True)
    verifycomassetpaper = TextField(db_column='verifyComAssetPaper', null=True)
    verifycomassociationarticle = TextField(db_column='verifyComAssociationArticle', null=True)
    verifycomcommitmentletter = TextField(db_column='verifyComCommitmentLetter', null=True)
    verifycomeletricitybill = TextField(db_column='verifyComEletricityBill', null=True)
    verifycominvestigation = TextField(db_column='verifyComInvestigation', null=True)
    verifycomlicence = TextField(db_column='verifyComLicence', null=True)
    verifycommarketingcontract = TextField(db_column='verifyComMarketingContract', null=True)
    verifycomorganizationcode = TextField(db_column='verifyComOrganizationCode', null=True)
    verifycomotherfile = TextField(db_column='verifyComOtherFile', null=True)
    verifycomrevenueproof = TextField(db_column='verifyComRevenueProof', null=True)
    verifycomstatement = TextField(db_column='verifyComStatement', null=True)
    verifycredit = TextField(db_column='verifyCredit', null=True)
    verifyeducation = TextField(db_column='verifyEducation', null=True)
    verifyhouse = TextField(db_column='verifyHouse', null=True)
    verifyhukou = TextField(db_column='verifyHukou', null=True)
    verifyid = TextField(db_column='verifyId', null=True)
    verifyincome = TextField(db_column='verifyIncome', null=True)
    verifymarriage = TextField(db_column='verifyMarriage', null=True)
    weeklypaid = TextField(db_column='weeklyPaid', null=True)
    workcity = TextField(db_column='workCity', null=True)

    class Meta:
        db_table = 'p2p_dianrong'

# credentials = pika.PlainCredentials('chenxl', '123456')# 连接远程IP
#parameters = pika.ConnectionParameters('192.168.10.123', 5672, '/', credentials)#修改成服务器对应即可
connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()
channel.queue_declare(queue="bankFinancial_rong360_original_url",durable=True)

def callback(ch,method,properties,body):
	original_dict = json.loads(body)
	original_url = original_dict.get("url")
	try:
		url = "https://www.dianrong.com/api/v2/loans/" + str(original_url)
		header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
				  'Accept-Encoding': 'gzip, deflate, sdch',
				  'Accept-Language': 'zh-CN,zh;q=0.8',
				  'Connection': 'keep-alive',
				  'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
				  }
		r = requests.get(url, headers=header)
		html = json.loads(r.content)


		loanId = str(html.get("content").get("loanInfo").get("loanId"))
		notInFunding = str(html.get("content").get("loanInfo").get("notInFunding"))
		amount = str(html.get("content").get("loanInfo").get("amount"))
		expirationDate = str(html.get("content").get("loanInfo").get("expirationDate"))
		expirationTime = str(html.get("content").get("loanInfo").get("expirationTime"))
		expired = str(html.get("content").get("loanInfo").get("expired"))
		inFunding = str(html.get("content").get("loanInfo").get("inFunding"))
		issued = str(html.get("content").get("loanInfo").get("issued"))
		pendingReview = str(html.get("content").get("loanInfo").get("pendingReview"))
		hasCreditHistory = str(html.get("content").get("loanInfo").get("hasCreditHistory"))
		fundingPercentage = str(html.get("content").get("loanInfo").get("fundingPercentage"))
		fundingReceived = str(html.get("content").get("loanInfo").get("fundingReceived"))
		grade = str(html.get("content").get("loanInfo").get("grade"))
		hasSubmissionDate = str(html.get("content").get("loanInfo").get("hasSubmissionDate"))
		loanLength = str(html.get("content").get("loanInfo").get("loanLength"))
		loanLengthYear = str(html.get("content").get("loanInfo").get("loanLengthYear"))
		loanType = str(html.get("content").get("loanInfo").get("loanType"))
		purposeText = str(html.get("content").get("loanInfo").get("purposeText"))
		purpose = str(html.get("content").get("loanInfo").get("purpose"))
		rate = str(html.get("content").get("loanInfo").get("rate"))
		repaymentMethod = str(html.get("content").get("loanInfo").get("repaymentMethod"))
		reviewStatus = str(html.get("content").get("loanInfo").get("reviewStatus"))
		status = str(html.get("content").get("loanInfo").get("status"))
		submissionDate = str(html.get("content").get("loanInfo").get("submissionDate"))
		submissionTime = str(html.get("content").get("loanInfo").get("submissionTime"))
		title = str(html.get("content").get("loanInfo").get("title"))
		description = str(html.get("content").get("loanInfo").get("description"))
		borrowerDescription = str(html.get("content").get("loanInfo").get("borrowerDescription"))
		loanTypeText = str(html.get("content").get("loanInfo").get("loanTypeText"))
		loanTimeRemaining = str(html.get("content").get("loanInfo").get("loanTimeRemaining"))
		loanUnfundedAmount = str(html.get("content").get("loanInfo").get("loanUnfundedAmount"))
		investorCount = str(html.get("content").get("loanInfo").get("investorCount"))
		loanStatus = str(html.get("content").get("loanInfo").get("loanStatus"))
		nonProfit = str(html.get("content").get("loanInfo").get("nonProfit"))
		guaranteed = str(html.get("content").get("loanInfo").get("guaranteed"))
		collateralized = str(html.get("content").get("loanInfo").get("collateralized"))
		dailyPaid = str(html.get("content").get("loanInfo").get("dailyPaid"))
		weeklyPaid = str(html.get("content").get("loanInfo").get("weeklyPaid"))
		cashGuaranteed = str(html.get("content").get("loanInfo").get("cashGuaranteed"))
		transferable = str(html.get("content").get("loanInfo").get("transferable"))
		maxInvestAmount = str(html.get("content").get("loanInfo").get("maxInvestAmount"))
		minInvestAmount = str(html.get("content").get("loanInfo").get("minInvestAmount"))
		remainingPaymentsCount = str(html.get("content").get("loanInfo").get("remainingPaymentsCount"))
		subType = str(html.get("content").get("loanInfo").get("subType"))
		subTypeText = str(html.get("content").get("loanInfo").get("subTypeText"))
		maturityFull = str(html.get("content").get("loanInfo").get("maturityFull"))

		username = str(html.get("content").get("borrowerInfo").get("username"))
		borrowerGender = str(html.get("content").get("borrowerInfo").get("borrowerGender"))
		age = str(html.get("content").get("borrowerInfo").get("age"))
		marital = str(html.get("content").get("borrowerInfo").get("marital"))
		companyCity = str(html.get("content").get("borrowerInfo").get("companyCity"))
		companyType = str(html.get("content").get("borrowerInfo").get("companyType"))
		companyIndustry = str(html.get("content").get("borrowerInfo").get("companyIndustry"))
		companySize = str(html.get("content").get("borrowerInfo").get("companySize"))
		jobTime = str(html.get("content").get("borrowerInfo").get("jobTime"))
		jobTitle = str(html.get("content").get("borrowerInfo").get("jobTitle"))
		workCity = str(html.get("content").get("borrowerInfo").get("workCity"))
		childrenStatus = str(html.get("content").get("borrowerInfo").get("childrenStatus"))
		educationLeve = str(html.get("content").get("borrowerInfo").get("educationLeve"))

		verifyId = str(html.get("content").get("metadataInfo").get("verifyId"))
		verifyCredit = str(html.get("content").get("metadataInfo").get("verifyCredit"))
		verifyHukou = str(html.get("content").get("metadataInfo").get("verifyHukou"))
		verifyIncome = str(html.get("content").get("metadataInfo").get("verifyIncome"))
		verifyHouse = str(html.get("content").get("metadataInfo").get("verifyHouse"))
		verifyMarriage = str(html.get("content").get("metadataInfo").get("verifyMarriage"))
		verifyEducation = str(html.get("content").get("metadataInfo").get("verifyEducation"))
		verifyBankAccount = str(html.get("content").get("metadataInfo").get("verifyBankAccount"))
		verifiedWorkEmail = str(html.get("content").get("metadataInfo").get("verifiedWorkEmail"))
		verifiedWorkPhone = str(html.get("content").get("metadataInfo").get("verifiedWorkPhone"))
		verifyComStatement = str(html.get("content").get("metadataInfo").get("verifyComStatement"))
		verifyComRevenueProof = str(html.get("content").get("metadataInfo").get("verifyComRevenueProof"))
		verifyComLicence = str(html.get("content").get("metadataInfo").get("verifyComLicence"))
		verifyComOrganizationCode = str(html.get("content").get("metadataInfo").get("verifyComOrganizationCode"))
		verifyComCommitmentLetter = str(html.get("content").get("metadataInfo").get("verifyComCommitmentLetter"))
		verifyComMarketingContract = str(html.get("content").get("metadataInfo").get("verifyComMarketingContract"))
		verifyComEletricityBill = str(html.get("content").get("metadataInfo").get("verifyComEletricityBill"))
		verifyComAssetPaper = str(html.get("content").get("metadataInfo").get("verifyComAssetPaper"))
		verifyComAssociationArticle = str(html.get("content").get("metadataInfo").get("verifyComAssociationArticle"))
		verifyComInvestigation = str(html.get("content").get("metadataInfo").get("verifyComInvestigation"))
		verifyComOtherFile = str(html.get("content").get("metadataInfo").get("verifyComOtherFile"))
		verified = str(html.get("content").get("metadataInfo").get("verified"))#
		unverified = str(html.get("content").get("metadataInfo").get("unverified"))#

		monthlyIncome = str(html.get("content").get("creditInfo").get("monthlyIncome"))
		monthlyIncomeInterval = str(html.get("content").get("creditInfo").get("monthlyIncomeInterval"))
		try:
			database.connect()
			DR = P2PDianrong.insert(age = age,amount = amount,borrowerdescription= borrowerDescription,borrowergender = borrowerGender,cashguaranteed = cashGuaranteed,childrenstatus =childrenStatus,
								collateralized = collateralized,companycity = companyCity,companyindustry = companyIndustry,companysize = companySize,companytype=companyType,dailypaid=dailyPaid,
								description = description,educationleve=educationLeve,expirationdate=expirationDate,expirationtime=expirationTime,expired = expired,fundingpercentage=fundingPercentage,
								fundingreceived = fundingReceived,grade = grade,guaranteed =guaranteed,hascredithistory =hasCreditHistory,hassubmissiondate = hasSubmissionDate,infunding =inFunding,
								investorcount = investorCount,issued = issued,jobtime= jobTime,jobtitle =jobTitle,loanid = loanId,loanlength = loanLength,loanlengthyear = loanLengthYear,
								loanstatus = loanStatus,loantimeremaining = loanTimeRemaining,loantype = loanType,loantypetext=loanTypeText,loanunfundedamount=loanUnfundedAmount,marital=marital,
								maturityfull=maturityFull,maxinvestamount=maxInvestAmount,mininvestamount=minInvestAmount,monthlyincome=monthlyIncome,monthlyincomeinterval=monthlyIncomeInterval,
								nonprofit=nonProfit,notinfunding=notInFunding,pendingreview=pendingReview,purpose=purpose,purposetext=purposeText,rate= rate,remainingpaymentscount=remainingPaymentsCount,
								repaymentmethod=repaymentMethod,reviewstatus=reviewStatus,status=status,subtype=subType,subtypetext=subTypeText,submissiondate=submissionDate,submissiontime=submissionTime,
								title = title,transferable = transferable,unverified = unverified,username=username,verified=verified,verifiedworkemail=verifiedWorkEmail,verifiedworkphone=verifiedWorkPhone,
								verifybankaccount=verifyBankAccount,verifycomassetpaper=verifyComAssetPaper,verifycomassociationarticle=verifyComAssociationArticle,verifycomcommitmentletter=verifyComCommitmentLetter,
								verifycomeletricitybill=verifyComEletricityBill,verifycominvestigation=verifyComInvestigation,verifycomlicence=verifyComLicence,verifycommarketingcontract=verifyComMarketingContract,
								verifycomorganizationcode=verifyComOrganizationCode,verifycomotherfile=verifyComOtherFile,verifycomrevenueproof=verifyComRevenueProof,verifycomstatement=verifyComStatement,
								verifycredit=verifyCredit,verifyeducation=verifyEducation,verifyhouse=verifyHouse,verifyhukou=verifyHukou,verifyid=verifyId,verifyincome=verifyIncome,
								verifymarriage=verifyMarriage,weeklypaid=weeklyPaid,workcity=workCity)
			DR.execute()
			print "success %s db"% str(loanId)
			database.commit()
			database.close()
			time.sleep(0.1)

		except Exception,e:
			print e
	except Exception,e:
		print e
	ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,     #如果收到消息，就调用callback函数处理消息
                      queue="p2p_dianRong",
                      )

print("Now waiting for p2p_dianRong's message. To exit press CTRL+C")
channel.start_consuming()   #begin to receive message
"""
数据库同步
"""
conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data_url',
						   charset='utf8')
cur = conn.cursor()

updateSql = "UPDATE all_ori_product_url SET statues ='DONE' where url_source='点融网' and statues='NEW' "
cur.excute(updateSql)
conn.commit()
cur.close()
print "all is done "