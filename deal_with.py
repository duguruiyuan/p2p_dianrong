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
database = MySQLDatabase('pretreatment_data', **{'host': '192.168.10.203', 'password': 'caiji@123', 'port': 3306, 'user': 'caiji'})
class UnknownField(object):
    def __init__(self, *_, **__): pass
class BaseModel(Model):
    class Meta:
        database = database
class Credit(BaseModel):
    age = CharField(null=True)
    apr = DecimalField()
    borrowergender = CharField(db_column='borrowerGender', null=True)
    category = CharField(null=True)
    childrenstatus = CharField(db_column='childrenStatus', null=True)
    code = CharField(primary_key=True)
    companycity = CharField(db_column='companyCity', null=True)
    companyindustry = CharField(db_column='companyIndustry', null=True)
    companysize = CharField(db_column='companySize', null=True)
    companytype = CharField(db_column='companyType', null=True)
    credit_statues = CharField(null=True)
    educationleve = CharField(db_column='educationLeve', null=True)
    full_name = CharField(null=True)
    investment_term = IntegerField(null=True)
    investment_term_show = CharField(null=True)
    investment_term_unit = CharField(null=True)
    is_recommended = UnknownField(null=True)  # bit
    issuer = CharField(null=True)
    issuer_detail = BigIntegerField(db_column='issuer_detail_id', null=True)
    issuer_grade = DecimalField(null=True)
    jobtime = CharField(db_column='jobTime', null=True)
    jobtitle = CharField(db_column='jobTitle', null=True)
    liquidity_grade = DecimalField(null=True)
    loantypetext = CharField(db_column='loanTypeText', null=True)
    loan_amount = DecimalField(null=True)
    marital = CharField(null=True)
    maturity_date = CharField(null=True)
    minimum_investment = DecimalField()
    monthlyincome = CharField(db_column='monthlyIncome', null=True)
    monthlyincomeinterval = CharField(db_column='monthlyIncomeInterval', null=True)
    name = CharField()
    product_url = TextField(null=True)
    project_description = CharField(null=True)
    purchase_date = DateTimeField(null=True)
    repayment_method = CharField(null=True)
    repayment_method_text = CharField(null=True)
    risk_level = CharField(null=True)
    safe_grade = DecimalField(null=True)
    source = TextField(null=True)
    synthetical_grade = DecimalField(null=True)
    term = CharField(null=True)
    username = CharField(null=True)
    workcity = CharField(db_column='workCity', null=True)

    class Meta:
        db_table = 'credit'
        indexes = (
            (('code', 'category'), True),
        )
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
def dealWithData():
	conn = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='original_data',
						   charset='utf8')
	cur = conn.cursor()
	sql = "SELECT * from p2p_dianrong where data_statues='DONE' and submissionDate !='None' and purposeText!='None'" #起标日和标题不为空
	main = pd.read_sql(sql, conn)
	data_tmp = pd.DataFrame()
	candidate_data = pd.DataFrame()
	code_wait_for_deal = map(lambda x:"DR"+str(x),main['loanId'])
	data_tmp['code'] = code_wait_for_deal
	data_tmp['name'] = main['title']
	fullname_wait_for_deal = []
	for lenN  in range(len(main['loanId'])):
		fullName = main['purposeText'][lenN]+u"  借款编号:"+str(main['loanId'][lenN])
		fullname_wait_for_deal.append(fullName)
	data_tmp['full_name'] = fullname_wait_for_deal
	investment_term_show_wait_for_deal_with = main['maturityFull']
	data_tmp['investment_term_show'] = investment_term_show_wait_for_deal_with
	data_tmp['category'] = "CREDIT"
	data_tmp['is_recommended'] =True
	data_tmp['issuer'] = u"点融网"
	data_tmp['loan_amount'] = main['amount']
	data_tmp["project_description"] = main['description']
	data_tmp['issuer_detail_id']= 7723
	data_tmp['source'] = u"点融网-陈鹤"
	def str2number(df, column, column_name, numberdict):
		df[column_name] = df[column].map(numberdict)
		return df
	"""
	EQUAL_LOAN("等额本息"),
	EQUAL_CAPITAL("等额本金"),
	INTEREST_BEFORE("先息后本"),
	ONCE("到期还本付息");
	"""

	candidate_data['repaymentMethod'] = main['repaymentMethod']
	repaymentMethod_to_number = {u'等额本息': 'EQUAL_LOAN', u'到期一次性还本付息': 'ONCE',u'每月还息,到期一次性还本': 'INTEREST_BEFORE',
								 u'等本等息': 'EQUAL_LOAN', u'特殊还款方式,每周扣款': 'SPECIAL_LOAN',u'每月还息，到期一次性还本': 'INTEREST_BEFORE',
								 u"到期一次性还本付息,固定利息":"ONCE",u"等额本息（管理费计入RPA）":"EQUAL_LOAN",u"每月还息,按季度还本":"SPECIAL_LOAN",
								 u"每月还息,按半年还本":"SPECIAL_LOAN"}
	repaymentMethod_to_number = pd.Series(repaymentMethod_to_number)
	str2number(candidate_data, 'repaymentMethod', u'还款方式', repaymentMethod_to_number)
	data_tmp['repayment_method'] = candidate_data[u'还款方式']
	data_tmp['minimum_investment'] =1
	data_tmp['product_url'] = "www.dianrong.com"
	submissionDate_wait_for_deal = map(lambda x:float(x)/1000.0,main['submissionDate'])
	purchase_date_wait_for_deal =[]
	for subDate in submissionDate_wait_for_deal:
		st = time.localtime(subDate)
		stb = time.strftime('%Y-%m-%d %H:%M:%S', st)
		purchase_date_wait_for_deal.append(stb)
	data_tmp['purchase_date'] = purchase_date_wait_for_deal
	apr_wait_for_deal =map(lambda x:float(x)*100.0,main['rate'])
	data_tmp['apr'] = apr_wait_for_deal
	candidate_data['riskLevel'] = main['grade']
	riskLevel_to_number = {'A1': 'LOW',"A2":"LOW","A3":"LOW","A4":"LOW","A5":"LOW",
						   "B1":"LOW","B2":"LOW","B3":"LOW","B4":"LOW","B5":"LOW",
						   "C1":"MIDDLE","C2":"MIDDLE","C3":"MIDDLE","C4":"MIDDLE","C5":"MIDDLE",
						   "D1":"MIDDLE","D2":'MIDDLE',"D3":"MIDDLE","D4":"MIDDLE","D5":"MIDDLE",
						   "E1":"HIGH","E2":"HIGH","E3":"HIGH","E4":"HIGH","E5":"HIGH",
						   "F1":"HIGH","F2":"HIGH","F3":"HIGH","F4":"HIGH","F5":"HIGH"}

	riskLevel_to_number = pd.Series(riskLevel_to_number)
	str2number(candidate_data,"riskLevel",u"风险等级",riskLevel_to_number)
	data_tmp['risk_level'] = candidate_data[u'风险等级']
	candidate_data['safe_grade'] = data_tmp['risk_level']

	safe_grade_to_number = {"LOW":5.0,"MIDDLE":4.0,"HIGH":3.0}
	safe_grade_to_number = pd.Series(safe_grade_to_number)
	str2number(candidate_data, "safe_grade", u"安全性", safe_grade_to_number)
	data_tmp['safe_grade'] = candidate_data[u'安全性']
	data_tmp['issuer_grade'] = 4.0
	investment_term_wait_for_deal = []
	investment_term_unit_wait_for_deal = []
	term_wait_for_deal=[]
	for invTrain in investment_term_show_wait_for_deal_with:
		if u"月" in invTrain:
			TermNum = float(invTrain.replace(u"个月",""))
			investment_term_wait_for_deal.append(TermNum)
			investment_term_unit_wait_for_deal.append("MONTH")
			if TermNum <= 12.0:
				term_wait_for_deal.append("SHORT")
			else:
				term_wait_for_deal.append("LONG")
		else:
			TermNum = float(invTrain.replace(u"天", ""))
			investment_term_wait_for_deal.append(TermNum)
			investment_term_unit_wait_for_deal.append("DAY")
			if TermNum <= 365.0:
				term_wait_for_deal.append("SHORT")
			else:
				term_wait_for_deal.append("LONG")
	data_tmp['investment_term'] = investment_term_wait_for_deal
	data_tmp['investment_term_unit'] = investment_term_unit_wait_for_deal
	data_tmp['term'] = term_wait_for_deal

	liquidity_grade_wait_for_deal =[]
	for liquG in investment_term_show_wait_for_deal_with:
		if u"月" in liquG:
			liquGNum = float(liquG.replace(u"个月",""))
			if liquGNum <= 1.0:
				liquidity_grade_wait_for_deal.append(5.0)
			elif liquGNum <=6.0:
				liquidity_grade_wait_for_deal.append(4.5)
			elif liquGNum<=12.0:
				liquidity_grade_wait_for_deal.append(4.0)
			elif liquGNum <= 36.0:
				liquidity_grade_wait_for_deal.append(3.5)
			else:
				liquidity_grade_wait_for_deal.append(3.0)
		else:
			liquGNum = float(liquG.replace(u"天", ""))
			if liquGNum <= 30.0:
				liquidity_grade_wait_for_deal.append(5.0)
			elif liquGNum <=180.0:
				liquidity_grade_wait_for_deal.append(4.5)
			elif liquGNum<=365.0:
				liquidity_grade_wait_for_deal.append(4.0)
			elif liquGNum <= 1080.0:
				liquidity_grade_wait_for_deal.append(3.5)
			else:
				liquidity_grade_wait_for_deal.append(3.0)
	data_tmp['liquidity_grade'] = liquidity_grade_wait_for_deal

	synthetical_grade = list()
	for i in range(len(data_tmp['code'])):
		x = float(int(data_tmp['issuer_grade'][i]) * 0.3 + int(data_tmp['liquidity_grade'][i]) * 0.3 + int(
			data_tmp['issuer_grade'][i]) * 0.4)
		synthetical_grade.append(x)
	data_tmp['synthetical_grade'] = synthetical_grade
	hah = map(lambda x:str(x).replace("None","0.0"),main['expirationDate'])
	expirationDate_wait_for_deal = map(lambda x: float(x) / 1000.0,hah)
	maturity_date_wait_for_deal = []
	for exDate in expirationDate_wait_for_deal:
		st1 = time.localtime(exDate)
		stb1 = time.strftime('%Y-%m-%d %H:%M:%S', st1)
		maturity_date_wait_for_deal.append(stb1)
	data_tmp['maturity_date'] = maturity_date_wait_for_deal

	data_tmp['username'] = main['username']
	data_tmp['borrowerGender'] = main['borrowerGender']
	data_tmp['age'] = main['age']
	data_tmp['credit_statues'] = main['status']
	data_tmp['repayment_method_text'] = main['repaymentMethod']
	data_tmp['loanTypeText'] = main['loanTypeText']
	data_tmp['marital'] = main['marital']
	data_tmp['companyCity'] = main['companyCity']
	data_tmp['companyType'] = main['companyType']
	data_tmp['companyIndustry'] = main['companyIndustry']
	data_tmp['companySize'] = main['companySize']
	data_tmp['jobTime'] = main['jobTime']
	data_tmp['jobTitle'] = main['jobTitle']
	data_tmp['workCity'] = main['workCity']
	data_tmp['educationLeve'] = main['educationLeve']
	data_tmp['childrenStatus'] = main['childrenStatus']
	data_tmp['monthlyIncome'] = main['monthlyIncome']
	data_tmp['monthlyIncomeInterval'] = main['monthlyIncomeInterval']

	cc = 0
	while cc < len(data_tmp['code']):
		try:
			database.connect()
			CC = Credit.insert(age = data_tmp['age'][cc],apr = data_tmp['apr'][cc],borrowergender=data_tmp['borrowerGender'][cc],
							   category = data_tmp['category'][cc],childrenstatus=data_tmp['childrenStatus'][cc],
							   code= data_tmp['code'][cc],companycity=data_tmp['companyCity'][cc],companyindustry=data_tmp['companyIndustry'][cc],
							   companysize = data_tmp['companySize'][cc],companytype=data_tmp['companyType'][cc],credit_statues =data_tmp['credit_statues'][cc],
							   educationleve = data_tmp['educationLeve'][cc],full_name=data_tmp['full_name'][cc],investment_term=data_tmp['investment_term'][cc],
							   investment_term_show=data_tmp['investment_term_show'][cc],investment_term_unit=data_tmp['investment_term_unit'][cc],
							   issuer=data_tmp['issuer'][cc],issuer_detail=data_tmp['issuer_detail_id'][cc],issuer_grade=data_tmp['issuer_grade'][cc],
							   jobtime =data_tmp['jobTime'][cc],jobtitle = data_tmp['jobTitle'][cc],liquidity_grade=data_tmp['liquidity_grade'][cc],
							   loantypetext = data_tmp['loanTypeText'][cc],loan_amount =data_tmp['loan_amount'][cc],marital=data_tmp['marital'][cc],
							   maturity_date = data_tmp['maturity_date'][cc],minimum_investment=data_tmp['minimum_investment'][cc],
							   monthlyincome =data_tmp['monthlyIncome'][cc],monthlyincomeinterval=data_tmp['monthlyIncomeInterval'][cc],
							   name = data_tmp['name'][cc],product_url =data_tmp['product_url'][cc],project_description=data_tmp['project_description'][cc],
							   purchase_date =data_tmp['purchase_date'][cc],repayment_method=data_tmp['repayment_method'][cc],
							   repayment_method_text= data_tmp['repayment_method_text'][cc],risk_level=data_tmp['risk_level'][cc],
							   safe_grade =data_tmp['safe_grade'][cc],source=data_tmp['source'][cc],synthetical_grade=data_tmp['synthetical_grade'][cc],
							   term =data_tmp['term'][cc],username = data_tmp['username'][cc],workcity = data_tmp['workCity'][cc])

			CC.execute()
			print "success %s db" % data_tmp['name'][cc]
			database.commit()
			database.close()
			time.sleep(0.1)
		except Exception,e:
			print e
		cc += 1

	print "now U can exit it "
	"""
	updateSql = "UPDATE p2p_dianrong SET data_statues='DONE'"
	cur.excute(updateSql)
	conn.commit()
	#cur.close()
"""
if __name__ =="__main__":
	dealWithData()
	print "now U can connect zhidian"