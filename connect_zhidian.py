# -*- coding:utf8 -*-
import pandas as pd
from peewee import *

database = MySQLDatabase('connect_zhidian', **{'host': '192.168.10.203', 'password': 'caiji@123', 'port': 3306, 'user': 'caiji'})

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = database

class BankFinancialProduct(BaseModel):
    bank = BigIntegerField(db_column='bank_id', null=True)
    currency = CharField(null=True)
    custodian = CharField(null=True)
    effective_date = DateField()
    expired_date = DateField(null=True)
    financial_type = CharField(null=True)
    id = BigIntegerField(primary_key=True)
    industry_benchmark = CharField(null=True)
    max_issuance = IntegerField(null=True)
    product = BigIntegerField(db_column='product_id', index=True, null=True)
    product_ptr = BigIntegerField(db_column='product_ptr_id', null=True)
    recruitment = CharField(null=True)
    redeemable = UnknownField(null=True)  # bit

    class Meta:
        db_table = 'bank_financial_product'

class Bond(BaseModel):
    bond_type = CharField(null=True)
    coupon_rate = DecimalField(null=True)
    currency = CharField(null=True)
    due_date = DateField(null=True)
    face_value = DecimalField(null=True)
    frequency_interest = CharField(null=True)
    id = BigIntegerField(primary_key=True)
    issue_price = DecimalField(null=True)
    product = BigIntegerField(db_column='product_id', null=True)
    product_ptr = BigIntegerField(db_column='product_ptr_id', null=True)
    release_deadline = DateField(null=True)
    release_startday = DateField(null=True)
    value_day = DateField(null=True)

    class Meta:
        db_table = 'bond'

class CreditProduct(BaseModel):
    id = BigIntegerField(primary_key=True)
    loan_amount = DecimalField(null=True)
    maturity_date = CharField(null=True)
    pid = CharField(null=True)
    product = BigIntegerField(db_column='product_id', null=True)
    product_ptr = BigIntegerField(db_column='product_ptr_id', null=True)
    project_description = CharField(null=True)
    repayment_method = CharField(null=True)

    class Meta:
        db_table = 'credit_product'

class FundDividend(BaseModel):
    amount = DecimalField()
    created_date = DateTimeField()
    dividend_date = DateField(index=True)
    id = BigIntegerField(primary_key=True)
    public_fund = BigIntegerField(db_column='public_fund_id', index=True)
    share = DecimalField()

    class Meta:
        db_table = 'fund_dividend'
        indexes = (
            (('public_fund', 'dividend_date'), True),
        )

class FundNetValue(BaseModel):
    id = BigIntegerField(primary_key=True)
    net_value = DecimalField(null=True)
    net_value_date = DateField(null=True)
    public_fund = BigIntegerField(db_column='public_fund_id', index=True)
    rise_decline = DecimalField(null=True)

    class Meta:
        db_table = 'fund_net_value'

class FundSplit(BaseModel):
    created_date = DateTimeField()
    follow_net_value = DecimalField()
    id = BigIntegerField(primary_key=True)
    previous_net_value = DecimalField()
    public_fund = BigIntegerField(db_column='public_fund_id', index=True)
    scale = DecimalField()
    split_date = DateField(index=True)

    class Meta:
        db_table = 'fund_split'
        indexes = (
            (('public_fund', 'split_date'), True),
        )

class IssuerDetail(BaseModel):
    assets_under_management = DecimalField(null=True)
    bond_level = CharField(null=True)
    code = CharField(null=True)
    create_time = DateTimeField(null=True)
    id = BigIntegerField(primary_key=True)
    initial = CharField(index=True, null=True)
    is_disable = UnknownField(null=True)  # bit
    is_financial_institution = UnknownField(null=True)  # bit
    is_hot = UnknownField(index=True, null=True)  # bit
    issuer_full_name = CharField(null=True)
    issuer_introduce = TextField(null=True)
    issuer_name = CharField(null=True)
    issuer_rank = CharField(null=True)
    issuer_type = CharField(index=True, null=True)
    issuer_url = TextField(null=True)
    login_name = CharField(null=True)
    password = CharField(null=True)
    registered_capital = DecimalField(null=True)

    class Meta:
        db_table = 'issuer_detail'

class MonetaryFundApr(BaseModel):
    apr = DecimalField(null=True)
    apr_date = DateField(index=True)
    avg_apr = DecimalField()
    copies_benefit = DecimalField()
    created_date = DateTimeField()
    id = BigIntegerField(primary_key=True)
    public_fund = BigIntegerField(db_column='public_fund_id', index=True)

    class Meta:
        db_table = 'monetary_fund_apr'

class Product(BaseModel):
    apr = DecimalField()
    apr_updated_date = DateField(null=True)
    category = CharField(index=True, null=True)
    code = CharField(index=True)
    currency = CharField(null=True)
    data_update_time = DateTimeField(null=True)
    extra_term = IntegerField(null=True)
    full_name = CharField(null=True)
    id = BigIntegerField(primary_key=True)
    investment_term = IntegerField(null=True)
    investment_term_show = CharField(null=True)
    investment_term_unit = CharField(null=True)
    investment_type = CharField(null=True)
    is_recommended = UnknownField(null=True)  # bit
    issuer = CharField(null=True)
    issuer_detail = BigIntegerField(db_column='issuer_detail_id', null=True)
    issuer_grade = DecimalField(null=True)
    latest_year_apr = CharField(null=True)
    liquidity_grade = DecimalField(null=True)
    minimum_investment = DecimalField()
    name = CharField(index=True)
    product_url = TextField(null=True)
    purchase_date = DateTimeField(null=True)
    review = CharField(null=True)
    risk_level = CharField(null=True)
    risk_type = CharField(null=True)
    safe_grade = DecimalField(null=True)
    source = TextField(null=True)
    synthetical_grade = DecimalField(null=True)
    term = CharField(null=True)

    class Meta:
        db_table = 'product'
        indexes = (
            (('code', 'category'), True),
            (('term', 'risk_level'), False),
        )

class PublicFund(BaseModel):
    company_introduce = TextField(null=True)
    established_time = CharField()
    fund_manager = CharField(null=True)
    fund_size = CharField(null=True)
    fund_type = CharField(index=True, null=True)
    manager_introduce = TextField(null=True)
    product_code = CharField(null=True)
    product = BigIntegerField(db_column='product_id', index=True, null=True)
    product_ptr = BigIntegerField(db_column='product_ptr_id', null=True)
    subscription_fee = TextField(null=True)
    today_netvalue = DecimalField(null=True)
    today_rise_decline = DecimalField(null=True)
    update_date = DateField(null=True)

    class Meta:
        db_table = 'public_fund'

class Trust(BaseModel):
    financiers = TextField(null=True)
    id = BigIntegerField(primary_key=True)
    interest_distribution = CharField(null=True)
    issuance = CharField(null=True)
    money_managers = TextField(null=True)
    mortgage = CharField(null=True)
    mortgage_rate = DecimalField(null=True)
    product = BigIntegerField(db_column='product_id', index=True, null=True)
    product_ptr = BigIntegerField(db_column='product_ptr_id', null=True)
    repaying_source = TextField(null=True)
    risk_control_measures = TextField(null=True)
    trust_type = CharField(null=True)
    use_of_funds = TextField(null=True)

    class Meta:
        db_table = 'trust'



def dianRong2db():
    import time
    import MySQLdb
    database.connect()
    conPrepared = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='pretreatment_data',
                                  charset='utf8')
    mainSql = 'SELECT * FROM credit WHERE category = "CREDIT" and source="点融网-陈鹤"'
    data_tmp = pd.read_sql(mainSql, conPrepared)
    connInsert = MySQLdb.connect(host='192.168.10.203', user='caiji', passwd='caiji@123', db='connect_zhidian',
                                 charset='utf8')
    i = 0
    while i < len(data_tmp['code']):
        findataSql = 'SELECT id FROM product WHERE code = "%s" and category="%s"' % (data_tmp['code'][i], data_tmp['category'][i])
        curInsert = connInsert.cursor()
        curInsert.execute(findataSql)
        returnID = curInsert.fetchall()
        if len(returnID) ==0:
            print "this is new data"
            panda = Product(apr=data_tmp['apr'][i],category=data_tmp['category'][i], code=data_tmp['code'][i],
                            currency="RMB",
                            full_name=data_tmp['full_name'][i],investment_term=data_tmp['investment_term'][i],
                            investment_term_show=data_tmp['investment_term_show'][i],
                            investment_term_unit=data_tmp['investment_term_unit'][i], issuer=data_tmp['issuer'][i],
                            issuer_detail=data_tmp['issuer_detail_id'][i],
                            issuer_grade=data_tmp['issuer_grade'][i],liquidity_grade=data_tmp['liquidity_grade'][i],
                            minimum_investment=data_tmp['minimum_investment'][i],
                            name=data_tmp['name'][i], product_url=data_tmp['product_url'][i],
                            purchase_date=data_tmp['purchase_date'][i],
                            risk_level=data_tmp['risk_level'][i],
                            safe_grade=data_tmp['safe_grade'][i], source=data_tmp['source'][i],
                            synthetical_grade=data_tmp['synthetical_grade'][i], term=data_tmp['term'][i],
                            is_recommended=True)
            panda.save()
            returnProductId = (Product.select().where(Product.code == data_tmp['code'][i]).get()).id
            panda2Credit = CreditProduct(loan_amount=data_tmp['loan_amount'][i],maturity_date=data_tmp['maturity_date'][i],product=returnProductId,
                                        project_description=data_tmp['project_description'][i],repayment_method=data_tmp['repayment_method'][i])

            panda2Credit.save()

            print "success to db:", data_tmp['code'][i]
        else:
            print "that`s exist", data_tmp['code'][i]
        i += 1
        time.sleep(0.2)
    print "now U can exit "
    database.close()

if __name__ =="__main__":

    dianRong2db()
