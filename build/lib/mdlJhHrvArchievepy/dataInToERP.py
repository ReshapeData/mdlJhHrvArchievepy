#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyrda.dbms.rds import RdClient
from k3cloud_webapi_sdk.main import K3CloudApiSdk
import json
from .voucher import *

def Fentry_model(data):
    '''
    分录数据格式
    :return:
    '''

    entry={
            "FEXPLANATION": str(data["FNotes"]),

            "FACCOUNTID": {
                "FNumber": str(data["FSubjectNumber"])
            },

            "FDetailID": {
                "FDETAILID__FFlex5": {
                    # 部门
                    "FNumber": str(data["FDeptNumber"])
                },
                "FDETAILID__FF100015": {
                    # 责任中心
                    "FNumber": str(data["FWorkCenterNumber"])
                },
                "FDETAILID__FF100016": {
                    # 重分类
                    "FNumber": str(data["FAcctreClassNumber"])
                },
                "FDETAILID__FF100002": {
                    # 银行账号
                    "FNumber": str(data["FBankAccount"])
                },
                "FDETAILID__FF100005": {
                    # 研发项目
                    "FNumber": str(data["FProjectNumber"])
                },
                "FDETAILID__FF100006": {
                    # 往来单位
                    "FNumber": str(data["FDealingUnitNumber"])
                },
                "FDETAILID__FFlex4": {
                    # 供应商
                    "FNumber": str(data["FSupplierNumber"])
                }
            },
            "FCURRENCYID": {
                # 币别
                "FNumber": "PRE001"
            },
            "FEXCHANGERATETYPE": {
                "FNumber": "HLTX01_SYS"
            },
            "FEXCHANGERATE": 1.0,
            "FDEBIT": data["allamountBorrow"],
            # 借方金额
            "FCREDIT": data["allamountLoan"]
            #贷方金额
        }


    return entry


def data_splicing(data):
    '''
    将订单内的物料进行遍历组成一个列表，然后将结果返回给 FSaleOrderEntry
    :param data: 数据源
    :return:
    '''

    list=[]

    for i in data:

        result=Fentry_model(i)

        if result:

            list.append(result)

        else:

            return []

    return list


def model(api_sdk,data,app):
    '''
    数据格式
    :param api_sdk: 调用金蝶API接口对象
    :param data: 数据源
    :param app: 中台数据库执行对象
    :return:
    '''

    for i in data:
    
        json_model={
                "Model": {
                    "FVOUCHERID": 0,
                    "FAccountBookID": {
                        "FNumber": str(i[0]['FAccountBookID'])
                    },
                    "FDate": str(i[0]['FDate']),
                    "FBUSDATE": str(i[0]['FDate']),
                    # "FDate": "2023-05-31",
                    # "FBUSDATE": "2023-05-31",
                    "FVOUCHERGROUPID": {
                        "FNumber": "PRE001"
                    },
                    "FVOUCHERGROUPNO": "540",
                    "FISADJUSTVOUCHER": False,
                    "FYEAR": int(float(i[0]['FYear'])),
                        "FDocumentStatus": "Z",
                    "FSourceBillKey": {
                        "FNumber": "78050206-2fa6-40e3-b7c8-bd608146fa38"
                    },
                    "FPERIOD": int(float(i[0]['FMonth'])),
                    # "FPERIOD": 5,
                    "FISREDWRITEOFF": False,
                    "FHasAttachments": False,
                    "FEntity": data_splicing(i)
                }
        }

        res=json.loads(api_sdk.Save("GL_VOUCHER",json_model))

        print(type(res['Result']['ResponseStatus']['IsSuccess']))


        if bool(res['Result']['ResponseStatus']['IsSuccess']):

            log_upload(app,str(i[0]['FBillNO']),"1","记账凭证已生成")

        else:
            log_upload(app, str(i[0]['FBillNO']), "2", "记账凭证生成失败"+res['Result']['ResponseStatus']['Errors'][0]['Message'])


    return "程序运行完成"


    
def dataSourceBillNo_query(app,FYear,FMonth):
    '''
    单据编号查询
    :param app: 数据库执行对象
    :param FYear: 年
    :param FMonth: 月
    :return:
    '''

    FYear=str(FYear)+".0"

    FMonth =str(FMonth) + ".0"

    sql=f"select distinct FBillNO from rds_hrv_ods_ds_middleTable where FIsdo=0 and FYear='{FYear}' and FMonth='{FMonth}'"

    res=app.select(sql)

    return res



def getClassfyData(app3, code):
    '''
    通过单据编号获得数据源
    :param app3:
    :param code: 数据库执行对象
    :return:
    '''


    sql = f"""select * from rds_hrv_ods_ds_middleTable where FBillNO='{code['FBillNO']}' order by FSeq asc"""

    res = app3.select(sql)

    return res


def fuz(app3, codeList):
    '''
    通过编码分类，将分类好的数据装入列表
    :param app3: 数据库执行对象
    :param codeList: 单据编号列表
    :return:
    '''


    singleList = []

    for i in codeList:

        data = getClassfyData(app3, i)

        singleList.append(data)

    return singleList


def classification_process(app3, data):
    '''
    将编码进行去重，然后进行分类
    :param app3: 数据库执行对象
    :param data: 数据源
    :return:
    '''

    res = fuz(app3, data)

    return res


def log_upload(app,FNumber,FIsdo,FMessage):
    '''
    更新ods表状态和日志
    :param app: 数据库执行对象
    :param FNumber: 单据编号
    :param FIsdo: 状态
    :param FMessage: 日志信息
    :return:
    '''

    sql=f"""update a set a.FIsdo='{FIsdo}',a.FMessage='{FMessage}'  
    from rds_hrv_ods_ds_middleTable a where a.FBillNO='{FNumber}'"""

    app.update(sql)


def src_clear(app, tablename):
    '''
    将SRC表中的数据清空
    :param app:
    :return:
    '''

    sql = f"truncate table {tablename}"

    app.update(sql)

def middleTableSrctoOds(app):
    '''
    中间表SRC-ODS
    :param app:
    :return:
    '''

    sql = """
    INSERT INTO rds_hrv_ods_ds_middleTable  
                SELECT 
				FDate,
				FYear,
				FMonth,
				FBillNO,
				FSeq,
				FNumber,
				FName,
				FTaxDeclarationOrg,
				FExpenseOrgID,
				FCategoryType,
				FNotes,
				FAccountBookID,
				FDealingUnitName,
				FDealingUnitNumber,
				FSupplierName,
				FSupplierNumber,
				FAccountName,
				FHightechDept,
				FSubjectNumber,
				FSubjectName,
				FLexitemProperty,
				FDeptNumber,
				FDeptName,
				FRdProject,
				FProjectNumber,
				FWorkCenterNumber,
				FAcctreClassNumber,
				FBankAccount,
				allamountBorrow,
				allamountLoan,
				FSettleMethod,
				FSettleNumber,
				FMessage,
				FIsdo,
				FWorkCenterName,
				FAcctreClassName,
				FSeqNew
                from (select * from rds_hrv_src_ds_middleTable where FIsdo=0) a
                where not exists
                (select * from rds_hrv_ods_ds_middleTable b 
                    where A.FBillNO = B.FBillNO)
    """
    app.update(sql)

    src_clear(app, "rds_hrv_src_ds_middleTable")



def voucher_save(FToken,FYear,FMonth,option):
    '''
    凭证生成
    :param FToken: 中台数据库密钥
    :param FYear: 年
    :param FMonth: 月
    :param option: ERP数据库对象
    :return:
    '''

    app = RdClient(FToken)

    api_sdk = K3CloudApiSdk()

    # 新账套

    middleTableSrctoOds(app)

    # Main.src_clear(app,"")


    api_sdk.InitConfig(option[0]['acct_id'], option[0]['user_name'], option[0]['app_id'],
                       option[0]['app_sec'], option[0]['server_url'])

    FBillNoRes=dataSourceBillNo_query(app,FYear,FMonth)

    if FBillNoRes != []:

        dataSource = classification_process(app, FBillNoRes)

        res=model(api_sdk=api_sdk,data=dataSource,app=app)

        return res

    else:

        return "没有需要同步至ERP的凭证"



