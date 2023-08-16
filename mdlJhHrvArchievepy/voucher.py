#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyrda.dbms.rds import RdClient
import pandas as pd
import numpy as np
import time
from .odsToStd import *
from .srcToOds import *
from .dataInToERP import *


def voucher_query(app, FNumber):
    '''
    凭证模板表与科目表结合，将科目字段带到模板表中
    :param app:数据库操作对象
    :param FNumber: 单据编号
    :return:
    '''

    sql = f"""
        select a.FNumber,a.FName,a.FCategoryType,a.FSeq,a.FNotes,
        a.FSubjectNumber,a.FSubjectName,a.FAccountNumber,a.FLexitemProperty,
        a.FObtainSource,a.FAccountBorrow,a.FAccountLoan,a.FAccountBorrowSql,FAccountLoanSql,
        a.FSettleMethod,a.FSettleNumber,a.FAccountBookID,a.FFirstAcct,
        b.FFirstAcct as FAccount,b.FLexitemProperty,b.FAccountName
        from rds_hrv_ods_tpl_voucher a
        inner join rds_hrv_ods_md_acct b
        on a.FSubjectNumber=b.FAccountNumber 
        where a.FNumber = '{FNumber}'
        order by FSeq asc
    """

    res = app.select(sql)

    df = pd.DataFrame(res)

    return df


def tableName_query(app, FCategory):
    '''
    根据类型查找相应的表名
    :param app:数据库操作对象
    :param FCategory: 单据类型
    :return:
    '''

    sql = f"""select FTableName from rds_hrv_ods_md_categoryTypeTable 
    where FCategoryType='{FCategory}'"""

    res = app.select(sql)

    if res:
        return res[0]["FTableName"]


def category_query(app, FNumber):
    '''
    单据类型查询根据单据编号
    :param app: 数据库操作对象
    :param FNumber: 单据编号
    :return:
    '''

    sql = f"select FCategoryType from rds_hrv_ods_ds_documentNumber where FNumber='{FNumber}'"

    res = app.select(sql)

    return res


def categorySecond_query(app, FYear, FMonth):
    '''
    单据类型查询,根据年月
    :param app: 数据库操作对象
    :param FYear: 年
    :param FMonth: 月
    :return:
    '''

    sql = f"select FCategoryType,FNumber from rds_hrv_ods_ds_documentNumber where FYear='{FYear}' and FMonth='{FMonth}'"

    res = app.select(sql)

    return res


def datasource_query(app, FTableName, FNumber):
    '''
    通过表名和单号查找数据源
    :param app: 数据库操作对象
    :param FTableName: 表名
    :param FNumber: 单据号
    :return:
    '''

    sql = f"""select * from {FTableName} where FNumber='{FNumber}'"""

    res = app.select(sql)

    dataSourcedf = pd.DataFrame(res)

    return dataSourcedf


def dept_query(app):
    '''
    部门查询
    :param FToken: token
    :param FName: 部门名称
    :return: dataframe二维表
    '''

    sql = f"""select * from rds_hrv_ods_md_dept """

    data = app.select(sql)

    res = pd.DataFrame(data)

    return res


def acctreclass_query(app):
    '''
    重分类查询
    :param app: 数据库操作对象
    :return: dataframe二维表
    '''

    sql = f"""select * from rds_hrv_ods_md_acctreclass"""

    data = app.select(sql)

    res = pd.DataFrame(data)

    return res


def workcenter_query(app):
    '''
    责任中心查询
    :param app: 数据库操作对象
    :param FDept: 部门名称
    :return: dataframe二维表
    '''

    sql = f"""select * from rds_hrv_ods_md_workcenter"""
    data = app.select(sql)
    res = pd.DataFrame(data)
    return res


def rditem_query(app):
    '''
    研发项目查询
    :param app: 数据库操作对象
    :return: dataframe二维表
    '''

    sql = f"""select * from rds_hrv_ods_ds_detail"""
    data = app.select(sql)
    res = pd.DataFrame(data)
    return res


def acct_query(app):
    '''
    研发项目查询
    :param app: 数据库操作对象
    :return: dataframe二维表
    '''

    sql = f"""select * from rds_hrv_ods_md_acct"""
    data = app.select(sql)
    res = pd.DataFrame(data)
    return res


def project_query(app):
    '''
    项目对照查询
    :param app:数据库操作对象
    :return:dataframe二维表
    '''

    sql = "select * from rds_hrv_ods_md_rditem"

    res = app.select(sql)

    df = pd.DataFrame(res)

    return df


def getRuleVars(app, FTableName, FBillNumber):
    '''
    通过单据编号从数据源获取规则变量：单据编号，费用承担组织，个税申报组织，银行，业务类型
    :param app: 数据库操作对象
    :param FTableName: 表名
    :param FBillNumber: 单据编号
    :return: 单据编号，费用承担组织，个税申报组织，银行，业务类型
    '''
    sql = f"""select FNumber, FExpenseOrgID, FTaxDeclarationOrg, FBankType, FCategoryType from {FTableName} where FNumber = '{FBillNumber}'"""

    res = app.select(sql)

    return res


def voucherRule_query(app, FExpenseOrgID, FTaxDeclarationOrg, FBankType, FCategoryType):
    '''
    规则表查询：根据任务单据的  费用承担组织，个税申报组织，银行，业务类型  获取凭证模版序号
    :param app: 数据库操作对象
    :param FExpenseOrgID: 费用承担组织
    :param FTaxDeclarationOrg: 个税申报组织
    :param FBankType: 银行
    :param FCategoryType: 业务类型
    :return:凭证模板号
    '''

    sql = f"""select FNumber from rds_hrv_ods_rule_voucher where  FExpenseOrgID = '{FExpenseOrgID}' and 
    FTaxDeclarationOrg = '{FTaxDeclarationOrg}' and FBankType = '{FBankType}' and FCategoryType = '{FCategoryType}'"""

    res = app.select(sql)

    return res


def permutation(oldList):
    '''
    判断核算维度
    :param oldList: 核算维度内容
    :return:
    '''
    s = pd.Series(["部门", "责任中心", "重分类", "研发项目", "银行账号", "往来单位", "供应商"])

    res = s.isin(oldList).to_frame()

    res.columns = ["judge"]

    res.loc[res['judge'] == True, 'judge'] = 1
    res.loc[res['judge'] == False, 'judge'] = 0

    return res


def fetchNumber_byFAcct(df, acct, borrowLoanSql):
    '''
    通过科目取数据
    :param df: 数据源
    :param acct:科目
    :param borrowLoanSql:取值字段
    :return:
    '''

    datadf = df[df["FAccount"] == acct][
        ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FRdProject", "FYear", "FMonth", "FDate",
         "FOldDept", "FNotePeriod", borrowLoanSql.strip()]]

    return datadf


def deptOldName_query(app2):
    '''
    查询部门对照表
    :param app2: 数据库操作对象
    :return:
    '''
    sql = "select * from rds_hrv_ods_md_deptcomparison"

    res = app2.select(sql)

    df = pd.DataFrame(res)

    return df


def deptName_repalce(deptOldName, FName):
    '''
    旧部门名字替换成新部门名称
    :param deptOldName: 部门二维表
    :param FName: 部门名称
    :return:
    '''

    oldname = FName

    if deptOldName[deptOldName["FDept"] == FName].empty != True:
        oldname = deptOldName[deptOldName["FDept"] == FName]["FOldDept"].tolist()[-1]

    return oldname


def dept_replace(df, deptdf, deptOldName):
    '''
    部门替换
    :param df: 数据源
    :param deptdf: 部门对照表
    :param deptOldName: 旧部门对照表
    :return:
    '''

    for i in df.index:

        # 取出高新部门
        deptName = df.loc[i]["FHightechDept"]

        # 将高新部门名称与旧部门对照表进行匹配，如果匹配的上就选取旧部门名称，否则就取高新部门名称
        FNewName = deptName_repalce(deptOldName, deptName)

        if deptdf[deptdf["FDepName"] == FNewName].empty != True:
            # 取出部门编码
            deptNumber = (deptdf[deptdf["FDepName"] == FNewName]).iloc[0]["FNumber"]

            # 替换部门编码
            df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FDeptNumber"] = deptNumber

            # 替换部门名称
            df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FDeptName"] = FNewName

    return df


def acctreclass_replace(df, voucherTpldf, acctreclassdf):
    '''
    重分类替换
    :param df: 数据源
    :param voucherTpldf: 凭证模板
    :param acctreclassdf: 科目对照表
    :return:
    '''
    # FAccountName = voucherTpldf.iloc[0]["FAccountName"]

    # 取出科目名称
    FAccountName = voucherTpldf.iloc[19]

    # 判断科目是否匹配到对应的重分类
    if acctreclassdf[acctreclassdf["FAccountItem"] == FAccountName].empty != True:
        # 取出重分类的编码
        acctreclass = (acctreclassdf[acctreclassdf["FAccountItem"] == FAccountName]).iloc[0]["FNumber"]

        # 替换重分类的编码
        df.loc[df.index.tolist(), "FAcctreClassNumber"] = acctreclass

        # 替换重分类的名称
        df.loc[df.index.tolist(), "FAcctreClassName"] = FAccountName

    return df


def workcenter_repalce(df, workcenterdf, deptOldName):
    '''
    责任中心替换
    :param df: 数据源
    :param workcenterdf: 责任中心对照表
    :param deptOldName: 旧部门对照表
    :return:
    '''

    for i in df.index:

        # 高新部门名称
        deptName = df.loc[i]["FHightechDept"]

        # 判断部门是否和责任中心对照表对应的上
        if workcenterdf[workcenterdf["FDept"] == deptName_repalce(deptOldName, deptName)].empty != True:

            # 取出责任中心的编码
            deptNumber = (workcenterdf[workcenterdf["FDept"] == deptName_repalce(deptOldName, deptName)]).iloc[0][
                "FNumber"]

            # 替换责任中心编码
            df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FWorkCenterNumber"] = deptNumber

            # 替换责任中心名称
            df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FWorkCenterName"] = deptName

        else:

            # 旧部门名称
            oldDeptName = df.loc[i]["FOldDept"]

            if workcenterdf[workcenterdf["FDept"] == deptName_repalce(deptOldName, oldDeptName)].empty != True:
                deptNumber = \
                    (workcenterdf[workcenterdf["FDept"] == deptName_repalce(deptOldName, oldDeptName)]).iloc[0][
                        "FNumber"]

                # 替换责任中心编码
                df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FWorkCenterNumber"] = deptNumber

                # 替换责任中心名称
                df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FWorkCenterName"] = oldDeptName

    return df


def rditem_repalce(df, projectdf):
    '''
    研发项目
    :param df: 数据源
    :param projectdf:项目对照表
    :return:
    '''

    for i in df.index:

        # 判断研发项目字段是否为空
        if df.loc[i]["FRdProject"] != '':

            # 获取项目的编码
            projectNumbercode = df.loc[i]["FRdProject"]

            # 将项目编码拆分后用-连接起来
            reProjectNumbercode = "-".join(projectNumbercode.split("_"))

            if projectdf[projectdf["FRDProjectManual"] == reProjectNumbercode].empty != True:
                # 通过项目对照表获取相应的ERP项目编号
                projectNumber = (projectdf[projectdf["FRDProjectManual"] == reProjectNumbercode]).iloc[0]["FRDProject"]

                # 替换ERP项目编号
                df.loc[df[df["FRdProject"] == projectNumbercode].index.tolist(), "FProjectNumber"] = projectNumber

    return df


def lowgradeFunction(data, columns, fSeq, rename, borrowLoanSql):
    '''
    数据拼接
    :param data: 数据源
    :param columns: 索引
    :param fSeq: 行号
    :param rename: 重命名
    :param borrowLoanSql: 埋点字段
    :return:
    '''

    # 新建一个新的指定列名的Dataframe
    newdf = pd.DataFrame(columns=columns)

    # 将数据源和新的dataframe合并
    res = pd.concat([data, newdf])

    # 新建一个索引列
    res["FSeq"] = fSeq

    # 对借方列或者贷方,单据编号列进行重命名
    res.rename(columns={borrowLoanSql.strip(): rename, "FNumber": "FBillNO"}, inplace=True)

    return res


def noFirstAcctData_deal(df, borrowLoanSql):
    '''
    不是一级科目数据处理
    :param df: 数据源
    :param borrowLoanSql:借方或者贷方字段名
    :return:
    '''

    # "FSeqNew"

    # 通过分组获取相应的数据
    res = df.groupby(
        ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth", "FDate", "FOldDept",
         "FNotePeriod"])[
        borrowLoanSql.strip()].sum().to_frame()

    # 重新设置dataframe索引
    res = res.reset_index()

    # 重新设置dataframe列名
    res.columns = ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth", "FDate",
                   "FOldDept", "FNotePeriod",
                   borrowLoanSql.strip()]

    # 将借方或者贷方列的类型设置为float
    res[borrowLoanSql.strip()] = res[borrowLoanSql.strip()].astype(float)

    # 将借方或者贷方列的金额保留两位小数
    res[borrowLoanSql.strip()] = res[borrowLoanSql.strip()].round(2)

    return res


def noFirstAcctDataDefualt_deal(df, borrowLoanSql, BorrowLoan, defultvalue, FNumberTpl, fSeq, rename):
    '''
    不是一级科目数据处理
    :param df: 数据源
    :param borrowLoanSql: 借方或者贷方埋点字段
    :param BorrowLoan: 借方或者贷方字段名
    :param defultvalue: 默认值数据源
    :param FNumberTpl: 凭证模板号
    :param fSeq: 序号
    :param rename: 重命名的字段名
    :return:
    '''

    # res = df.groupby(
    #     ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth","FDate","FOldDept","FSeqNew"])[
    #     borrowLoanSql.strip()].sum().to_frame()
    #
    # res = res.reset_index()
    #
    # res.columns = ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth","FDate","FOldDept","FSeqNew",
    #                borrowLoanSql.strip()]
    #
    # d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]
    #
    # if BorrowLoan != "":
    #     res[rename] = abs(float(d))
    #
    # return res

    # "FSeqNew"

    res = df.groupby(
        ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth", "FDate", "FOldDept",
         "FNotePeriod"])[
        "FComPensionBenefitsAmt"].sum().to_frame()

    res = res.reset_index()

    res.columns = ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth", "FDate",
                   "FOldDept", "FNotePeriod",
                   "FComPensionBenefitsAmt"]

    res.drop(['FComPensionBenefitsAmt'], axis=1, inplace=True)

    d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]

    if BorrowLoan != "":
        res[rename] = round(abs(float(d)), 2)

    return res


def totalAmount_deal(df, fSeq, BorrowLoan, rename, defultvalue, FNumberTpl):
    '''
    总额处理
    :param df: 数据源
    :param fSeq: 序号
    :param BorrowLoan: 借方或者贷方字段名
    :param rename: 重命名的字段名
    :param defultvalue: 默认值数据源
    :param FNumberTpl: 凭证模板号
    :return:
    '''

    # 构建一个外面是list，里面是字典的数据结构
    list = [{
        "FBillNO": df.loc[0]["FNumber"],
        "FExpenseOrgID": df.loc[0]["FExpenseOrgID"],
        "FTaxDeclarationOrg": df.loc[0]["FTaxDeclarationOrg"],
        "FHightechDept": "",
        "FYear": df.loc[0]["FYear"],
        "FMonth": df.loc[0]["FMonth"],
        "FDate": df.loc[0]["FDate"],
        "FOldDept": df.loc[0]["FOldDept"],
        "FNotePeriod": df.loc[0]["FNotePeriod"],
        # "FSeqNew":df.loc[0]["FSeqNew"],
        "FSeq": fSeq,
        rename: ""
        # rename:abs(df[BorrowLoan.strip()].astype(np.float64).sum())
    }]

    # 将之前新建的list构建成一个dataframe
    res = pd.DataFrame(list)

    # 判断借方或者贷方字段埋点是否是默认值
    if BorrowLoan.strip() != 'FDefaultAmt':
        # 先将借方或贷方金额字段类型转换成float，然后求和，取绝对值，保留两位小数
        res[rename] = round(abs(df[BorrowLoan.strip()].astype(np.float64).sum()), 2)

    # 如果埋点字段是默认值
    if BorrowLoan.strip() == 'FDefaultAmt':
        # 通过凭证模板号和凭证模板行号在默认值表中取出默认值
        d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]

        # 对默认值进行类型转换取绝对值，保留2位小数，然后赋值给贷方或者借方列
        res[rename] = round(abs(float(d)), 2)

    return res


def bankAmount_deal(df, fSeq, BorrowLoan, rename, defultvalue, FNumberTpl, row):
    '''
    银行账号类情况处理
    :param df: 数据源
    :param fSeq: 序号
    :param BorrowLoan: 借方或者贷方字段名
    :param rename:  重命名的字段名
    :param defultvalue: 默认值数据源
    :param FNumberTpl: 凭证模板号
    :param row: 凭证模板数据源
    :return:
    '''

    # 取出凭证模板银行账号字段，然后通过/对字段进行拆分
    FBankAccount = field_split(row["FObtainSource"], "/")

    # 新增一个带有FBankAccount字段的数据结构
    list = [{
        "FBillNO": df.loc[0]["FNumber"],
        "FExpenseOrgID": df.loc[0]["FExpenseOrgID"],
        "FTaxDeclarationOrg": df.loc[0]["FTaxDeclarationOrg"],
        "FHightechDept": "",
        "FYear": df.loc[0]["FYear"],
        "FMonth": df.loc[0]["FMonth"],
        "FDate": df.loc[0]["FDate"],
        "FOldDept": df.loc[0]["FOldDept"],
        "FNotePeriod": df.loc[0]["FNotePeriod"],
        # "FSeqNew":df.loc[0]["FSeqNew"],
        "FSeq": fSeq,
        "FBankAccount": FBankAccount,
        rename: ""
        # rename:abs(df[BorrowLoan.strip()].astype(np.float64).sum())
    }]

    # 通过一个list数据结构构建一个新的dataframe
    res = pd.DataFrame(list)

    # 判断埋点字段是否是默认值
    if BorrowLoan.strip() != 'FDefaultAmt':
        # 根据埋点取借方或者贷方金额的和，然后赋值给借方或者贷方列
        res[rename] = round(abs(df[BorrowLoan.strip()].astype(np.float64).sum()), 2)

    # 如果埋点字段是默认值
    if BorrowLoan.strip() == 'FDefaultAmt':
        # 通过凭证模板号和凭证行号到默认值表中取出默认值
        d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]

        # 将取出的默认值类型转换成float，对其取反，保留两位小数
        res[rename] = round(abs(float(d)), 2)

    return res


def field_split(field, symbol):
    '''
    字段拆分
    :param field:字段
    :param symbol:拆分符号
    :return:
    '''

    res = field.split(symbol)

    if res:
        return res[-1]


def field_splitCount(field, symbol):
    '''
    字段拆分
    :param field:字段
    :param symbol:拆分符号
    :return:
    '''

    res = field.split(symbol)

    return res


def NotesFiscalYear_repalce(df):
    '''
    会计年度替换
    :param df: 数据源
    :return:
    '''

    res = df.apply(lbNY, axis=1)

    df["FNotes"] = res

    return df


def lbNA(x):
    '''
    摘要替换辅助函数
    :param x:
    :return:
    '''

    # if x['FCategoryType'].__contains__('发放'):
    #
    #     FMonth = int(x["FMonth"]) - 1
    #
    #     if int(x["FMonth"]) == 1:
    #         FMonth = 12
    #
    #     return x['FNotes'].replace('{会计期间}', str(FMonth))
    #
    # return x['FNotes'].replace('{会计期间}', str(int(x["FMonth"])) if x['FMonth'] != '' else "")

    # res[1].split("月")[0]

    return x['FNotes'].replace('{摘要期间}', str(int((str(x["FNotePeriod"]).split("年")[1]).split("月")[0])) if x[
                                                                                                              "FNotePeriod"] != '' else "")


def lbNY(x):
    '''
    摘要替换辅助函数
    :param x:
    :return:
    '''

    # if x['FCategoryType'].__contains__('发放'):
    #
    #     FYear = int(x["FYear"])
    #
    #     if int(x["FMonth"]) == 1:
    #         FYear = int(x["FYear"]) - 1
    #
    #     return x['FNotes'].replace('{会计年度}', str(FYear))
    #
    # return x['FNotes'].replace('{会计年度}', str(int(x["FYear"])) if x['FYear'] != '' else "")

    return x['FNotes'].replace('{会计年度}',
                               str(int(str(x["FNotePeriod"]).split("年")[0])) if x["FNotePeriod"] != '' else "")


def lbDept(x):
    '''
    摘要替换辅助函数
    :param x:
    :return:
    '''

    return x['FNotes'].replace('{部门}', str(x["FDeptName"]) if str(x["FDeptName"]) != "" else str(x["FHightechDept"]))


def NotesAccountingPeriod_repalce(df):
    '''
    会计期间替换
    :param df: 数据源
    :return:
    '''

    res = df.apply(lbNA, axis=1)

    df["FNotes"] = res

    return df


def NotesDept_repalce(df):
    '''
    摘要部门替换
    :param df: 数据源
    :return:
    '''

    df = df.fillna("")

    res = df.apply(lbDept, axis=1)

    df["FNotes"] = res

    return df


def defult_query(app):
    '''
    默认值查询
    :param app: 数据库执行对象
    :return:
    '''

    sql = "select * from rds_hrv_ods_tpl_defaultValue"

    res = app.select(sql)

    df = pd.DataFrame(res)

    return df


def totalValue_deal(df, fsql):
    '''
    合计取值
    :param df: 数据源
    :param fsql: 借方或者贷方埋点字段
    :return:
    '''

    add = field_splitCount(fsql, "+")
    subtract = field_splitCount(fsql, "-")

    if len(add) > 1:

        df[fsql] = 0

        df[fsql] = df[fsql].astype(float)

        for i in add:
            df[i] = df[i].astype(float)

            df[fsql] = abs(df[fsql].round(2) + df[i].round(2))

    if len(subtract) > 1:

        df[fsql] = 0

        df[fsql] = df[fsql].astype(float)

        for i in subtract:
            df[i] = df[i].astype(float)

            df[fsql] = abs(df[fsql].round(2) - df[i].round(2))

    return df


def defaultValueDept_deal(df, FNumberTpl, fSeq, defultvalue):
    '''
    判断凭证模板管理费用的高新部门
    :param df: 数据源
    :param FNumberTpl: 凭证模板号
    :param fSeq: 凭证模板行号
    :param defultvalue: 默认值数据源
    :return:
    '''

    # FDeptName = ""

    if FNumberTpl == "C003":

        FDeptName = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))][
            "FDefaultDeptName"].iloc[0]
    else:

        FDeptName = df.loc[0]["FHightechDept"]

    return FDeptName


def fixedValue_deal(df, fSeq, BorrowLoan, rename, defultvalue, FNumberTpl):
    '''
    管理费用固定值处理
    :param df: 数据源
    :param fSeq: 行号
    :param BorrowLoan:
    :param rename: 重命名字段
    :param defultvalue:  默认值数据源
    :param FNumberTpl: 凭证模板号
    :return:
    '''

    # ["FDeptNumber", "FDeptName", "FWorkCenterNumber", "FWorkCenterName", "FAcctreClassNumber",
    #                    "FAcctreClassName"]

    # FDeptName = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))][
    #     "FDefaultDeptName"].iloc[0]

    # 获取高新部门
    FDeptName = defaultValueDept_deal(df=df, FNumberTpl=FNumberTpl, fSeq=fSeq, defultvalue=defultvalue)

    # 构建默认值行的数据结构
    list = [{
        "FBillNO": df.loc[0]["FNumber"],
        "FExpenseOrgID": df.loc[0]["FExpenseOrgID"],
        "FTaxDeclarationOrg": df.loc[0]["FTaxDeclarationOrg"],
        # "FHightechDept": FDeptName if FNumberTpl == "C003" else df.loc[0]["FHightechDept"],
        "FHightechDept": FDeptName if FNumberTpl == "C003" else df.loc[0]["FHightechDept"],
        "FYear": df.loc[0]["FYear"],
        "FMonth": df.loc[0]["FMonth"],
        "FDate": df.loc[0]["FDate"],
        "FOldDept": df.loc[0]["FOldDept"],
        "FNotePeriod": df.loc[0]["FNotePeriod"],
        # "FSeqNew":df.loc[0]["FSeqNew"],
        "FDeptNumber": "",
        "FDeptName": "",
        "FWorkCenterNumber": "",
        "FWorkCenterName": "",
        "FAcctreClassNumber": "",
        "FAcctreClassName": "",
        "FSeq": fSeq,
        rename: ""
    }]

    # 新建dataframe
    res = pd.DataFrame(list)

    # 获取默认值
    d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]

    # 判断借方或贷方埋点字段是否为空
    if BorrowLoan != "":
        # 将默认值的字段类型转换成float类型，然后取绝对值，保留2位小数
        res[rename] = round(abs(float(d)), 2)

    return res


def dealingUnit_deal(df, fSeq, BorrowLoan, rename, defultvalue, FNumberTpl):
    '''
    往来单位处理
    :param df: 数据源
    :param fSeq: 凭证模板行号
    :param BorrowLoan: 借方或贷方埋点字段
    :param rename: 重命名的字段名
    :param defultvalue: 默认值
    :param FNumberTpl: 凭证号
    :return:
    '''

    # 构建带有往来单位名称(FDealingUnitName)，往来单位编码(FDealingUnitNumber)的数据结构
    list = [{
        "FBillNO": df.loc[0]["FNumber"],
        "FExpenseOrgID": df.loc[0]["FExpenseOrgID"],
        "FTaxDeclarationOrg": df.loc[0]["FTaxDeclarationOrg"],
        "FHightechDept": "",
        "FYear": df.loc[0]["FYear"],
        "FMonth": df.loc[0]["FMonth"],
        "FDate": df.loc[0]["FDate"],
        "FOldDept": df.loc[0]["FOldDept"],
        "FNotePeriod": df.loc[0]["FNotePeriod"],
        # "FSeqNew":df.loc[0]["FSeqNew"],
        "FSeq": fSeq,
        rename: "",
        "FDealingUnitNumber": "",
        "FDealingUnitName": ""
    }]

    # 新建一个dataframe
    res = pd.DataFrame(list)

    # 通过凭证模板号和行号获取默认值往来单位的编码
    FNumber = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))][
        "FDefaultNumber"].tolist()

    # 通过凭证模板号和行号获取默认值往来单位的名称
    FName = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))][
        "FDefaultName"].tolist()

    # 将往来单位的编码赋值给FDealingUnitNumber字段
    res["FDealingUnitNumber"] = FNumber

    # 将往来单位的名称赋值给FDealingUnitName字段
    res["FDealingUnitName"] = FName

    # 判断埋点字段是否是默认值
    if BorrowLoan.strip() != 'FDefaultAmt':
        # 取出借方或贷方金额
        res[rename] = round(abs(df[BorrowLoan.strip()].astype(np.float64).sum()), 2)

    # 判断埋点字段为默认值
    if BorrowLoan.strip() == 'FDefaultAmt':
        # 取出默认值
        d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]

        # 对默认值进行处理
        res[rename] = round(abs(float(d)), 2)

    return res


def supplier_deal(df, fSeq, BorrowLoan, rename, defultvalue, FNumberTpl):
    '''
    供应商处理
    :param df: 数据源
    :param fSeq: 凭证模板行号
    :param BorrowLoan: 借方或贷方字段
    :param rename: 重命名的字段名
    :param defultvalue: 默认值数据源
    :param FNumberTpl: 凭证模板号
    :return:
    '''

    # 构建一个带有供应商名称(FSupplierName)，供应商编码(FSupplierNumber)的数据结构

    list = [{
        "FBillNO": df.loc[0]["FNumber"],
        "FExpenseOrgID": df.loc[0]["FExpenseOrgID"],
        "FTaxDeclarationOrg": df.loc[0]["FTaxDeclarationOrg"],
        "FHightechDept": "",
        "FYear": df.loc[0]["FYear"],
        "FMonth": df.loc[0]["FMonth"],
        "FDate": df.loc[0]["FDate"],
        "FOldDept": df.loc[0]["FOldDept"],
        "FNotePeriod": df.loc[0]["FNotePeriod"],
        # "FSeqNew":df.loc[0]["FSeqNew"],
        "FSeq": fSeq,
        rename: "",
        "FSupplierNumber": "",
        "FSupplierName": ""
    }]

    # 新建一个dataframe
    res = pd.DataFrame(list)

    # 通过凭证模板号，凭证行号到默认值表中取出供应商编号
    FNumber = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))][
        "FDefaultNumber"].tolist()

    # 通过凭证模板号，凭证行号到默认值表中取出供应商名称
    FName = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))][
        "FDefaultName"].tolist()

    # 将供应商编码赋给FSupplierNumber字段
    res["FSupplierNumber"] = FNumber

    # 将供应商名称赋给FSupplierName字段
    res["FSupplierName"] = FName

    # 判断埋点字段是否是默认值
    if BorrowLoan.strip() != 'FDefaultAmt':
        # 获取借方或者贷方金额的和
        res[rename] = round(abs(df[BorrowLoan.strip()].astype(np.float64).sum()), 2)

    # 判断埋点字段是默认值的情况
    if BorrowLoan.strip() == 'FDefaultAmt':
        # 取出默认值
        d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]

        # 将处理后的默认值赋值给借方或者贷方字段
        res[rename] = round(abs(float(d)), 2)

    return res


def subfunction(df, acct, fSql, fSeq, fDept, FFirstAcct, fAccountNumber, FWorkCenter, FRclass, FRDItem,
                deptdf, acctreclassdf, workcenterdf, rditemdf, acctdf, projectdf, voucherTpldf, rename, defultvalue,
                FNumberTpl, deptOldName, Bankaccount, row, dealingUnit, supplier):
    '''
    对不同的情况进行分类处理
    :param df:  数据源
    :param acct: 会计科目数据源
    :param fSql: 埋点字段
    :param fSeq: 凭证模板行号
    :param fDept: 部门对照表
    :param FFirstAcct:  是否一级科目
    :param fAccountNumber:  会计科目编码
    :param FWorkCenter:  责任中心核算维度
    :param FRclass:  重分类核算维度
    :param FRDItem: 研发项目核算维度
    :param deptdf: 部门对照表
    :param acctreclassdf: 重分类对照表
    :param workcenterdf:责任中心对照表
    :param rditemdf: 工时表
    :param acctdf:
    :param projectdf: 研发项目对照表
    :param voucherTpldf: 凭证模板
    :param rename: 重命名的字段名
    :param defultvalue: 默认值表
    :param FNumberTpl: 凭证模板号
    :param deptOldName: 旧部门对照表
    :param Bankaccount: 银行
    :param row: 行凭证模板
    :param dealingUnit:往来单位
    :param supplier: 供应商
    :return:
    '''

    # 一级会计科目 核算维度为 ['部门', '责任中心', '重分类']的情况
    if FFirstAcct == 1 and fAccountNumber != 0 and fDept == 1 and FWorkCenter == 1 and FRclass == 1 and FRDItem == 0 and Bankaccount == 0 and dealingUnit == 0 and supplier == 0:
        # ['部门', '责任中心', '重分类']

        columns = ["FDeptNumber", "FDeptName", "FWorkCenterNumber", "FWorkCenterName", "FAcctreClassNumber",
                   "FAcctreClassName"]

        # 通过科目取出对应数据源的相关字段
        dataAcct = fetchNumber_byFAcct(df, acct, fSql)

        # 将取出的数据源与新构建的Dataframe进行拼接
        res = lowgradeFunction(data=dataAcct, columns=columns, fSeq=fSeq, rename=rename, borrowLoanSql=fSql)

        # 部门替换
        deptafter = dept_replace(res, deptdf, deptOldName)

        # 责任中心替换
        workcenterdf = workcenter_repalce(deptafter, workcenterdf, deptOldName)

        # res = acctreclass_replace(workcenterdf, voucherTpldf, acctreclassdf)

        # 重分类替换
        res = acctreclass_replace(workcenterdf, row, acctreclassdf)

        return res

    # 一级会计科目 核算维度为['部门']的情况
    if FFirstAcct == 1 and fAccountNumber != 0 and fDept == 1 and FWorkCenter == 0 and FRclass == 0 and FRDItem == 0 and Bankaccount == 0 and dealingUnit == 0 and supplier == 0:
        # ['部门']

        columns = ["FDeptNumber", "FDeptName"]

        # 通过科目取出对应数据源的相关字段
        dataAcct = fetchNumber_byFAcct(df, acct, fSql)

        # 将取出的数据源与新构建的Dataframe进行拼接
        res = lowgradeFunction(data=dataAcct, columns=columns, fSeq=fSeq, rename=rename, borrowLoanSql=fSql)

        # 部门替换
        res = dept_replace(res, deptdf, deptOldName)

        return res

    # 一级会计科目 核算维度为['研发项目', '责任中心', '重分类']的情况
    if FFirstAcct == 1 and fAccountNumber != 0 and fDept == 0 and FWorkCenter == 1 and FRclass == 1 and FRDItem == 1 and Bankaccount == 0 and dealingUnit == 0 and supplier == 0:
        # ['研发项目', '责任中心', '重分类']

        columns = ["FProjectNumber", "FWorkCenterNumber", "FWorkCenterName", "FAcctreClassNumber", "FAcctreClassName"]

        # 通过科目取出对应数据源的相关字段
        dataAcct = fetchNumber_byFAcct(df, acct, fSql)

        # 将取出的数据源与新构建的Dataframe进行拼接
        res = lowgradeFunction(data=dataAcct, columns=columns, fSeq=fSeq, rename=rename, borrowLoanSql=fSql)

        # 研发项目替换
        rditemafter = rditem_repalce(res, projectdf)

        # 责任中心替换
        workcenterdf = workcenter_repalce(rditemafter, workcenterdf, deptOldName)

        # 重分类替换
        res = acctreclass_replace(workcenterdf, row, acctreclassdf)

        return res

    # 不是一级会计科目 核算维度为['部门']的情况
    if FFirstAcct == 0 and fAccountNumber == 1 and fDept == 1 and FWorkCenter == 0 and FRclass == 0 and FRDItem == 0 and Bankaccount == 0 and dealingUnit == 0 and supplier == 0:
        # 部门 不是一级科目分配

        columns = ["FDeptNumber", "FDeptName"]

        noFirstAcctData = noFirstAcctData_deal(df=df, borrowLoanSql=fSql)

        res = lowgradeFunction(data=noFirstAcctData, columns=columns, fSeq=fSeq, rename=rename, borrowLoanSql=fSql)

        # 部门替换
        res = dept_replace(res, deptdf, deptOldName)

        return res

    # 总额的情况
    if FFirstAcct == 0 and fAccountNumber == 0 and fDept == 0 and FWorkCenter == 0 and FRclass == 0 and FRDItem == 0 and Bankaccount == 0 and dealingUnit == 0 and supplier == 0:
        # 总额

        res = totalAmount_deal(df=df, fSeq=fSeq, BorrowLoan=fSql, rename=rename, defultvalue=defultvalue,
                               FNumberTpl=FNumberTpl)

        return res

    # 不是一级会计科目 银行账号的情况
    if FFirstAcct == 0 and fAccountNumber == 0 and fDept == 0 and FWorkCenter == 0 and FRclass == 0 and FRDItem == 0 and Bankaccount == 1 and dealingUnit == 0 and supplier == 0:
        # 银行账号

        res = bankAmount_deal(df=df, fSeq=fSeq, BorrowLoan=fSql, rename=rename, defultvalue=defultvalue,
                              FNumberTpl=FNumberTpl, row=row)

        return res

    # 不是一级会计科目 服务费的情况
    if FFirstAcct == 0 and fAccountNumber == 3 and fSql == "FDefaultAmt" and dealingUnit == 0 and supplier == 0 and fDept == 1 and FWorkCenter == 1 and FRclass == 1:
        # 服务费
        # columns = ["FDeptNumber", "FDeptName", "FWorkCenterNumber", "FWorkCenterName", "FAcctreClassNumber",
        #            "FAcctreClassName"]

        # noFirstAcctData = noFirstAcctDataDefualt_deal(df=df, borrowLoanSql=fSql, BorrowLoan=fSql,
        #                                               defultvalue=defultvalue, FNumberTpl=FNumberTpl, fSeq=fSeq,
        #                                               rename=rename)
        #
        # res = lowgradeFunction(data=noFirstAcctData, columns=columns, fSeq=fSeq, rename=rename, borrowLoanSql=fSql)

        # deptafter = dept_replace(res, deptdf, deptOldName)
        #
        # workcenterdf = workcenter_repalce(deptafter, workcenterdf, deptOldName)
        #
        # # res = acctreclass_replace(workcenterdf, voucherTpldf, acctreclassdf)
        #
        # # rowdf=pd.DataFrame(row)
        #
        # # print(rowdf.iloc[19])
        #
        # res = acctreclass_replace(workcenterdf, row, acctreclassdf)

        # FDeptNumber = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))][
        #     "FDefaultDeptNumber"]

        # FDeptName = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))][
        #     "FDefaultDeptName"]

        # 对凭证模板上的固定值行进行处理
        res = fixedValue_deal(df=df, fSeq=fSeq, BorrowLoan=fSql, rename=rename, defultvalue=defultvalue,
                              FNumberTpl=FNumberTpl)

        # 部门替换
        deptafter = dept_replace(res, deptdf, deptOldName)

        # 责任中心替换
        workcenterdf = workcenter_repalce(deptafter, workcenterdf, deptOldName)

        # 重分类替换
        res = acctreclass_replace(workcenterdf, row, acctreclassdf)

        return res

    # 不是一级会计科目 往来单位的情况
    if FFirstAcct == 0 and fAccountNumber == 0 and fDept == 0 and FWorkCenter == 0 and FRclass == 0 and FRDItem == 0 and Bankaccount == 0 and dealingUnit == 1 and supplier == 0:
        # 往来单位

        # 处理凭证模板往来单位情况
        res = dealingUnit_deal(df=df, fSeq=fSeq, BorrowLoan=fSql, rename=rename, defultvalue=defultvalue,
                               FNumberTpl=FNumberTpl)

        return res

    # 不是一级会计科目 供应商的情况
    if FFirstAcct == 0 and fAccountNumber == 0 and fAccountNumber == 0 and fDept == 0 and FWorkCenter == 0 and FRclass == 0 and FRDItem == 0 and Bankaccount == 0 and dealingUnit == 0 and supplier == 1:
        # 供应商

        # 处理凭证模板供应商情况
        res = supplier_deal(df=df, fSeq=fSeq, BorrowLoan=fSql, rename=rename, defultvalue=defultvalue,
                            FNumberTpl=FNumberTpl)

        return res


def judgement(row, df, deptdf, acctreclassdf, workcenterdf, rditemdf, acctdf, projectdf, voucherTpldf, defultvalue,
              deptOldName):
    # 将核算维度按/拆分
    fLexitemProperty = row["FLexitemProperty"].split("/")

    # 获得会计科目
    fAcct = row["FAccount"]

    # 获得是否一级科目值
    fFirstAcct = row["FFirstAcct"]

    # 获得会计科目编码
    fAccountNumber = row["FAccountNumber"]

    # 获得借方埋点字段
    fAccountBorrowSql = row["FAccountBorrowSql"]

    # 获得贷方埋点字段
    fAccountLoanSql = row["FAccountLoanSql"]

    # 获得凭证模板行号
    fSeq = row["FSeq"]

    # 获得凭证模板号
    FNumberTpl = row["FNumber"]

    # 重命名字段
    rename = ""

    # 埋点字段
    fSql = ""

    # df.rename(columns={"FSeqNew":"FNewSeq"},inplace=True)

    # 取出借方埋点字段的
    if fAccountBorrowSql != "":
        fSql = field_split(fAccountBorrowSql, ".")

        df = totalValue_deal(df, fSql)

        rename = "allamountBorrow"

    # 取出贷方埋点字段的
    if fAccountLoanSql != "":
        fSql = field_split(fAccountLoanSql, ".")

        df = totalValue_deal(df, fSql)

        rename = "allamountLoan"

    # 判断核算维度情况
    arg = permutation(fLexitemProperty)

    # 对不同的情况进行处理
    res = subfunction(df=df, acct=fAcct, fSeq=fSeq, fSql=fSql, fDept=arg.loc[0]["judge"], FFirstAcct=fFirstAcct,
                      fAccountNumber=fAccountNumber, FWorkCenter=arg.loc[1]["judge"],
                      FRclass=arg.loc[2]["judge"], FRDItem=arg.loc[3]["judge"], deptdf=deptdf,
                      acctreclassdf=acctreclassdf, workcenterdf=workcenterdf, rditemdf=rditemdf, acctdf=acctdf,
                      projectdf=projectdf, voucherTpldf=voucherTpldf, rename=rename, defultvalue=defultvalue,
                      FNumberTpl=FNumberTpl, deptOldName=deptOldName, Bankaccount=arg.loc[4]["judge"], row=row,
                      dealingUnit=arg.loc[5]["judge"], supplier=arg.loc[6]["judge"])

    return res


def data_deal(voucherTpldf, datadf, deptdf, acctreclassdf, workcenterdf, rditemdf, acctdf, projectdf, defultvalue,
              deptOldName):
    resultdf = pd.DataFrame()

    # 遍历凭证每一行
    for i in voucherTpldf.index:
        resultdf = pd.concat([resultdf,
                              judgement(voucherTpldf.iloc[i], datadf, deptdf, acctreclassdf, workcenterdf, rditemdf,
                                        acctdf, projectdf, voucherTpldf, defultvalue, deptOldName)])

    return resultdf


def result_deal(df):
    '''
    对过程表进行处理
    :param df:
    :return:
    '''

    df.drop(['FAccountBorrow', 'FAccountLoan', 'FAccountBorrowSql', 'FAccountLoanSql', 'FFirstAcct', 'FAccount',
             'FAccountName', 'FAccountNumber', 'FLexitemProperty', 'FObtainSource', "FOldDept", "FNotePeriod"], axis=1,
            inplace=True)

    return df


def sqlSplicing(df):
    df = df.reset_index(drop=True)

    df = df.fillna("")

    # df["allamountBorrow"] = df["allamountBorrow"].round(2)
    #
    # df["allamountLoan"] = df["allamountLoan"].round(2)

    df["allamountBorrow"] = df["allamountBorrow"].astype(str)
    df["allamountLoan"] = df["allamountLoan"].astype(str)
    df["FDate"] = df["FDate"].astype(str)

    col = ",".join(df.columns.tolist())

    sql = """insert into rds_hrv_src_ds_middleTable_filtration(""" + col + """) values"""

    for i in df.index:

        if i == len(df) - 1:

            sql = sql + str(tuple(df.iloc[i]))

        else:

            sql = sql + str(tuple(df.iloc[i])) + ""","""

    return sql


def errorData_clear(app, FYear, FMonth):
    '''
    将中间表异常数据清理
    :param app:
    :return:
    '''

    FYear = str(FYear) + ".0"
    FMonth = str(FMonth) + ".0"

    sql = f"delete from rds_hrv_ods_ds_middleTable where FIsdo=2 and FYear='{FYear}' and FMonth='{FMonth}'"

    app.update(sql)


def middleTableSrc(app):
    '''
    中间表SRC-ODS
    :param app:
    :return:
    '''

    sql = """
    INSERT INTO rds_hrv_src_ds_middleTable  
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
				FWorkCenterName,
				FAcctreClassName,
				Row_number() OVER(partition by a.FBIllNO order by a.FDate desc) as FSeqNew,
				0 as FIsdo,
				'' as FMessage,
				'' as FSrcSeq,
				'' as FStdSeq
                from rds_hrv_src_ds_middleTable_filtration a
                where not exists
                (select * from rds_hrv_src_ds_middleTable b 
                    where A.FBillNO = B.FBillNO)
    """
    app.update(sql)

    src_clear(app, "rds_hrv_src_ds_middleTable_filtration")


def datatable_into(app, sql):
    '''
    将中间表插入数据库
    :param app:
    :param df:
    :return:
    '''

    app.insert(sql)


def erpKey_query(app, FCategory):
    '''
    查询erp密钥
    :param FCategory:
    :return:
    '''

    sql = f"select acct_id,user_name,app_id,app_sec,server_url,FToken from rds_erp_key where FCategory='{FCategory}'"

    res = app.select(sql)

    return res


def dept_update(app3):
    '''
    将与工时表未匹配到的高新部门更新
    :param app3: 数据库操作对象
    :return:
    '''

    sql = """
    update a set a.FHightechDept=a.FOldDept  
    from rds_hrv_std_ds_salary  a 
    where a.FHightechDept=''

    update a set a.FHightechDept=a.FOldDept 
    from rds_hrv_std_ds_socialsecurity a 
    where a.FHightechDept=''

    """

    app3.update(sql)


def action(FToken, FYear, FMonth, FOpthon):
    # 通过传递token，获得操作数据库对象
    appKey = RdClient(FToken)

    # 将SRC的数据源同步到ODS表中
    main_srcToOds(FToken)

    # 将ODS表的数据源同步到STD表中
    main_odsToStd(FToken)

    # 从数据库表中获取ERP密钥
    erpkey = erpKey_query(appKey, FOpthon)

    # 通过数据库中的密钥获取对应数据的操作对象
    app = RdClient(erpkey[0]['FToken'])

    # 更新STD表中未匹配到工时表的数据所对应高新部门
    dept_update(app)

    # 清空凭证中间表
    src_clear(app, "rds_hrv_src_ds_middleTable")

    # 清空数据缓存表
    src_clear(app, "rds_hrv_src_ds_middleTable_filtration")

    # 部门对照表查询
    deptdf = dept_query(app)

    # 重分类对照表查询
    acctreclassdf = acctreclass_query(app)

    # 责任中心对照表查询
    workcenterdf = workcenter_query(app)

    # 研发项目对照表查询
    rditemdf = rditem_query(app)

    # 科目对照表查询
    acctdf = acct_query(app)

    # 项目对照表查询
    projectdf = project_query(app)

    # 默认值表查询
    defultvalue = defult_query(app)

    # 旧部门对照表查询
    deptOldName = deptOldName_query(app)

    # 单据类型查询
    categoryName = categorySecond_query(app, FYear, FMonth)

    if erpkey:

        if categoryName:

            for values in categoryName:

                # 通过单据类型查询对应的表名
                tableNameRes = tableName_query(app, values["FCategoryType"])

                if tableNameRes is not None:

                    # 通过表名与单据号查找规则表
                    ruleVars = getRuleVars(app, tableNameRes, values["FNumber"])

                    if ruleVars:

                        # 通过对应的规则表中的 费用承担组织，个税申报组织，银行，单据类型查找对应的凭证模板
                        voucherRuleRes = voucherRule_query(app, ruleVars[0]["FExpenseOrgID"],
                                                           ruleVars[0]["FTaxDeclarationOrg"],
                                                           ruleVars[0]["FBankType"], ruleVars[0]["FCategoryType"])

                        if voucherRuleRes:

                            # 遍历查询到的凭证模板
                            for i in voucherRuleRes:
                                # 通过模板号查询对应的凭证模板
                                voucherTpldf = voucher_query(app, i["FNumber"])

                                # 通过表名和单据编号查找数据源
                                dataSourceDF = datasource_query(app, tableNameRes, values["FNumber"])

                                # 凭证处理函数
                                res = data_deal(voucherTpldf, dataSourceDF, deptdf, acctreclassdf, workcenterdf,
                                                rditemdf, acctdf,
                                                projectdf, defultvalue, deptOldName)

                                # 将凭证处理的结果与模板表通过行号(FSeq)字段关联进行关联
                                df4 = pd.merge(voucherTpldf, res, how="inner", on="FSeq")

                                # 摘要会计年度替换
                                result = NotesFiscalYear_repalce(df4)

                                # 摘要会计期间替换
                                result = NotesAccountingPeriod_repalce(result)

                                # 摘要部门替换
                                result = NotesDept_repalce(result)

                                # 将不需要的字段去掉
                                result_deal(result)

                                # 将凭证拼接成插入语句字符串
                                res = sqlSplicing(
                                    result[(result["allamountBorrow"] != 0) & (result["allamountLoan"] != 0)])

                                # 将凭证插入数据缓存表中
                                datatable_into(app, res)

                                print(values["FNumber"] + "_" + i["FNumber"] + "生成凭证完成")

                        else:

                            print(values["FNumber"] + "没有查到相应的模板编号")

                else:

                    print("没有找到对应的表名")

            else:

                print("请检查单据编号,未查到对应的单据类型")

    else:

        print("ERP权限查询失败")

    # 将缓存表中的数据同步到SRC表中
    middleTableSrc(app)

    # 核算维度检查
    precheckData(app)

    # 借贷不平数据检查
    precheckData2(app)

    # 更新STD SRC行号到凭证中
    LineNumber_get(app)

    # 将未匹配到的模板信息插入到预览表中
    templateNumber_deal(app, FYear, FMonth)

    return True


def vch_save(FToken, FYear, FMonth, FOpthon):
    '''
    凭证生成功能
    :param FToken: 中台数据库token
    :param FYear: 年
    :param FMonth: 月
    :param FOpthon: 账套名
    :return:
    '''

    appKey = RdClient(FToken)

    erpkey = erpKey_query(appKey, FOpthon)

    voucher_save(FToken=FToken, FYear=FYear, FMonth=FMonth, option=erpkey)

    return True


def precheckData(app):
    '''
    对凭证中间表进行检查
    :param app: 数据库操作对象
    :return:
    '''

    sql = """
    --核算维度部门的

        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，部门未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='部门' and a.FDeptNumber=''


        -- 部门/责任中心/重分类

        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，部门未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='部门/责任中心/重分类' and (FDeptName='')

        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，责任中心未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='部门/责任中心/重分类' and (FWorkCenterNumber='')

        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，重分类未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='部门/责任中心/重分类' and (FAcctreClassNumber='' )


        --供应商

        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，供应商未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='供应商' and (FSupplierNumber='')


        --往来单位

        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，往来单位未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='往来单位' and (FDealingUnitNumber='')

        --研发项目/责任中心/重分类

        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，研发项目未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='研发项目/责任中心/重分类' and (FRdProject='')


        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，研发项目编码未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='研发项目/责任中心/重分类' and (FProjectNumber='')


        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，责任中心未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='研发项目/责任中心/重分类' and (FWorkCenterNumber='')



        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，重分类未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='研发项目/责任中心/重分类' and (FAcctreClassNumber='' )


        --银行账号

        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，银行账号未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='银行账号' and (FBankAccount='')


        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，固定值，请注意！' from rds_hrv_src_ds_middleTable a
        inner join (
        select FNumber,FSeq from rds_hrv_ods_tpl_defaultValue where FDefaultAmt!=0) c
        on a.FNumber=c.FNumber and a.FSeq=c.FSeq


        update a set a.FIsdo=2 from rds_hrv_src_ds_middleTable a
        inner join 
        (select b.FBillNO from rds_hrv_src_ds_middleTable b where b.FIsdo=2) c
        on a.FBillNO=c.FBillNO
    """

    app.update(sql)


def precheckData2(app):
    '''
    数据预检查
    :param app:
    :return:
    '''

    sql = "select * from rds_hrv_src_ds_middleTable"
    res = app.select(sql)

    df = pd.DataFrame(res)

    if not df.empty:

        df["allamountBorrow"] = df["allamountBorrow"].replace("", "0")
        df["allamountLoan"] = df["allamountLoan"].replace("", "0")
        df["allamountBorrow"] = df["allamountBorrow"].astype(np.float64)
        df["allamountLoan"] = df["allamountLoan"].astype(np.float64)

        dfB = df.groupby(["FBillNO"])["allamountBorrow"].sum()

        dfB = dfB.reset_index()

        dfL = df.groupby(["FBillNO"])["allamountLoan"].sum()

        dfL = dfL.reset_index()

        dfRes = pd.merge(dfB, dfL, how="inner", on="FBillNO")

        dfRes["allamountBorrow"] = dfRes["allamountBorrow"].round(2)

        dfRes["allamountLoan"] = dfRes["allamountLoan"].round(2)

        result = dfRes[dfRes["allamountBorrow"] != dfRes["allamountLoan"]]["FBillNO"].tolist()

        result = list(set(result))

        for i in result:
            sql = f"update a set a.FIsdo=2 , a.FMessage=a.FMessage+'借贷不平，请检查！' from rds_hrv_src_ds_middleTable a where a.FBillNO='{i}'"

            app.update(sql)


def LineNumber_get(app):
    '''
    更新std,src行号,单据编号
    :param app:
    :return:
    '''

    sql = """
    update a set a.FSrcSeq=b.FSeq,a.FStdSeq=b.FSeqNew 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_linenumber3 b
        on a.FHightechDept=b.FHightechDept and a.FBillNO=b.FNumber

    update a set a.FBillNO=FBillNO+'-'+FNumber from 
    rds_hrv_src_ds_middleTable a where FNumber='A20'
    """
    app.update(sql)


def templateNumber_deal(app, FYear, FMonth):
    '''
    将未匹配到的模板信息插入到预览表中
    :return:
    '''

    sql = f"""
    select FNumber as FBillNO,'2' as FIsdo from 
    (select 
    FCategoryType,FNumber,FYear,FMonth
    from rds_hrv_ods_ds_documentNumber ) a
    left join (
    select distinct FBillNO from rds_hrv_src_ds_middleTable) b
    on a.FNumber=b.FBILLNO
    where b.FBillNO is null and (a.FYear='{FYear}' and a.FMonth='{FMonth}')
    """

    res = app.select(sql)

    df = pd.DataFrame(res)

    if not df.empty:

        df["FMessage"] = "模板未找到，请核对查询模板条件！"
        df["FYear"] = str(FYear) + ".0"
        df["FMonth"] = str(FMonth) + ".0"

        col = ",".join(df.columns.tolist())

        sql = """insert into rds_hrv_src_ds_middleTable(""" + col + """) values"""

        for i in df.index:

            if i == len(df) - 1:

                sql = sql + str(tuple(df.iloc[i]))

            else:

                sql = sql + str(tuple(df.iloc[i])) + ""","""

        app.insert(sql)
