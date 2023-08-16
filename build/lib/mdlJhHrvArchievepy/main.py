#!/usr/bin/env python
# -*- coding: utf-8 -*-
from .voucher import *

def voucher_create(FToken, FYear, FMonth, FOpthon):
    '''
    生成凭证
    :param FToken: 中台数据库token
    :param FYear: 年
    :param FMonth: 月
    :param FOpthon: ERP系统账套名
    :return:
    '''

    res=action(FToken=FToken, FYear=FYear, FMonth=FMonth, FOpthon=FOpthon)

    return res


def voucher_save(FToken, FYear, FMonth, FOpthon):
    '''
    凭证同步至ERP
    :param FToken: 中台数据库token
    :param FYear: 年
    :param FMonth: 月
    :param FOpthon: ERP系统账套名
    :return:
    '''

    res=vch_save(FToken=FToken, FYear=FYear, FMonth=FMonth, FOpthon=FOpthon)

    return res




