#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mdlJhHrvArchievepy import *
import pytest


@pytest.mark.parametrize('FToken,FYear,FMonth,FOpthon,output',
                         [("057A7F0E-F187-4975-8873-AF71666429AB", "2023","7","账套查询DMS测试", True)])
def test_voucher_create(FToken, FYear, FMonth, FOpthon,output):

    assert voucher_create(FToken=FToken, FYear=FYear, FMonth=FMonth, FOpthon=FOpthon) == output