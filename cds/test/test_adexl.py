import os

from cds.adexl import ADE_XL_Result

def test_adexl():
    r = ADE_XL_Result(os.path.join(os.path.dirname(__file__), 'data', 'adexl'))
    print r


