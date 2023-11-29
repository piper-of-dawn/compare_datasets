import polars as pl
from compare_datasets import Compare
import pandas as pd
def test_compare():
    data = pl.scan_csv('compare_datasets/tests/mfPrices.csv', try_parse_dates=True).fetch(100)
    data_changed = pl.scan_csv('compare_datasets/tests/mfPrices_changed.csv',try_parse_dates=True).fetch(100)
    # print(data.schema)
    c = Compare(tested=data_changed,expected=data, verbose=False)
    assert c is not None 
    print(c.get_report(format='html', save_at_path='compare_datasets/tests/'))

def test_compare_pandas():
    data = pd.read_csv('compare_datasets/tests/mfPrices.csv', parse_dates=True, nrows=100)
    data_changed = pd.read_csv('compare_datasets/tests/mfPrices_changed.csv', nrows=100)
    # print(data.schema)
    c = Compare(tested=data_changed,expected=data, verbose=True)
    assert c is not None 
    print(c.get_report(format='html'))
    
test_compare()
