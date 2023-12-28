import polars as pl
from compare_datasets import Compare
import pandas as pd
def test_compare():
    data = pl.scan_csv('compare_datasets/tests/mfPrices.csv', try_parse_dates=True).fetch(100)
    data_changed = pl.scan_csv('compare_datasets/tests/mfPrices_changed.csv',try_parse_dates=True).fetch(100)
    c = Compare(tested=data_changed,expected=data, verbose=False)
    assert c is not None 

def test_compare_low_memory():
    data = pl.scan_csv('compare_datasets/tests/mfPrices.csv', try_parse_dates=True, low_memory=True).collect()
    data_changed = pl.scan_csv('compare_datasets/tests/mfPrices_changed.csv',try_parse_dates=True, low_memory=True).collect()
    c = Compare(tested=data_changed,expected=data, verbose=False)
    assert c is not None 


def test_compare_pandas():
    data = pd.read_csv('compare_datasets/tests/mfPrices.csv', parse_dates=True, nrows=100)
    data_changed = pd.read_csv('compare_datasets/tests/mfPrices_changed.csv', nrows=100)
    c = Compare(tested=data_changed,expected=data, verbose=True)
    assert c is not None 

def test_join_on_duplicated_keys():
    tested = pl.read_excel('compare_datasets/tests/join.xlsx', sheet_name='tested')
    expected = pl.read_excel('compare_datasets/tests/join.xlsx', sheet_name='expected')
    c = Compare(tested=tested, expected=expected, key=["GROUP",'NODE_1'], verbose=False)
    assert c is not None

def test_duplicated_keys_mismatched_rows():
    tested = pl.read_excel('compare_datasets/tests/duplicate_keys_and_mismatched_rows.xlsx', sheet_name='tested')
    expected = pl.read_excel('compare_datasets/tests/duplicate_keys_and_mismatched_rows.xlsx', sheet_name='expected')
    c = Compare(tested=tested, expected=expected, key=["ID1",'ID2'], verbose=False)
    assert c is not None

def test_duplicated_keys_mismatched_rows_and_low_memory():
    tested = pl.read_excel('compare_datasets/tests/duplicate_keys_and_mismatched_rows.xlsx', sheet_name='tested')
    expected = pl.read_excel('compare_datasets/tests/duplicate_keys_and_mismatched_rows.xlsx', sheet_name='expected')
    c = Compare(tested=tested, expected=expected, key=["ID1",'ID2'], verbose=False, low_memory=True)
    assert c is not None
    
def test_datetime_bug():    
    test = pd.read_csv('compare_datasets/tests/datetime_bug.csv')
    test['DATE'] = pd.to_datetime(test['DATE'])
    c = pl.read_csv('compare_datasets/tests/datetime_bug.csv', try_parse_dates=True)
    assert c is not None

def test_key_bug():
    data = {
        'id': [101, 102, 103, 104, 105, 106, 107, 108, None, None],
        'another_id':[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, None, None],
        'name': ['John', 'Alice', 'Bob', 'Eva', 'Charlie', 'Linda', 'David', 'Sophie', 'Michael', 'Emma'],
        'age': list(range(25, 35)),
        'height': [170, 165, 180, 160, 175, 160, 185, 175, 172, 168],
        'weight': [70, 55, 80, 50, 68, 52, 95, 73, 78, 60],
        'is_married': [True, False, True, False, True, False, True, False, True, True]
    }

    data_test = {
        'id': [101, 102, 103, 104, 105, 106, 107, 108,202, None, None],
        'another_id':[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, None, None, 1009],
        'name': ['John', 'Alice', 'Bob', 'Eva', 'Charlie', 'Linda', 'David', 'Sophie', 'Michael', 'Emma', 'Peter'],
        'age': list(range(25, 36)),
        'height': [170, 165, 180, 160, 175, 160, 185, 175, 172, 168, 169],
        'weight': [70, 55, 80, 50, 68, 52, 95, 73, 78, 60, 45],
        'is_married': [True, False, True, False, True, False, True, False, True, True, False]
    }

    expected = pl.DataFrame(data)
    tested = pl.DataFrame(data_test)

    tested = tested.with_columns([
        pl.col('id').cast(pl.Int16)
    ])
    key = ['id', 'another_id']
    c = Compare(tested=tested,expected=expected,key=key, verbose=True)
    assert c is not None

def test_datetime():
    test = pd.read_excel('compare_datasets/tests/datetime_bug.xlsx',sheet_name='tested')
    test['DATE'] = pd.to_datetime(test['DATE'])
    expect = pd.read_excel('compare_datasets/tests/datetime_bug.xlsx',sheet_name='expected')
    c = Compare(tested=test,expected=expect,key=['ID'], verbose=False)
    c.get_report(filename='test.txt')
    assert c is not None
