# Compare Dataframes

## Description
This powerful Python library is designed to facilitate easy and efficient comparison of data frames.


### Key Features
- Universal Compatibility: The library is designed to work out of the box with data frames of any type, including pandas, polars, or Spark data frames. This flexibility allows you to use the library with your preferred data manipulation framework.

- String Comparison: For string comparison, the library employs the Levenshtein distance algorithm. The Levenshtein distance is a string metric for measuring the difference between two sequences. The algorithm is used to identify the minimum number of single-character edits (insertions, deletions, or substitutions) required to change one word into the other.

- Numeric Comparison: Numeric comparisons are conducted using the Euclidean distance between columns. This method is effective for identifying differences in numeric data, providing insights into variations between datasets.

- User-friendly reporting: The library generates a detailed tabular report that provides a comprehensive overview of the differences between the two datasets. 

## Example Usage
```python
import polars as pl
expected = pl.DataFrame(
    {
    'id': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
    'another_id':[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010],
    'name': ['John', 'Alice', 'Bob', 'Eva', 'Charlie', 'Linda', 'David', 'Sophie', 'Michael', 'Emma'],
    'age': list(range(25, 35)),
    'height': [170, 165, 180, 160, 175, 160, 185, 175, 172, 168],
    'weight': [70, 55, 80, 50, 68, 52, 95, 73, 78, 60]
    }
)

tested = pl.DataFrame(
    {
    'id': [102, 103, 104, 105, 106, 107, 108, 109, 110],
    'another_id':[1002, 1003, 1004, 1005, 1006, 1007, 2008, 2009, 2010],
    'name': ['Albert', 'Bobby', 'Evan', 'Charlie', 'Linda', 'David', 'Sophie', 'Michael', 'Emma'],
    'age': list(range(25, 34)),
    'height': [165, 180, 160, 175, 160, 185, 175, 172, 168],
    'weight': [55, 80, 50, 68, 52, 95, 73, 78, 60]
        
    }
)
from compare_datasets import Compare
key = ['id', 'another_id']
compared = Compare(tested=tested,expected=expected, key=key) # creates a Compare object
print(compared) # prints the tabulated result
compared.get_report("<PATH_TO_SAVE_REPORT>, format='txt'") # saves the report to a file
```
## Use Cases
Thisi s particularly useful (not exhaustive) in the following scenarios:

- Testing: Quickly identify and verify differences between expected and actual data frames during testing phases.

- Analysis: Gain insights into the variations and discrepancies between two datasets, facilitating thorough data analysis.

## Roadmap
- Add other distance functions
- Add seamless integration with pytest
- Write a user guide

## License
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

