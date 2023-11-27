import polars as pl
from tabulate import tabulate
import pandas as pd
from compare_datasets.structure import stringify_result
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class PrepareForComparison:
    """
    This class encapsulates the methods to compare two dataframes.

    Args:
        tested (polars.DataFrame): The dataframe to be tested.
        expected (polars.DataFrame): The expected dataframe.
        key (str): The column name to sort the dataframes.
        cast_numeric (bool, optional): Whether to cast numeric columns to a common type. Defaults to True.
        tolerance (int, optional): The numeric tolerance for comparison. Defaults to 6.

    Attributes:
        tested (polars.DataFrame): The dataframe to be tested.
        expected (polars.DataFrame): The expected dataframe.
        _tolerance (int): The numeric tolerance for comparison.
        _numeric_types (list): A list of numeric types for casting columns.
        _result (dict): A dictionary to store the comparison result.
        _mistmatched_schema (dict): A dictionary to store information about mismatched schemas.
        _row_and_column_counts (polars.DataFrame): A dataframe to store row counts.

    Methods:
        test_counts(): Compares the number of columns and rows between the dataframes.
        test_column_names(): Compares the column names between the dataframes.

    """

    @classmethod
    def __notPolars__(self, df):
        return not isinstance(df, pl.DataFrame)

    @classmethod
    def __convertToPolars__(self, df):
        try:
            if isinstance(df, pd.DataFrame):
                return pl.from_pandas(df)        
            else:
                return pl.DataFrame(df)
        except:
            raise TypeError(f"The input dataframe is not a pandas or polars dataframe. The dataframe of type {type(df)} was passed, which is not supported by the comparison utility.")

    def __init__( self, tested: pl.DataFrame, expected: pl.DataFrame, key=None, tolerance: float = 10e-6, verbose: bool = False, progress_bar=None) -> None:
        self.getDataType = lambda df, series: df[series].dtype if series in df.columns else "Does not exist"
        self.verbose = verbose
        self.tested = tested
        self.expected = expected
        self.key = key
        self.report = {}
        self._result = {}
        self._tolerance = tolerance
        self.row_and_column_counts = pl.DataFrame()
        self._numeric_types = [ pl.Int64, pl.Float64, pl.UInt64, pl.Int32, pl.Float32, pl.UInt32, pl.Int16, pl.UInt16, pl.Int8, pl.Int8, ]
        self._datetime_types = [pl.Date, pl.Datetime]
        self._boolean_types = [pl.Boolean]
        self._string_types = [pl.Utf8]
        self._list_types = ["List"]
        self._category_types = [pl.Categorical]
        self.intersection = set(self.expected.columns).intersection(
            set(self.tested.columns)
        )
        
        if self.verbose:
            logger.info(f"Common columns in both the dataframes: {self.intersection}")
        self.__union__ = set(self.expected.columns).union(
            set(self.tested.columns))
        
        
        
        if self.__notPolars__(self.tested):
            self.tested = self.__convertToPolars__(self.tested)
        if self.__notPolars__(self.expected):
            self.expected = self.__convertToPolars__(self.expected)
        progress_bar.update(5)
        progress_bar.set_description("Testing column names")
        self.testColumnNames()
        progress_bar.set_description("Testing counts")
        progress_bar.update(5)
        self.testCounts()
        progress_bar.set_description("Testing schema")
        self.testSchema()
        
        progress_bar.update(5)    
        progress_bar.set_description("Segregating Columns")
        self.__partitionOnColumnTypes__()
        if self.verbose:
            logger.info(f"Intersection of columns in both the dataframes (after schema test): {self.intersection}")
        progress_bar.update(5)
        progress_bar.set_description("Matching row counts")
        self.matchRowCounts()
        progress_bar.update(20)
        progress_bar.set_description("Sorting the data if a key is provided")
        self.__sort__()
        progress_bar.update(5)
        self.result = self._result["count_result"]['result'] and self._result["column_names_result"] and self._result["schema_result"]
        

    def __sort__(self) -> None:
        if self.key is None:
            logger.info("No key provided. Performing comparison without sorting.")
        else:
            if self.verbose:
                logger.info(f"Sorting the dataframes on the key: {self.key}")                
            self.tested = self.tested.sort(by=self.key, descending=False)
            self.expected = self.expected.sort(by=self.key, descending=False)

    def testCounts(self):
        """
        Compares the number of columns and rows between the dataframes.

        Returns:
            polars.DataFrame: A dataframe containing the counts and the difference between expected and tested dataframes.
        """
        row_and_column_counts = {
            "Attributes": ["No of columns", "No of rows"],
            "Expected": [len(self.expected.columns), self.expected.shape[0]],
            "Tested": [len(self.tested.columns), self.tested.shape[0]],
            "Difference": [
                len(self.expected.columns) - len(self.tested.columns),
                self.expected.shape[0] - self.tested.shape[0],
            ],
        }
        self._result["count_result"] = {
            "row_count_result": row_and_column_counts["Difference"][0] == 0,
            "column_count_result": row_and_column_counts["Difference"][1] == 0,
            "result": row_and_column_counts["Difference"][0] == 0
            and row_and_column_counts["Difference"][1] == 0,
        }
        self.report[
            "count_report"
        ] = f"COUNT COMPARISON: {stringify_result(self._result['count_result']['result'])}\n{tabulate(row_and_column_counts, headers='keys', tablefmt='psql')}"
        

        return self.row_and_column_counts

    def testColumnNames(self):
        """
        Compares the column names between the dataframes.

        Returns:
            dict: A dictionary containing the intersection and difference between expected and tested dataframes.
        """

        self.column_comparison = {
            "Expected ∩ Tested": self.intersection,
            "Expected ∪ Tested": self.__union__,
            "Expected - Tested": set(self.expected.columns) - set(self.tested.columns),
            "Tested - Expected": set(self.tested.columns) - set(self.expected.columns),
        }
        self._result["column_names_result"] = (
            len(self.column_comparison["Expected ∩ Tested"])
            == len(self.expected.columns)
            == len(self.tested.columns)
        )

        self.report[
            "column_names_report"
        ] = f"COLUMNS COMPARISON: {stringify_result(self._result['column_names_result'])}\n{tabulate(self.column_comparison, headers='keys', tablefmt='psql')}"
        return self.column_comparison

    def matchRowCounts(self):
        self.row_mismatch = False
        if not self.key is None:
            number_unique_keys = {'expected': self.expected[self.key].unique().shape[0], 'tested': self.tested[self.key].unique().shape[0]}
            if self.verbose:
                logger.info(f"Number of unique keys in the expected dataframe: {number_unique_keys['expected']}")
                logger.info(f"Number of unique keys in the tested dataframe: {number_unique_keys['tested']}")
                logger.info(f"Taking the intersection of the keys in both the dataframes to perform comparison.")
                logger.info("Performing Anti-Join to find the rows in the expected dataframe but not in the tested dataframe.")
            self.expected_minus_tested = self.expected.with_columns(self.key).join(self.tested.with_columns(self.key), on=self.key, how='anti').select(self.intersection)
            if self.verbose:
                logger.info("Performing Anti-Join to find the rows in the tested dataframe but not in the expected dataframe.")
            self.tested_minus_expected = self.tested.with_columns(self.key).join(self.expected.with_columns(self.key), on=self.key, how='anti').select(self.intersection)                  
            self.expected = self.expected.with_columns(self.key).join(self.tested.with_columns(self.key), on=self.key, how='inner').select(self.intersection)
            self.tested = self.tested.with_columns(self.key).join(self.expected.with_columns(self.key), on=self.key, how='inner').select(self.intersection)                      
            if self.expected_minus_tested.shape[0] != 0:
                self.row_mismatch = True
                self.report['expected_minus_tested'] = self.expected_minus_tested.__str__()
            if self.tested_minus_expected.shape[0] != 0:
                self.row_mismatch = True
                self.report['tested_minus_expected'] = self.tested_minus_expected.__str__()
            if self.verbose:
                logger.info(f"Number of unique keys in the expected dataframe after taking the intersection: {self.expected.shape[0]}")
                logger.info(f"Number of unique keys in the tested dataframe after taking the intersection: {self.tested.shape[0]}")          
        else:
            if self.expected.shape[0] != self.tested.shape[0]:
                if self.verbose:
                    logger.info(f"Number of unique keys in the expected dataframe after taking the intersection: {self.expected.shape[0]}")
                    logger.info(f"Number of unique keys in the tested dataframe after taking the intersection: {self.tested.shape[0]}")  
                logger.info( "The number of rows in the expected and tested dataframes do not match. \nTruncating the dataframes to the same number of rows." )
                min_rows = min(self.expected.shape[0], self.tested.shape[0])
                self.expected = self.expected.head(min_rows)
                self.tested = self.tested.head(min_rows)
                logger.info( f"Dataframes have been truncated to the same number of rows. Since no key is provided, the first {min_rows} of both the dataframes have been taken." )

    def testSchema(self):
        all_columns = set(self.expected.columns).union(
            set(self.tested.columns))
        self.schema_comparison = {
            "Column": list(all_columns),
            "Expected": [
                self.getDataType(self.expected, column) for column in all_columns
            ],
            "Tested": [self.getDataType(self.tested, column) for column in all_columns],
            "Result": [stringify_result(self.getDataType(self.expected, column) == self.getDataType(self.tested, column)) for column in all_columns],
        }
        self.mismatched_schema = [
            column
            for column in all_columns
            if self.getDataType(self.expected, column)
            != self.getDataType(self.tested, column)
        ]
        if self.verbose:
            logger.info(f"Schema comparison: {self.schema_comparison}")
            logger.info(f"Mismatched schema: {self.mismatched_schema}")
        self._result["schema_result"] = len(self.mismatched_schema) == 0
        self.report[
            "schema_report"
        ] = f"SCHEMA COMPARISON: {stringify_result(self._result['schema_result'])}\n{tabulate(self.schema_comparison, headers=['Column', 'Expected', 'Tested', 'Result'], tablefmt='psql')}"
        if len(self.mismatched_schema) != 0:
            self.report["schema_report"] += f"\nColumns with mismatched schema: {self.mismatched_schema}\nThese columns would be ignored while performing value-by-value comparison for all datatypes."      
        return self._result["schema_result"]

    def __partitionOnColumnTypes__(self):
        intersection = self.intersection - set(self.mismatched_schema)
        self.intersection = intersection
        self.testSchema()
        self._numeric_columns = [
            column
            for column in intersection
            if self.getDataType(self.expected, column) in self._numeric_types
        ]
        self._datetime_columns = [
            column
            for column in intersection
            if self.getDataType(self.expected, column) in self._datetime_types
        ]
        self._boolean_columns = [
            column
            for column in intersection
            if self.getDataType(self.expected, column) in self._boolean_types
        ]
        self._string_columns = [
            column
            for column in intersection
            if self.getDataType(self.expected, column) in self._string_types
        ]
        self._list_columns = [
            column
            for column in intersection
            if self.getDataType(self.expected, column) in self._list_types
        ]
        self._category_columns = [
            column
            for column in intersection
            if self.getDataType(self.expected, column) in self._category_types
        ]

        self.column_list = {
            "Numeric Columns": self._numeric_columns,
            "Datetime Columns": self._datetime_columns,
            "Boolean Columns": self._boolean_columns,
            "String Columns": self._string_columns,
            "List Columns": self._list_columns,
        }
        if self.verbose:
            logger.info(f"Numeric columns: {self._numeric_columns}")
            logger.info(f"Datetime columns: {self._datetime_columns}")
            logger.info(f"Boolean columns: {self._boolean_columns}")
            logger.info(f"String columns: {self._string_columns}")
            logger.info(f"List columns: {self._list_columns}")
            logger.info(f"Category columns: {self._category_columns}")
        self.report[
            "column_types_report"
        ] = f"COLUMN TYPES: \n{tabulate({k: v for k, v in self.column_list.items() if len(v) > 0}, headers='keys', tablefmt='psql')}"

    def __str__(self):
        text = []        
        text.append(self.report["count_report"])
        text.append(self.report["column_names_report"])
        text.append(self.report["schema_report"])        
        text.append(self.report["column_types_report"])

        if self.row_mismatch:
            text.append("Row counts by key")
            text.append("Rows in the expected dataframe but not in the tested dataframe. Please view the dataframe by <YOUR_COMPARE_OBJECT_NAME>.expected_minus_tested:")
            text.append(self.report['expected_minus_tested'])
            text.append("Rows in the tested dataframe but not in the expected dataframe. Please view the dataframe by calling <YOUR_COMPARE_OBJECT_NAME>.tested_minus_expected:")
            text.append(self.report['tested_minus_expected'])
        return "\n \n".join(text)
