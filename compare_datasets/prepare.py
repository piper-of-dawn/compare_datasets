import polars as pl
from tabulate import tabulate
import pandas as pd
from compare_datasets.structure import stringify_result, generate_report
from tqdm import tqdm
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
from compare_datasets.utilities import testSchema
import polars.selectors as cs

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
        result (dict): A dictionary to store the comparison result.
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

    def __init__( self, tested: pl.DataFrame, expected: pl.DataFrame, key=None, tolerance: float = 10e-6, verbose: bool = False, progress_bar=None, low_memory=False, case_sensitive_columns=False, strict_schema=False, numeric_tolerance=4) -> None:
        self.getDataType = lambda df, series: df[series].dtype if series in df.columns else "Does not exist"
        self.numeric_tolerance = numeric_tolerance
        self.low_memory = low_memory
        self.case_sensitive_columns = case_sensitive_columns
        self.strict_schema = strict_schema
        self.verbose = verbose
        
        # Handle single string key
        if not (isinstance(key, list)) and (not key is None):
            self.key = [key]
        else:
            self.key = key
        
        if self.__notPolars__(tested):
            self.verbose and logger.info(f"Converting the tested dataframe to polars. The input dataframe is of type {type(tested)}")
            self.tested = self.__convertToPolars__(tested)
        else:
            self.tested = tested
        if self.__notPolars__(expected):
            self.verbose and logger.info(f"Converting the expected dataframe to polars. The input dataframe is of type {type(expected)}")
            self.expected = self.__convertToPolars__(expected)
        else:
            self.expected = expected  

        
        
        if not self.strict_schema:
            self.expected = self.__clean_schema__(self.expected)
            self.tested = self.__clean_schema__(self.tested)

        if self.verbose:
            logger.info("\n\n=================== Details after cleaning schema =====================\n\n")
            logger.info(f"Expected dataframe columns: \n{self.expected.columns}")
            logger.info(f"Tested dataframe columns: \n{self.tested.columns}")
            logger.info(f"Key columns: \n{self.key}")

        if self.key is not None:            
            self.__verify_keys__()
        
        self.__report__ = {}
        self.result = {}
        self._tolerance = tolerance
        self.row_and_column_counts = pl.DataFrame()
        self._numeric_types = [ pl.Int64, pl.Float64, pl.UInt64, pl.Int32, pl.Float32, pl.UInt32, pl.Int16, pl.UInt16, pl.Int8, pl.Int8, ]
        self._datetime_types = [pl.Date, pl.Datetime]
        self._boolean_types = [pl.Boolean]
        self._string_types = [pl.Utf8]
        self._list_types = ["List"]
        self._category_types = [pl.Categorical]       
        
        if progress_bar is None:
            progress_bar = tqdm(total=100)
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
        self.result = self.result["count_result"]['result'] and self.result["column_names_result"] and self.result["schema_result"]
        self.report = [self.__report__[key] for key in self.__report__ if key in  ["count_report", "column_names_report", "schema_report", "column_types_report"]]
        

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
        self.result["count_result"] = {
            "row_count_result": row_and_column_counts["Difference"][0] == 0,
            "column_count_result": row_and_column_counts["Difference"][1] == 0,
            "result": row_and_column_counts["Difference"][0] == 0
            and row_and_column_counts["Difference"][1] == 0,
        }

        report = {
            "name": "Count Comparison",
            "explanation": "This section compares the number of columns and rows between the dataframes.",
            "result": self.result["count_result"]["result"],
            "report": tabulate(row_and_column_counts, headers="keys", tablefmt="psql"),
            "html_report": tabulate(row_and_column_counts, headers="keys", tablefmt="html"),            
        }

        if self.result["count_result"]["result"]:
            report["conclusion"] = "The number of columns and rows in both the dataframes match."
        else:
            report["conclusion"] = "The number of columns and rows in both the dataframes do not match."

        self.__report__["count_report"] = report
        

        return self.row_and_column_counts

    def testColumnNames(self):
        """
        Compares the column names between the dataframes.

        Returns:
            dict: A dictionary containing the intersection and difference between expected and tested dataframes.
        """
        self.verbose and logger.info("================TESTING COLUMN NAMES================")
    
            
        self.intersection = set(self.expected.columns).intersection(
            set(self.tested.columns)
        )
        if self.verbose:
            logger.info(f"Common columns in both the dataframes: {self.intersection}")
        self.__union__ = set(self.expected.columns).union(
            set(self.tested.columns))
        
        self.column_comparison = {
            "Expected ∩ Tested": self.intersection,
            "Expected ∪ Tested": self.__union__,
            "Expected - Tested": set(self.expected.columns) - set(self.tested.columns),
            "Tested - Expected": set(self.tested.columns) - set(self.expected.columns),
        }
        self.result["column_names_result"] = (
            len(self.column_comparison["Expected ∩ Tested"])
            == len(self.expected.columns)
            == len(self.tested.columns)
        )

        report  = {
            "name": "Column Names Comparison",
            "explanation": "This section compares the column names between the dataframes.",
            "report": tabulate(self.column_comparison, headers="keys", tablefmt="psql"),
            "html_report": tabulate(self.column_comparison, headers="keys", tablefmt="html"),
            "result": stringify_result(self.result["column_names_result"]),
        }

        if self.result["column_names_result"]:
            report["conclusion"] = "The column names in both the dataframes match."
        else:
            report["conclusion"] = "The column names in both the dataframes do not match."


        self.__report__[
            "column_names_report"
        ] =report
        return self.column_comparison

    def matchRowCounts(self):
        
        self.verbose and logger.info("\n\n====================MATCHING ROW COUNTS====================\n\n")
        if not self.key is None:
            number_unique_keys = {'expected': self.expected[self.key].unique().shape[0], 'tested': self.tested[self.key].unique().shape[0]}
            if self.verbose:
                logger.info(f"Number of unique keys in the expected dataframe: {number_unique_keys['expected']}")
                logger.info(f"Number of unique keys in the tested dataframe: {number_unique_keys['tested']}")     
                logger.info(f"Performing Anti-Join to find the mismatched keys in both the dataframes.")           
            self.expected_minus_tested = self.expected.join(self.tested, on=self.key, how='anti').select(self.intersection)
            self.tested_minus_expected = self.tested.join(self.expected, on=self.key, how='anti').select(self.intersection)  
            if self.expected_minus_tested.shape[0] != 0:
                self.__report__['expected_minus_tested'] = self.expected_minus_tested.__str__()
            if self.tested_minus_expected.shape[0] != 0:
                self.__report__['tested_minus_expected'] = self.tested_minus_expected.__str__()       
            unique_keys_match_expected = (number_unique_keys['expected'] == self.expected.shape[0])
            unique_keys_match_expected and self.verbose and logger.info(f"Number of unique keys in the expected dataframe match the number of rows in the expected dataframe: {unique_keys_match_expected}")
            unique_keys_match_tested = (number_unique_keys['tested'] == self.tested.shape[0])
            unique_keys_match_tested and self.verbose and logger.info(f"Number of unique keys in the tested dataframe match the number of rows in the tested dataframe: {unique_keys_match_tested}")
            unique_keys_match = unique_keys_match_expected and unique_keys_match_tested
            if self.verbose:
                logger.info(f"Non Unique keys match: {not unique_keys_match} | Low memory: {self.low_memory}")
            if (not unique_keys_match) or self.low_memory:
                self.verbose and logger.info(f"Entered non-unique keys / low memory block")
                if not unique_keys_match:                    
                    if self.verbose:
                        logger.info("Entered non-unique keys block")
                        logger.info(f"Number of rows in tested: {self.tested.shape[0]}")
                        logger.info(f"Number of rows in expected: {self.expected.shape[0]}")
                        logger.warn(f"Number of unique keys in the expected dataframe: {number_unique_keys['expected']}")
                        logger.warn(f"Number of unique keys in the tested dataframe: {number_unique_keys['tested']}")             
          
                    print(f"Number of unique keys in the expected dataframe: {number_unique_keys['expected']}")
                    print(f"Number of unique keys in the tested dataframe: {number_unique_keys['tested']}")
                    if self.expected.shape[0] == self.tested.shape[0]:
                        print("\n=====WARNING=====\nThe keys provided do not UNIQUELY identify the rows in the provided dataframes. The comparison will be performed by matching the rows after sorting the dataframes on the provided key.\nIt is highly recommended to provide a column or a set of columns as key that uniquely identifies the rows in the dataframes for a robust comparison.\nIt is highly recommended to provide a column or a set of columns as key that uniquely identifies the rows in the dataframes.\n\n Non unique keys can result in FALSE POSITIVE Test Failures i.e. the comparison will fail even if the dataframes are identical.\n\n")
                    if self.verbose:
                        logger.info( "The number of rows in the expected and tested dataframes do not match. \nTruncating the dataframes to the same number of rows by removing the last remained rows since key is not provided" )
                    if self.expected.shape[0] != self.tested.shape[0]:
                        print("\n=====WARNING=====\nThe number of rows in the expected and tested dataframes do not match and furthermore, the keys provided do not uniquely identify the rows in the dataframes.\nThe dataframes will be truncated to the same number of rows equal to the minimum number of rows in both the dataframes.\nIt is highly recommended to provide a column or a set of columns as key that uniquely identifies the rows in the dataframes for a robust comparison.")
                        min_rows = min(self.expected.shape[0], self.tested.shape[0])                        
                        self.verbose and logger.info(f"Minimum number of rows in both the dataframes: {min_rows}")
                        self.expected = self.expected.head(min_rows)
                        self.tested = self.tested.head(min_rows)          
            else:
                self.verbose and logger.info(f"Taking the inner join of the keys in both the dataframes to perform comparison.")                                   
                self.expected = self.expected.join(self.tested, on=self.key, how='inner').select(self.intersection)
                self.tested = self.tested.join(self.expected, on=self.key, how='inner').select(self.intersection)  
                
                if self.verbose:
                    logger.info(f"Number of unique keys in the expected dataframe after taking the intersection: {self.expected.shape[0]}")
                    logger.info(f"Number of unique keys in the tested dataframe after taking the intersection: {self.tested.shape[0]}")          
        else:
            if self.expected.shape[0] != self.tested.shape[0]:
                if self.verbose:
                    logger.info(f"Number of unique keys in the expected dataframe after taking the intersection: {self.expected.shape[0]}")
                    logger.info(f"Number of unique keys in the tested dataframe after taking the intersection: {self.tested.shape[0]}")  
                logger.info( "The number of rows in the expected and tested dataframes do not match. \nTruncating the dataframes to the same number of rows by removing the last remained rows since key is not provided" )
                self.expected_minus_tested  = "Since key is not provided to identify the rows in the dataframes, the rows in the expected dataframe but not in the tested dataframe cannot be identified."
                self.tested_minus_expected  = "Since key is not provided to identify the rows in the dataframes, the rows in the tested dataframe but not in the expected dataframe cannot be identified."
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
        self.result["schema_result"] = len(self.mismatched_schema) == 0

        report = {
            "name": "Schema Comparison",
            "explanation": "This section compares the schema between the dataframes.",
            "report": tabulate(self.schema_comparison, headers=['Column', 'Expected', 'Tested', 'Result'], tablefmt='psql'),
            "html_report": tabulate(self.schema_comparison, headers=['Column', 'Expected', 'Tested', 'Result'], tablefmt='html'),
            "result": stringify_result(self.result["schema_result"]),
        }
        if self.result["schema_result"]:
            report["conclusion"] = "The schemas of both the dataframes match."
        else:
            report["conclusion"] = "The schemas of both the dataframes do not match."

        self.__report__[ "schema_report" ] = report      
        return self.result["schema_result"]

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

        report  = {
            "name": "Column Types",
            "explanation": "This section shows the column types in both the dataframes.",
            "result": "N.A.",
            "report": tabulate({k: v for k, v in self.column_list.items() if len(v) > 0}, headers='keys', tablefmt='psql'),
            "html_report": tabulate({k: v for k, v in self.column_list.items() if len(v) > 0}, headers='keys', tablefmt='html'),
            "conclusion": "N.A."
        }

        
        self.__report__[
            "column_types_report"
        ] = report
    
    def __convert_column_names_to_upper__(self, df: pl.DataFrame) -> pl.DataFrame:
        for column in df.columns:
            df = df.rename({column: column.upper()})
        return df

    def __str__(self):
        text = []        
        text.append(self.__report__["count_report"])
        text.append(self.__report__["column_names_report"])
        text.append(self.__report__["schema_report"])        
        text.append(self.__report__["column_types_report"])
        text = [generate_report(report) for report in text]
        if self.key is not None:
            if self.expected_minus_tested.shape[0] != 0:
                text.append("Row counts by key")
                text.append("Rows in the expected dataframe but not in the tested dataframe. Please view the dataframe by <YOUR_COMPARE_OBJECT_NAME>.expected_minus_tested:")
                text.append(self.__report__['expected_minus_tested'])
            if self.tested_minus_expected.shape[0] != 0:
                text.append("Rows in the tested dataframe but not in the expected dataframe. Please view the dataframe by calling <YOUR_COMPARE_OBJECT_NAME>.tested_minus_expected:")
                text.append(self.__report__['tested_minus_expected'])
        return "\n \n".join(text)
    
    def __verify_keys__(self):
        if self.verbose:
            logger.info("\n\n=================== Key Verification =====================\n\n")
            logger.info(f"Key provided: {self.key}")
            logger.info(f"Columns in the expected dataframe: {self.expected.columns}")
            logger.info(f"Columns in the tested dataframe: {self.tested.columns}")
        is_tested_valid = all(self.tested.columns.__contains__(col) for col in self.key)
        is_expected_valid = all(self.expected.columns.__contains__(col) for col in self.key)
        if not is_tested_valid:
            raise KeyError(f"One or more key column(s) provided {self.key} does not exist in the tested dataframe.")
        if not is_expected_valid:
            raise KeyError(f"One or more key column(s) provided {self.key} does not exist in the expected dataframe.")
        # Check if any of the key columns are completely null:
        if is_tested_valid:
            is_tested_valid = not any(all(col) for col in self.tested.select(self.key).with_columns(pl.col(k).is_null().all() for k in self.key))
        if is_expected_valid:
            is_expected_valid = not any(all(col) for col in self.expected.select(self.key).with_columns(pl.col(k).is_null().all() for k in self.key))
        if not is_tested_valid:
            raise KeyError(f"One or more key column(s) provided {self.key} only has NULL values. Please provide a key that has non-NULL values in the tested dataframe.")
        if not is_expected_valid:
            raise KeyError(f"One or more key column(s) provided {self.key} only has NULL values. Please provide a key that has non-NULL values in the expected dataframe.")        
        if not testSchema(self.expected.select(self.key), self.tested.select(self.key)):
            if not self.strict_schema:
                raise TypeError(f"The key columns provided {self.key} do not have the same type in both the dataframes.\nPlease provide a key that has the same type in both the dataframes or typecast it.")
            else:
                raise TypeError(f"The key columns provided {self.key} do not have the same type in both the dataframes.\nPlease provide a key that has the same type in both the dataframes or typecast it.\nStrict schema is enabled. You can also disable strict schema by passing strict_schema=False to automatically typecast the key columns to the same type in both the dataframes.")

    def __clean_schema__(self, df):
        self.verbose and logger.info("Convert the keys to uppercase")
  
        if (self.key is not None):
            if self.verbose:
                logger.info(f"Converting the key to uppercase: {self.key}")            
            self.key = [key.upper() for key in self.key]
        
        self.verbose and logger.info("Converting the column names to uppercase")
        self.expected = self.__convert_column_names_to_upper__(self.expected)
        self.tested = self.__convert_column_names_to_upper__(self.tested)
        
        self.verbose and logger.info("\n\n=================== Cleaning Schema =====================\n\n")
        self.verbose and logger.info("Casting columns to super types")
        df = df.with_columns([
            pl.col(name).cast(pl.Datetime) for name in df.select(cs.temporal()).columns
        ])
        df = df.with_columns([
            pl.col(name).cast(pl.Int64) for name in df.select(cs.integer()).columns
        ])
        df = df.with_columns([
            pl.col(name).cast(pl.Float64).round(self.numeric_tolerance) for name in df.select(cs.float()).columns
        ])
        self.verbose and logger.info(f"Floating point tolerance is set to: {self.numeric_tolerance}. All the decimal values will be rounded to {self.numeric_tolerance} decimal places.")
        # 
        return df