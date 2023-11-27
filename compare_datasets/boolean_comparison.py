from Levenshtein import distance
import polars as pl
from tabulate import tabulate
from tqdm import tqdm

from compare_datasets.prepare import PrepareForComparison
from compare_datasets.structure import Comparison, stringify_result, generate_report

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class BooleanComparisons(Comparison):
    def __init__(self, prepared_data: PrepareForComparison, verbose=False, progress_bar=None):
        self.column_list = prepared_data.column_list
        progress_bar.set_description("Preparing Boolean Comparison")
        self.tested = prepared_data.tested.select(self.column_list["Boolean Columns"])
        self.expected = prepared_data.expected.select( self.column_list["Boolean Columns"] )
        progress_bar.set_description("Counting Nulls for Boolean columns")
        null_counts = {
            "tested": self.tested.null_count(),
            "expected": self.expected.null_count(),
        }
        progress_bar.set_description("Filing Nulls with 'False' for comparison")
        self.tested = self.tested.with_columns(pl.all().fill_null(False))
        self.expected = self.expected.with_columns(pl.all().fill_null(False))
        super().validate(self.tested, self.expected, "Boolean")
        self.data_type = "BOOLEAN"
        self.columns_names = list(self.expected.columns)
        self.report = {}
        self.report["name"] = "Boolean Column Comparison"
        progress_bar.set_description("Performing XNOR operation for Boolean columns")
        self.compare()      
        if verbose:
            logger.info(f"Columns in the expected dataframe: {self.expected.columns}")
            logger.info(f"Columns in the tested dataframe: {self.tested.columns}")
            logger.info(f"Shape of the expected dataframe: {self.expected.shape}")
            logger.info(f"Shape of the tested dataframe: {self.tested.shape}")
            logger.info(
                f"Null counts in the expected dataframe: {null_counts['expected']}"
            )
            logger.info(f"Null counts in the tested dataframe: {null_counts['tested']}")
            logger.info("Nulls filled with blank Boolean for comparison")

        self.result = self.report["result"]
    
    def generate_differenced_dataframe(self):
        """
        Generates a dataframe containing the difference between the expected and tested dataframes.
        """
        return pl.DataFrame(
            [
                pl.Series(
                    s1^s2 for s1, s2 in zip(self.expected[c], self.tested[c])
                ).alias(c)
                for c in self.columns_names
            ]
        )
    
    def compare(self):
        self.differenced = self.generate_differenced_dataframe()
        xnor = pl.Series(
            self.differenced.select(pl.all().sum()).melt()["value"]
        )
        failed_columns = [
            column
            for column, distance in zip(self.columns_names, xnor)
            if distance != True
        ]
        self.differenced = self.differenced.select(failed_columns)
        self.report = {}
        self.report["result"] = xnor.sum() == 0
        self.report["report"] = tabulate(
            [
                (column, distance, stringify_result(result))
                for column, distance, result in zip(
                    self.columns_names,
                    xnor,
                    xnor == 0,
                )
            ],
            headers=["Column Name", "Total XNOR", "Result"],
            tablefmt="psql",
        )
        self.report["differenced"] = self.differenced
        self.report["name"] = f"COMPARISON FOR {self.data_type} COLUMNS"

        self.report[
            "explanation"
        ] = "The boolean comparison is performed by performing an XNOR operation between the expected and tested dataframes.\nIf the XNOR operation returns True, it means that the expected and tested dataframes have the same boolean values in the same column(s)."

        if not self.report["result"]:
            self.report[
                "explanation"
            ] += f"\nThe XNOR operation between the expected and tested dataframes is not True for all columns.\nThis means that the expected and tested dataframes have different boolean values for the same column(s)."
        else:
            self.report[
                "explanation"
            ] += f"\nThe XNOR operation between the expected and tested dataframes is True for all columns.\nThis means that the expected and tested dataframes have the same boolean values for the same column(s)."
        

    def __repr__(self):
        return generate_report(self.report)