import numpy as np
import polars as pl
from tabulate import tabulate
from tqdm import tqdm
from scipy.spatial import distance
from compare_datasets.prepare import PrepareForComparison
from compare_datasets.structure import (
    Comparison,
    stringify_result,
    generate_report,
    format_float,
)
from scipy.linalg import norm
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class NumericComparisons(Comparison):
    def __init__(self, prepared_data: PrepareForComparison, verbose=False, progress_bar=None):
        self.column_list = prepared_data.column_list
        progress_bar.set_description("Preparing Numeric Comparison")
        self.tested = prepared_data.tested.select(self.column_list["Numeric Columns"])
        self.expected = prepared_data.expected.select(
            self.column_list["Numeric Columns"]
        )
        progress_bar.set_description("Counting Nulls for numeric columns")
        null_counts = {
            "tested": self.tested.null_count(),
            "expected": self.expected.null_count(),
        }
        progress_bar.set_description("Filing Nulls with 0 for comparison")
        self.tested = self.tested.with_columns(pl.all().fill_null(0))
        self.expected = self.expected.with_columns(pl.all().fill_null(0))
        self.columns_names = list(self.expected.columns)
        super().validate(
            self.tested,
            self.expected,
            [
                "Int64",
                "Float64",
                "UInt64",
                "Int32",
                "Float32",
                "UInt32",
                "Int16",
                "UInt16",
                "Int8",
                "UInt8",
            ],
        )
        self.data_type = "NUMERIC"
        self.report = {}
        progress_bar.set_description("Calculating Euclidean Distance for numeric columns")   
        self.compare()
        self.report["overall_result"] = ( 
            self.report["result"]
        )
        if verbose:
            logger.info(f"Columns in the expected dataframe: {self.expected.columns}")
            logger.info(f"Columns in the tested dataframe: {self.tested.columns}")
            logger.info(f"Shape of the expected dataframe: {self.expected.shape}")
            logger.info(f"Shape of the tested dataframe: {self.tested.shape}")
            logger.info(
                f"Null counts in the expected dataframe: {null_counts['expected']}"
            )
            logger.info(f"Null counts in the tested dataframe: {null_counts['tested']}")
            logger.info("Nulls filled with 0 for comparison")
            # logger.info(self.report)
        self.result = self.report["overall_result"]

    # @timeit("Euclidean Distance")
    def generate_differenced_dataframe(self):
        """
        Generates a dataframe containing the difference between the expected and tested dataframes.
        """
        differenced = pl.DataFrame(
            [
                (
                    c,
                    distance.euclidean(self.expected[c], self.tested[c]),
                )
                for c in self.columns_names
            ],
            schema=[
                "Column Name",
                "Euclidean Distance",
            ],
        )
        return differenced.with_columns(
            [(pl.col("Euclidean Distance") == 0).alias("Result")]
        )

    def compare(self):
        self.euclidean_differenced = self.generate_differenced_dataframe()
        failed_columns = list(
            self.euclidean_differenced.select(
                "Column Name", "Euclidean Distance"
            ).filter(pl.col("Euclidean Distance") > 0.0)["Column Name"]
        )
        self.differenced = self.tested.select(failed_columns) - self.expected.select(
            failed_columns
        )
        self.report = {}
        self.report["name"] = "COMPARISON FOR NUMERIC COLUMNS"
        self.report["result"] = (
            self.euclidean_differenced["Euclidean Distance"].sum() == 0
        )
        self.report["report"] = tabulate(
            [
                (
                    column,
                    format_float(distance),
                    stringify_result(result),
                )
                for column, distance, result in self.euclidean_differenced.rows()
            ],
            headers=[
                "Column Name",
                "Euclidean Distance",
                "Result",
            ],
            tablefmt="psql",
        )

        self.report[
            "explanation"
        ] = "The numeric comparisons are done using the Euclidean distance. The Euclidean distance is a measure of the straight line distance between two points in a space.\nGiven two points P and Q with coordinates (p1, p2, ..., pn) and (q1, q2, ..., qn) respectively, the Euclidean distance d between P and Q is: d(P, Q) = sqrt((q1 - p1)² + (q2 - p2)² + ... + (qn - pn)²)\nColumns are scaled to have a unit norm (magnitude or length of 1) for calculation of the Euclidean Distance (Normalized). "

        if not self.report["result"]:
            self.report[
                "explanation"
            ] += f"\nThe Euclidean distance between the expected and tested dataframes is not 0 for all columns. This means that the expected and tested dataframes have different numeric values in the same column(s)."
        else:
            self.report[
                "explanation"
            ] += f"\nThe Euclidean distance between the expected and tested dataframes is 0 for all columns. This means that the expected and tested dataframes have the same numeric values for the same column(s)."

    def __repr__(self):
        return generate_report(self.report)