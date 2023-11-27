import polars as pl
from tabulate import tabulate
from compare_datasets.prepare import PrepareForComparison
from compare_datasets.structure import Comparison, stringify_result, timeit, generate_report
from datetime import datetime
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DateTimeComparisons(Comparison):
    def __init__(self, prepared_data: PrepareForComparison, verbose=False, progress_bar=None):
        self.column_list = prepared_data.column_list
        progress_bar.set_description("Preparing Datetime Comparison")
        self.tested = prepared_data.tested.select(self.column_list["Datetime Columns"])
        self.expected = prepared_data.expected.select(
            self.column_list["Datetime Columns"]
        )
 
        progress_bar.set_description("Counting Nulls for Datetime columns")
        null_counts = {
            "tested": self.tested.null_count(),
            "expected": self.expected.null_count(),
        }
        progress_bar.set_description("Filing Nulls with today's date for comparison")
        self.tested = self.tested.with_columns(pl.all().fill_null(datetime.today().date()))
        self.expected = self.expected.with_columns(pl.all().fill_null(datetime.today().date()))
        super().validate(self.tested, self.expected, ["Date", "DateTime"])
        self.data_type = "DATETIME"
        self.columns_names = list(self.expected.columns)
        self.report = {}
        self.report["name"] = "Datetime Column Comparison"
        progress_bar.set_description("Calculating time-delta for datetime column")
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
            logger.info("Nulls filled with today's datefor comparison")
            logger.info(self.report)
        self.result = self.report["overall_result"]
            
    def generate_differenced_dataframe(self):
        """
        Generates a dataframe containing the difference between the expected and tested dataframes.
        """
        return pl.DataFrame(
            [
                pl.Series(
                    (s1- s2) for s1, s2 in zip(self.expected[c], self.tested[c])
                ).alias(c)
                for c in self.columns_names
            ]
        )
        
    def compare(self):
        self.date_time_differenced = self.generate_differenced_dataframe()
        time_delta = pl.Series(
            self.date_time_differenced.select(pl.all().sum()).melt()["value"].cast(pl.Float64)/1_000_000
        )
        failed_columns = [
            column
            for column, distance in zip(self.columns_names, time_delta)
            if distance != 0.0
        ]       
 
        self.differenced = self.tested.select(failed_columns) - self.expected.select(
            failed_columns
        )
        self.report = {}
        self.report["name"] = "COMPARISON FOR DATETIME COLUMNS"
        self.report["result"] = (
            len(failed_columns) == 0
        )
        self.report["report"] = tabulate(
            [
                (
                    column,
                    f"{str(distance)}s",
                    stringify_result(result),
                )
                for column, distance, result in zip(
                    self.columns_names,
                    time_delta,
                    time_delta == 0.0
                )
            ],
            headers=[
                "Column Name",
                "Time Delta",
                "Result",
            ],
            tablefmt="psql",
        )

        self.report[
            "explanation"
        ] = "The date-time comparison is done by calculating the time-delta between the expected and tested dataframes. The time-delta is calculated by subtracting the timestamp in tested dataframe from the expected dataframe. If the time-delta is 0s, then the expected and tested dataframes have the same date-time values for the same column(s)."

        if not self.report["result"]:
            self.report[
                "explanation"
            ] += f"\nThe time-delta between the expected and tested dataframes is not 0s for all columns. This means that the expected and tested dataframes do not have the same date-time values for the same column(s)."
        else:
            self.report[
                "explanation"
            ] += f"\nThe time-delta between the expected and tested dataframes is 0s for all columns. This means that the expected and tested dataframes have the same date-time values for the same column(s)."

    def __repr__(self):
        return generate_report(self.report)
