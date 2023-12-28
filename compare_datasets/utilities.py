from compare_datasets.structure import stringify_result
from tabulate import tabulate
import polars as pl
import polars.selectors as cs

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def testSchema(expected, tested, verbose=False):
        getDataType = lambda df, series: df[series].dtype if series in df.columns else "Does not exist"
        all_columns = set(expected.columns).union(
            set(tested.columns))
        schema_comparison = {
            "Column": list(all_columns),
            "Expected": [
                getDataType(expected, column) for column in all_columns
            ],
            "Tested": [getDataType(tested, column) for column in all_columns],
            "Result": [stringify_result(getDataType(expected, column) == getDataType(tested, column)) for column in all_columns],
        }
        mismatched_schema = [
            column
            for column in all_columns
            if getDataType(expected, column)
            != getDataType(tested, column)
        ]
        if verbose:
            logger.info(f"Schema comparison: {schema_comparison}")
            logger.info(f"Mismatched schema: {mismatched_schema}")
        result = len(mismatched_schema) == 0
        if not result:
            report = f"SCHEMA COMPARISON: {stringify_result(result)}\n{tabulate(schema_comparison, headers=['Column', 'Expected', 'Tested', 'Result'], tablefmt='psql')}"
            print(report)
        return result

def __clean_schema__(df, verbose=False):
        verbose and logger.info("\n\n===================Cleaning Schema for keys=====================\n\n")
        verbose and logger.info("Casting columns to super types")
        df = df.with_columns([
            pl.col(name).cast(pl.Datetime) for name in df.select(cs.temporal()).columns
        ])
        df = df.with_columns([
            pl.col(name).cast(pl.Int64) for name in df.select(cs.integer()).columns
        ])
        df = df.with_columns([
            pl.col(name).cast(pl.Float64) for name in df.select(cs.float()).columns
        ])
        return df