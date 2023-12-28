import polars as pl
from tabulate import tabulate
from tqdm import tqdm

from compare_datasets.prepare import PrepareForComparison
from compare_datasets.structure import Comparison, stringify_result, generate_report

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class JaccardSimilarity:
    def __init__(self, prepared_data: PrepareForComparison, verbose=False, progress_bar=None):
        self.column_list = prepared_data.column_list
        progress_bar.set_description("Calculating Jaccard Similarity")
        self.tested = prepared_data.tested
        self.expected = prepared_data.expected
        progress_bar.set_description("Counting Nulls for string columns")
        self.columns_names = prepared_data.intersection
        self.report = {}
        self.report["name"] = "Jaccard Similarity"
        self.compare()

        if verbose:
            logger.info(f"Columns in the expected dataframe: {self.expected.columns}")
            logger.info(f"Columns in the tested dataframe: {self.tested.columns}")
            logger.info(f"Shape of the expected dataframe: {self.expected.shape}")
            logger.info(f"Shape of the tested dataframe: {self.tested.shape}")
            logger.info("Nulls filled with blank string for comparison")
            # logger.info(self.report)

    def jaccard_similarity_calculator(self, s1: pl.Series, s2: pl.Series) -> float:
        """
        This method calculates the Jaccard similarity between two series.
        The Jaccard similarity is the size of the intersection divided by the size of the union of the two series.
        :param s1: The first series.
        :param s2: The second series.
        :return: The Jaccard similarity between the two series.
        """
        s1 = set(s1)
        s2 = set(s2)
        intersection = len(s1.intersection(s2))
        union = len(s1.union(s2))
        if union == intersection == 0:
            return 1
        return intersection / union

    def compare(self):
        report = {}
        report["name"] = "JACCARD SIMILARITY"
        definition = "Jaccard Similarity is defined as the size of the intersection divided by the size of the union of the sets.\nJ(A,B) = |A ∩ B| / |A ∪ B|."
        jaccard_similarity = [
            self.jaccard_similarity_calculator(
                self.expected[column], self.tested[column])
            for column in self.columns_names
        ]
        result = [
            "PASSED" if jaccard_score == 1 else "FAILED"
            for jaccard_score in jaccard_similarity
        ]
        report["result"] = all(
            jaccard_score == 1 for jaccard_score in jaccard_similarity
        )
        report["report"] = tabulate(
            sorted([
                (column, jaccard_score, result)
                for column, jaccard_score, result in zip(
                    self.columns_names, jaccard_similarity, result
                )
            ], key=lambda x: x[1]),
            headers=["Column Name", "Jaccard Similarity", "Result"],
            tablefmt="psql",
        )
        report["html_report"] = tabulate(
            [
                (column, jaccard_score, result)
                for column, jaccard_score, result in zip(
                    self.columns_names, jaccard_similarity, result
                )
            ],
            headers=["Column Name", "Jaccard Similarity", "Result"],
            tablefmt="html",
        )
        report['explanation'] = definition
        if not report["result"]:
            report[
                "conclusion"
            ] = f"The Jaccard similarity between the expected and tested dataframes is not 1 for all columns.\nThis means that the expected and tested dataframes have different values for the same column(s)."
        else:
            report[
                "conclusion"
            ] = f"The Jaccard similarity between the expected and tested dataframes is 1 for all columns. This means that the expected and tested dataframes have the same values for the same column(s)."
        self.report = report
        
    def __repr__(self):
        return generate_report(self.report)