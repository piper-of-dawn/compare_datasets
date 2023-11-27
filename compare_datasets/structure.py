from abc import ABC, abstractmethod
from tabulate import tabulate
import logging
from scipy.spatial import distance
import polars as pl
from time import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Comparison(ABC):
    @abstractmethod
    def generate_differenced_dataframe(self):
        pass

    @abstractmethod
    def compare(self):
        pass

    # @abstractmethod
    def validate(self, tested, expected, data_type, verbose=False):
        data_type = [data_type] if isinstance(data_type, str) else data_type
        schema_of_expected = expected.schema
        schema_of_tested = tested.schema
        if verbose:
            logger.info([str(dtype) for dtype in schema_of_expected.values()])
            logger.info(
                [str(dtype) == data_type for dtype in schema_of_expected.values()]
            )
            logger.info(data_type)
        if not all(str(dtype) in data_type for dtype in schema_of_expected.values()):
            logger.info(
                f"\n{tabulate( [ (name, dtype) for name, dtype in schema_of_expected.items() if dtype != data_type ], headers=['Column Name', 'Data Type'] )}"
            )
            raise TypeError(
                f"Non-{data_type} column passed to the {data_type} comparison utility"
            )

    


def stringify_result(result):
    """
    This method is used to convert the result into a string format.
    :param result: The result to be converted into string format.
    :return: "PASSED" if result is True, else "FAILED".
    """
    return "PASSED" if result else "FAILED"


def timeit(name=""):
    def time_it(func):
        def wrapper(*args, **kwargs):
            start = time()
            result = func(*args, **kwargs)
            end = time()
            print(
                f"Function {name} took {(end - start)*1000:.2f} milliseconds. 1 millisecond is 1/1000th of a second"
            )
            return result

        return wrapper

    return time_it


def format_float(float_value):
    return f"{float_value:.2f}"

def jaccard_similarity_calculator(s1: pl.Series, s2: pl.Series) -> float:
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


def generate_report (report):
    name_length = len(report["name"])
    return f"""
{report["name"]}: 
{"="*name_length}
RESULT: {stringify_result(report["result"])}
{report["report"]}
{report["explanation"]}
    """

def calculate_jaccard_similarity(tested, expected, columns_names):
        report = {}
        report["name"] = "JACCARD SIMILARITY"
        definition = "Jaccard Similarity is defined as the size of the intersection divided by the size of the union of the sets.\nJ(A,B) = |A ∩ B| / |A ∪ B|."
        jaccard_similarity = [
            jaccard_similarity_calculator(expected[column], tested[column])
            for column in columns_names
        ]
        result = [
            "PASSED" if jaccard_score == 1 else "FAILED"
            for jaccard_score in jaccard_similarity
        ]
        report["result"] = all(
            jaccard_score == 1 for jaccard_score in jaccard_similarity
        )
        report["report"] = tabulate(
            [
                (column, jaccard_score, result)
                for column, jaccard_score, result in zip(
                    columns_names, jaccard_similarity, result
                )
            ],
            headers=["Column Name", "Jaccard Similarity", "Result"],
            tablefmt="psql",
        )

        if not report["result"]:
            report[
                "explanation" 
            ] = f"{definition}\nThe Jaccard similarity between the expected and tested dataframes is not 1 for all columns.\nThis means that the expected and tested dataframes have different values for the same column(s)."
        else:
            report["jaccard_similarity"][
                "explanation"
            ] = f"{definition}\nThe Jaccard similarity between the expected and tested dataframes is 1 for all columns. This means that the expected and tested dataframes have the same values for the same column(s)."
        return report

