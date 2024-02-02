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
        # I don't have a good feeling about this function. Find a better way to do it.
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



def generate_report (report: dict):
    if not isinstance(report['result'], bool) and not report['result'] is None:
        raise TypeError(f"Expected boolean, got {type(report['result'])} in the report generator in the {report['name']} comparison. Rogue value: {report['result']}")
    name_length = len(report["name"])
    return f"""
{report["name"]}: 
{"="*name_length}
RESULT: {stringify_result(report["result"]) if isinstance(report["result"], bool) else "Not Applicable"}
{report["report"]}
{report["explanation"]}
{report["conclusion"]}
"""


