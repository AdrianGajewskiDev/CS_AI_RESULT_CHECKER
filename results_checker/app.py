
from results_checker.check_results.check_results import check_results


def handler(event: dict, context):
    return check_results(event)