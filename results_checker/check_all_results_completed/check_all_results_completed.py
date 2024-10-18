import os
from results_checker.dynamo_db.dynamo_db import get_processed_results
import ast

from cs_ai_common.logging.internal_logger import InternalLogger

TO_PROCESS = os.getenv("RESOLVER_NAMES", "[]")

def check_all_results_completed(task_id: str) -> tuple:
    _to_process = ast.literal_eval(TO_PROCESS)
    processed = []
    for resolver in _to_process:
        InternalLogger.LogDebug(f"Checking results for resolver: {resolver}")
        resolver = _extract_resolver_name(resolver)
        response = get_processed_results(task_id, resolver)
        InternalLogger.LogDebug(f"Response: {response}")
        _response = response[0] if response else None
        _status = _response["status"]["S"] if "status" in _response else None
        if _status == "COMPLETED":
            processed.append(_response)

    return processed, len(processed) == len(_to_process)

def _extract_resolver_name(resolver: str) -> str:
    return resolver.split("-")[2]
            