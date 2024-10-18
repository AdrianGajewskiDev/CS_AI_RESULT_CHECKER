from typing import Dict, List
from cs_ai_common.models.resolvers import TaskStatisticsModel, ResolverStatsModel

def build_stats(resolvers_stats: List[Dict]) -> TaskStatisticsModel:
    stats: list = []
    for resolver in resolvers_stats:
        _resolver_name = resolver["resolver"]["S"]
        _resolver_stats = int(resolver["ads_found"]["S"])
        stats.append(ResolverStatsModel(
            ResolverName=_resolver_name,
            TotalAds=_resolver_stats
        ))

    return TaskStatisticsModel(Stats=stats)