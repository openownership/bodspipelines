import tracemalloc

def top_mem():
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics("lineno")
    print("---------------------------------------------------------")
    [print(stat) for stat in top_stats]
    print("---------------------------------------------------------")
