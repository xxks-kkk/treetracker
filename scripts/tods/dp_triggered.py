"""
What is the ratio (total # of times DP is triggered) / (total number of deletions) over all queries?
"""
import csv

from plot.constants import TTJHP_JOB_SQLITE_ORDERING_STATS_CSV, TTJHP_SSB_SQLITE_ORDERING_STATS_CSV, \
    TTJHP_TPCH_SQLITE_ORDERING_STATS_CSV


def sum_deletion_propagation_triggered(csv_filename, stats_name):
    total = 0
    with open(csv_filename) as f:
        reader = csv.reader(f)
        for row in reader:
            if row[0] == stats_name:
                # Convert all values except the first column to integers and sum them
                total = sum(int(value) for value in row[1:] if value.strip().isdigit())
                break
    return total

def compute():
    total_dp_triggered = 0.0
    total_dp_triggered += sum_deletion_propagation_triggered(TTJHP_JOB_SQLITE_ORDERING_STATS_CSV, 'numberOfDeletionPropagationTriggered')
    total_dp_triggered += sum_deletion_propagation_triggered(TTJHP_SSB_SQLITE_ORDERING_STATS_CSV, 'numberOfDeletionPropagationTriggered')
    total_dp_triggered += sum_deletion_propagation_triggered(TTJHP_TPCH_SQLITE_ORDERING_STATS_CSV, 'numberOfDeletionPropagationTriggered')
    total_number_of_deletions = 0.0
    total_number_of_deletions += sum_deletion_propagation_triggered(TTJHP_JOB_SQLITE_ORDERING_STATS_CSV, 'totalTuplesRemoved')
    total_number_of_deletions += sum_deletion_propagation_triggered(TTJHP_SSB_SQLITE_ORDERING_STATS_CSV, 'totalTuplesRemoved')
    total_number_of_deletions += sum_deletion_propagation_triggered(TTJHP_TPCH_SQLITE_ORDERING_STATS_CSV, 'totalTuplesRemoved')
    print(f"the ratio (total # of times DP is triggered) / (total number of deletions) over all queries: {total_dp_triggered / total_number_of_deletions}")
    
if __name__ == '__main__':
    compute()