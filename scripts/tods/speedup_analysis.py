"""
Speedup analysis of TTJ-eps compared to other methods in rpt-sheet7.csv
Computes geometric mean, maximum, and minimum speedup for each comparison.
"""
import csv
import math
from pathlib import Path


def load_data_from_csv():
    """Load data from rpt-sheet7.csv"""
    csv_path = Path(__file__).parent / "rpt-sheet7.csv"
    data = {'query': [], 'TTJ-eps': [], 'DuckDB': [], 'YA+': [], 'RPT': [], 'TTJ': []}
    
    with open(csv_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data['query'].append(row['query'])
            data['TTJ-eps'].append(float(row['TTJ-eps']))
            data['DuckDB'].append(float(row['DuckDB']))
            data['YA+'].append(float(row['YA+']))
            data['RPT'].append(float(row['RPT']))
            data['TTJ'].append(float(row['TTJ']))
    
    return data


def compute_speedup_stats(ttj_eps_times, other_times, other_method_name, queries):
    """
    Compute speedup statistics for TTJ-eps vs another method
    Speedup = other_method_time / ttj_eps_time (higher is better for TTJ-eps)
    """
    speedups = [other_times[i] / ttj_eps_times[i] for i in range(len(ttj_eps_times))]
    
    # Geometric mean
    log_sum = sum(math.log(speedup) for speedup in speedups)
    geometric_mean = math.exp(log_sum / len(speedups))
    
    # Maximum speedup and corresponding query
    max_speedup = max(speedups)
    max_query_idx = speedups.index(max_speedup)
    max_query = queries[max_query_idx]
    
    # Minimum speedup and corresponding query
    min_speedup = min(speedups)
    min_query_idx = speedups.index(min_speedup)
    min_query = queries[min_query_idx]
    
    return {
        'method': other_method_name,
        'geometric_mean': geometric_mean,
        'max_speedup': max_speedup,
        'max_query': max_query,
        'min_speedup': min_speedup,
        'min_query': min_query,
        'speedups': speedups
    }


def analyze_speedups():
    """Analyze TTJ-eps speedup compared to all other methods"""
    data = load_data_from_csv()
    
    # Extract data
    queries = data['query']
    ttj_eps_times = data['TTJ-eps']
    
    # Methods to compare against
    other_methods = ['DuckDB', 'YA+', 'RPT', 'TTJ']
    
    results = []
    
    print("TTJ-eps Speedup Analysis")
    print("=" * 50)
    print(f"Total number of queries: {len(queries)}")
    print()
    
    for method in other_methods:
        other_times = data[method]
        stats = compute_speedup_stats(ttj_eps_times, other_times, method, queries)
        results.append(stats)
        
        print(f"TTJ-eps vs {method}:")
        print(f"  Geometric Mean Speedup: {stats['geometric_mean']:.3f}x")
        print(f"  Maximum Speedup: {stats['max_speedup']:.3f}x (Query: {stats['max_query']})")
        print(f"  Minimum Speedup: {stats['min_speedup']:.3f}x (Query: {stats['min_query']})")
        print()
    
    return results


def method_specific_analysis():
    """Print top and bottom speedups for each method individually"""
    data = load_data_from_csv()
    
    queries = data['query']
    ttj_eps_times = data['TTJ-eps']
    other_methods = ['DuckDB', 'YA+', 'RPT', 'TTJ']
    
    print("\nMethod-Specific Speedup Analysis")
    print("=" * 80)
    
    for method in other_methods:
        other_times = data[method]
        method_speedups = []
        
        # Calculate speedups for this method
        for i, query in enumerate(queries):
            speedup = other_times[i] / ttj_eps_times[i]
            method_speedups.append({
                'query': query,
                'speedup': speedup,
                'ttj_eps_time': ttj_eps_times[i],
                'other_time': other_times[i]
            })
        
        # Sort by speedup (highest first)
        method_speedups.sort(key=lambda x: x['speedup'], reverse=True)
        
        print(f"\nTTJ-eps vs {method} - Top 10 Speedups:")
        print("-" * 70)
        print(f"{'Query':<12} {'Speedup':<10} {'TTJ-eps (s)':<12} {'{} (s)'.format(method):<12}")
        print("-" * 70)
        for i in range(min(10, len(method_speedups))):
            row = method_speedups[i]
            print(f"{row['query']:<12} {row['speedup']:8.3f}x {row['ttj_eps_time']:10.6f}   {row['other_time']:10.6f}")
        
        print(f"\nTTJ-eps vs {method} - Bottom 10 Speedups:")
        print("-" * 70)
        print(f"{'Query':<12} {'Speedup':<10} {'TTJ-eps (s)':<12} {'{} (s)'.format(method):<12}")
        print("-" * 70)
        start_idx = max(0, len(method_speedups) - 10)
        for i in range(start_idx, len(method_speedups)):
            row = method_speedups[i]
            print(f"{row['query']:<12} {row['speedup']:8.3f}x {row['ttj_eps_time']:10.6f}   {row['other_time']:10.6f}")
        print()


def detailed_analysis():
    """Print detailed speedup analysis with per-query breakdown"""
    data = load_data_from_csv()
    
    queries = data['query']
    ttj_eps_times = data['TTJ-eps']
    other_methods = ['DuckDB', 'YA+', 'RPT', 'TTJ']
    
    print("\nDetailed Per-Query Speedup Analysis")
    print("=" * 80)
    
    # Calculate speedups for each query
    query_speedups = []
    for i, query in enumerate(queries):
        speedups = {}
        speedups['query'] = query
        total_speedup = 0
        for method in other_methods:
            speedup = data[method][i] / ttj_eps_times[i]
            speedups[f'vs_{method}'] = speedup
            total_speedup += speedup
        speedups['avg_speedup'] = total_speedup / len(other_methods)
        query_speedups.append(speedups)
    
    # Sort by average speedup
    query_speedups.sort(key=lambda x: x['avg_speedup'], reverse=True)
    
    print("Top 10 queries with highest average speedup:")
    print("-" * 80)
    for i in range(min(10, len(query_speedups))):
        row = query_speedups[i]
        print(f"{row['query']:12} | ", end="")
        for method in other_methods:
            speedup = row[f'vs_{method}']
            print(f"{method:8}: {speedup:6.2f}x | ", end="")
        print(f"Avg: {row['avg_speedup']:6.2f}x")
    
    print("\nBottom 10 queries with lowest average speedup:")
    print("-" * 80)
    start_idx = max(0, len(query_speedups) - 10)
    for i in range(start_idx, len(query_speedups)):
        row = query_speedups[i]
        print(f"{row['query']:12} | ", end="")
        for method in other_methods:
            speedup = row[f'vs_{method}']
            print(f"{method:8}: {speedup:6.2f}x | ", end="")
        print(f"Avg: {row['avg_speedup']:6.2f}x")


if __name__ == '__main__':
    # Run the main analysis
    results = analyze_speedups()
    
    # Run method-specific analysis
    method_specific_analysis()
    
    # Run detailed analysis
    detailed_analysis()
    
    # Summary table
    print("\nSummary Table")
    print("=" * 60)
    print(f"{'Method':<10} {'Geo Mean':<10} {'Max':<10} {'Min':<10}")
    print("-" * 60)
    for result in results:
        print(f"{result['method']:<10} {result['geometric_mean']:8.3f}x "
              f"{result['max_speedup']:8.3f}x {result['min_speedup']:8.3f}x")
