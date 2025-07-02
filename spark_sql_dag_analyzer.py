import json
import requests
import time
import pandas as pd
from collections import defaultdict
import re

class SparkUIMetricsExtractor:
    def __init__(self, spark_ui_url="http://localhost:4040"):
        """
        Initialize the metrics extractor
        
        Args:
            spark_ui_url: URL of your Spark UI (default: http://localhost:4040)
        """
        self.base_url = spark_ui_url
        self.api_url = f"{self.base_url}/api/v1"
    
    def get_application_info(self):
        """Get basic application information"""
        try:
            response = requests.get(f"{self.api_url}/applications")
            return response.json()[0] if response.status_code == 200 else None
        except:
            return None
    
    def get_recent_jobs(self, limit=10):
        """Get recent jobs with timing information"""
        try:
            response = requests.get(f"{self.api_url}/applications/{self.get_app_id()}/jobs")
            if response.status_code == 200:
                jobs = response.json()
                return sorted(jobs, key=lambda x: x['submissionTime'], reverse=True)[:limit]
        except:
            return []
    
    def get_app_id(self):
        """Get the current application ID"""
        app_info = self.get_application_info()
        return app_info['id'] if app_info else None
    
    def get_stages_for_job(self, job_id):
        """Get stages for a specific job"""
        try:
            app_id = self.get_app_id()
            response = requests.get(f"{self.api_url}/applications/{app_id}/stages")
            if response.status_code == 200:
                all_stages = response.json()
                # Filter stages that belong to this job
                job_stages = [stage for stage in all_stages if job_id in stage.get('jobIds', [])]
                return job_stages
        except:
            return []
    
    def get_stage_details(self, stage_id):
        """Get detailed metrics for a specific stage"""
        try:
            app_id = self.get_app_id()
            response = requests.get(f"{self.api_url}/applications/{app_id}/stages/{stage_id}")
            if response.status_code == 200:
                return response.json()[0]  # Returns list with one element
        except:
            return None
    
    def get_sql_execution_details(self, execution_id):
        """Get SQL execution details including DAG info"""
        try:
            app_id = self.get_app_id()
            response = requests.get(f"{self.api_url}/applications/{app_id}/sql/{execution_id}")
            if response.status_code == 200:
                return response.json()
        except:
            return None
    
    def get_recent_sql_executions(self, limit=5):
        """Get recent SQL executions"""
        try:
            app_id = self.get_app_id()
            response = requests.get(f"{self.api_url}/applications/{app_id}/sql")
            if response.status_code == 200:
                executions = response.json()
                return sorted(executions, key=lambda x: x['submissionTime'], reverse=True)[:limit]
        except:
            return []

def analyze_last_execution(ui_url="http://localhost:4040", execution_id=None):
    """
    Analyze a specific SQL execution or the most recent one and print detailed metrics
    
    Args:
        ui_url: Spark UI URL
        execution_id: Specific execution ID to analyze. If None, analyzes the most recent execution
    """
    extractor = SparkUIMetricsExtractor(ui_url)
    
    print("=== Spark Execution Analysis ===")
    
    if execution_id is not None:
        # Analyze specific execution
        print(f"\\nAnalyzing SQL Execution ID: {execution_id}")
        
        execution_details = extractor.get_sql_execution_details(execution_id)
        if not execution_details:
            print(f"No execution found with ID: {execution_id}")
            return
        
        target_execution = execution_details
        
    else:
        # Get recent SQL executions and use the latest
        sql_executions = extractor.get_recent_sql_executions(limit=3)
        
        if not sql_executions:
            print("No SQL executions found")
            return
        
        target_execution = sql_executions[0]
        execution_id = target_execution['id']
        print(f"\\nLatest SQL Execution ID: {execution_id}")
        
        # Get detailed execution info
        execution_details = extractor.get_sql_execution_details(execution_id)
        if execution_details:
            target_execution = execution_details
    
    print(f"Description: {target_execution.get('description', 'N/A')}")
    print(f"Duration: {target_execution.get('duration', 0) / 1000:.2f} seconds")
    print(f"Status: {target_execution.get('status', 'N/A')}")
    
    print(f"\\nDetailed Metrics:")
    print(f"Submission Time: {target_execution.get('submissionTime', 'N/A')}")
    
    # Get associated jobs
    jobs = extractor.get_recent_jobs(limit=5)
    
    print(f"\\nRecent Jobs:")
    for job in jobs:
        job_id = job['jobId']
        duration = job.get('duration', 0) / 1000 if job.get('duration') else 0
        status = job.get('status', 'N/A')
        num_tasks = job.get('numTasks', 0)
        
        print(f"  Job {job_id}: {duration:.2f}s, {num_tasks} tasks, Status: {status}")
        
        # Get stages for this job
        stages = extractor.get_stages_for_job(job_id)
        for stage in stages:
            stage_id = stage['stageId']
            stage_duration = stage.get('executorRunTime', 0) / 1000
            num_tasks = stage.get('numCompleteTasks', 0)
            
            print(f"    Stage {stage_id}: {stage_duration:.2f}s, {num_tasks} tasks")
            
            # Get detailed stage metrics
            stage_details = extractor.get_stage_details(stage_id)
            if stage_details:
                metrics = stage_details.get('executorSummary', {})
                if metrics:
                    print(f"      Shuffle Read: {metrics.get('shuffleReadBytes', 0) / 1024**3:.2f} GB")
                    print(f"      Shuffle Write: {metrics.get('shuffleWriteBytes', 0) / 1024**3:.2f} GB")

def list_recent_sql_executions(ui_url="http://localhost:4040", limit=10):
    """
    List recent SQL executions with their IDs for easy reference
    
    Args:
        ui_url: Spark UI URL
        limit: Number of recent executions to show
    """
    extractor = SparkUIMetricsExtractor(ui_url)
    
    print("=== Recent SQL Executions ===")
    
    sql_executions = extractor.get_recent_sql_executions(limit=limit)
    
    if not sql_executions:
        print("No SQL executions found")
        return
    
    print(f"{'ID':<5} {'Duration (s)':<12} {'Status':<10} {'Description'}")
    print("-" * 80)
    
    for execution in sql_executions:
        exec_id = execution['id']
        duration = execution.get('duration', 0) / 1000
        status = execution.get('status', 'N/A')
        description = execution.get('description', 'N/A')
        
        # Truncate long descriptions
        if len(description) > 50:
            description = description[:47] + "..."
        
        print(f"{exec_id:<5} {duration:<12.2f} {status:<10} {description}")
    
    return sql_executions

def track_execution_with_timing(df_operation, operation_name="Operation"):
    """
    Execute a DataFrame operation while tracking timing and UI metrics
    
    Args:
        df_operation: A lambda or function that performs the DataFrame operation
        operation_name: Name for the operation
    
    Returns:
        Result of the operation and timing info
    """
    
    print(f"\\n=== Executing {operation_name} ===")
    
    # Record start time
    start_time = time.time()
    
    # Execute the operation
    result = df_operation()
    
    # Record end time
    end_time = time.time()
    wall_clock_time = end_time - start_time
    
    print(f"Wall clock time: {wall_clock_time:.2f} seconds")
    
    # Wait a moment for UI to update
    time.sleep(1)
    
    # Analyze the execution
    analyze_last_execution()
    
    return result, wall_clock_time

# Convenience functions for common operations
def time_dataframe_action(df, action_name="count"):
    """
    Time a specific DataFrame action and get Spark UI metrics
    
    Args:
        df: PySpark DataFrame
        action_name: Name of the action ('count', 'collect', 'show', etc.)
    """
    
    if action_name == "count":
        operation = lambda: df.count()
    elif action_name == "collect":
        operation = lambda: df.collect()
    elif action_name == "show":
        operation = lambda: df.show()
    elif action_name == "first":
        operation = lambda: df.first()
    else:
        raise ValueError(f"Unsupported action: {action_name}")
    
    result, timing = track_execution_with_timing(operation, f"df.{action_name}()")
    return result, timing

# Example usage:
"""
# List recent executions to see available IDs
list_recent_sql_executions()

# Analyze the most recent execution (default behavior)
analyze_last_execution()

# Analyze a specific execution by ID
analyze_last_execution(execution_id=42)

# Basic usage with different UI URL
analyze_last_execution(ui_url="http://localhost:4041", execution_id=15)

# Track a specific operation
df = generate_large_dataframe_sql(1000000)
result, timing = time_dataframe_action(df, "count")
print(f"Count result: {result}, Wall clock: {timing:.2f}s")

# Then analyze that specific execution
analyze_last_execution(execution_id=43)  # Use the ID you want to investigate

# For your salting experiments:
def salted_groupby():
    # Your salting code here
    return salted_df.groupBy("col_a").count().collect()

result, timing = track_execution_with_timing(salted_groupby, "Salted GroupBy")
"""

class SparkExecutionAnalyzer:
    def __init__(self, execution_data):
        """
        Initialize with execution data from analyze_last_execution()
        
        Args:
            execution_data: Dict containing the execution plan data
        """
        self.data = execution_data
        self.nodes = {node['nodeId']: node for node in execution_data.get('nodes', [])}
        self.edges = execution_data.get('edges', [])
    
    def extract_shuffle_metrics(self):
        """Extract and analyze shuffle operations"""
        shuffle_ops = []
        
        for node in self.nodes.values():
            node_name = node.get('nodeName', '')
            if 'Exchange' in node_name or 'Shuffle' in node_name:
                metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
                
                shuffle_info = {
                    'node_id': node['nodeId'],
                    'node_name': node_name,
                    'shuffle_bytes_written': self._parse_size_metric(metrics.get('shuffle bytes written', '0')),
                    'shuffle_write_time': self._parse_time_metric(metrics.get('shuffle write time', '0')),
                    'fetch_wait_time': self._parse_time_metric(metrics.get('fetch wait time', '0')),
                    'data_size': self._parse_size_metric(metrics.get('data size', '0')),
                    'remote_bytes_read': self._parse_size_metric(metrics.get('remote bytes read', '0')),
                    'local_bytes_read': self._parse_size_metric(metrics.get('local bytes read', '0')),
                    'output_rows': self._parse_numeric_metric(metrics.get('output rows', '0'))
                }
                shuffle_ops.append(shuffle_info)
        
        return sorted(shuffle_ops, key=lambda x: x['shuffle_write_time'], reverse=True)
    
    def extract_join_metrics(self):
        """Extract and analyze join operations"""
        join_ops = []
        
        for node in self.nodes.values():
            node_name = node.get('nodeName', '')
            if 'Join' in node_name:
                metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
                
                join_info = {
                    'node_id': node['nodeId'],
                    'node_name': node_name,
                    'op_time': self._parse_time_metric(metrics.get('op time', '0')),
                    'build_time': self._parse_time_metric(metrics.get('build time', '0')),
                    'output_rows': self._parse_numeric_metric(metrics.get('output rows', '0')),
                    'build_side_size': self._parse_size_metric(metrics.get('build side size', '0'))
                }
                join_ops.append(join_info)
        
        return sorted(join_ops, key=lambda x: x['op_time'], reverse=True)
    
    def extract_codegen_metrics(self):
        """Extract WholeStageCodegen metrics"""
        codegen_ops = []
        
        for node in self.nodes.values():
            node_name = node.get('nodeName', '')
            if 'WholeStageCodegen' in node_name:
                metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
                
                codegen_info = {
                    'node_id': node['nodeId'],
                    'node_name': node_name,
                    'duration': self._parse_time_metric(metrics.get('duration', '0')),
                    'codegen_id': node.get('wholeStageCodegenId', 'N/A')
                }
                codegen_ops.append(codegen_info)
        
        return sorted(codegen_ops, key=lambda x: x['duration'], reverse=True)
    
    def find_bottlenecks(self, top_n=5):
        """Identify the top performance bottlenecks"""
        all_operations = []
        
        # Collect all operations with timing info
        for node in self.nodes.values():
            node_name = node.get('nodeName', '')
            metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
            
            # Extract various timing metrics
            times = {}
            for metric_name in ['op time', 'duration', 'shuffle write time', 'fetch wait time', 'scan time']:
                if metric_name in metrics:
                    times[metric_name] = self._parse_time_metric(metrics[metric_name])
            
            if times:
                max_time = max(times.values())
                max_metric = max(times.keys(), key=lambda k: times[k])
                
                all_operations.append({
                    'node_id': node['nodeId'],
                    'node_name': node_name,
                    'max_time_seconds': max_time,
                    'metric_type': max_metric,
                    'output_rows': self._parse_numeric_metric(metrics.get('output rows', '0'))
                })
        
        return sorted(all_operations, key=lambda x: x['max_time_seconds'], reverse=True)[:top_n]
    
    def analyze_skew_indicators(self):
        """Look for indicators of data skew"""
        skew_indicators = []
        
        for node in self.nodes.values():
            metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
            
            # Look for high fetch wait times (indicates stragglers)
            fetch_wait = self._parse_time_metric(metrics.get('fetch wait time', '0'))
            if fetch_wait > 60:  # More than 1 minute
                skew_indicators.append({
                    'node_id': node['nodeId'],
                    'node_name': node.get('nodeName', ''),
                    'issue': 'High fetch wait time',
                    'value': f"{fetch_wait:.1f} seconds",
                    'severity': 'high' if fetch_wait > 3600 else 'medium'
                })
            
            # Look for operations with high max times vs typical times
            op_time_str = metrics.get('op time', '')
            if 'max' in op_time_str and 'med' in op_time_str:
                max_time = self._extract_max_from_time_range(op_time_str)
                med_time = self._extract_med_from_time_range(op_time_str)
                if max_time > 0 and med_time > 0 and max_time / med_time > 5:
                    skew_indicators.append({
                        'node_id': node['nodeId'],
                        'node_name': node.get('nodeName', ''),
                        'issue': 'Task time skew',
                        'value': f"Max: {max_time:.1f}s, Median: {med_time:.1f}s (ratio: {max_time/med_time:.1f})",
                        'severity': 'high' if max_time / med_time > 10 else 'medium'
                    })
        
        return skew_indicators
    
    def generate_summary_report(self):
        """Generate a comprehensive summary report"""
        print("=" * 80)
        print("SPARK EXECUTION ANALYSIS REPORT")
        print("=" * 80)
        
        # Basic info
        print(f"\nExecution ID: {self.data.get('id', 'N/A')}")
        print(f"Status: {self.data.get('status', 'N/A')}")
        print(f"Duration: {self.data.get('duration', 0) / 1000:.1f} seconds")
        
        # Top bottlenecks
        print(f"\n🔥 TOP PERFORMANCE BOTTLENECKS:")
        print("-" * 50)
        bottlenecks = self.find_bottlenecks(5)
        for i, op in enumerate(bottlenecks, 1):
            print(f"{i}. {op['node_name']} (Node {op['node_id']})")
            print(f"   Time: {op['max_time_seconds']:.1f}s ({op['metric_type']})")
            print(f"   Rows: {op['output_rows']:,}")
            print()
        
        # Slowest individual tasks
        print(f"\n🐌 SLOWEST INDIVIDUAL TASKS:")
        print("-" * 50)
        slowest_tasks = self.find_slowest_tasks(5)
        for i, task in enumerate(slowest_tasks, 1):
            print(f"{i}. Stage {task['stage_id']}, Task {task['task_id']}")
            print(f"   Operation: {task['node_name']} - {task['metric_name']}")
            print(f"   Time: {task['max_time']:.1f}s")
            print()
        
        # Task-level skew analysis
        print(f"\n⚖️  TASK-LEVEL SKEW ANALYSIS:")
        print("-" * 50)
        task_skew = self.analyze_task_level_skew()
        for skew in task_skew[:3]:  # Top 3 most skewed
            print(f"• {skew['node_name']} - {skew['metric_name']}")
            print(f"  Skew Ratio: {skew['skew_ratio']:.1f}x")
            print(f"  Times: {skew['min_time']:.1f}s / {skew['med_time']:.1f}s / {skew['max_time']:.1f}s (min/med/max)")
            if skew['slowest_stage']:
                print(f"  Slowest: Stage {skew['slowest_stage']}, Task {skew['slowest_task']}")
            print()
        
        # Stage performance summary
        print(f"\n📊 STAGE PERFORMANCE SUMMARY:")
        print("-" * 50)
        stage_stats = self.analyze_stage_performance()
        # Sort stages by total time
        sorted_stages = sorted(stage_stats.items(), key=lambda x: x[1]['total_time'], reverse=True)
        for stage_id, stats in sorted_stages[:5]:  # Top 5 stages
            print(f"Stage {stage_id}:")
            print(f"  Total Time: {stats['total_time']:.1f}s")
            print(f"  Max Task Time: {stats['max_task_time']:.1f}s")
            print(f"  Data Processed: {self._format_bytes(stats['data_processed'])}")
            print(f"  Operations: {len(set(op['node_name'] for op in stats['operations']))} distinct")
            print()
        
        # Shuffle analysis
        print(f"\n📊 SHUFFLE OPERATIONS:")
        print("-" * 50)
        shuffles = self.extract_shuffle_metrics()
        for shuffle in shuffles[:3]:  # Top 3
            print(f"• {shuffle['node_name']} (Node {shuffle['node_id']})")
            print(f"  Write Time: {shuffle['shuffle_write_time']:.1f}s")
            print(f"  Data Size: {self._format_bytes(shuffle['data_size'])}")
            print(f"  Fetch Wait: {shuffle['fetch_wait_time']:.1f}s")
            print()
        
        # Join analysis
        print(f"\n🔗 JOIN OPERATIONS:")
        print("-" * 50)
        joins = self.extract_join_metrics()
        for join in joins:
            print(f"• {join['node_name']} (Node {join['node_id']})")
            print(f"  Op Time: {join['op_time']:.1f}s")
            print(f"  Output Rows: {join['output_rows']:,}")
            print()
        
        # Skew indicators
        print(f"\n⚠️  SKEW INDICATORS:")
        print("-" * 50)
        skew_issues = self.analyze_skew_indicators()
        if skew_issues:
            for issue in skew_issues:
                severity_icon = "🔴" if issue['severity'] == 'high' else "🟡"
                print(f"{severity_icon} {issue['issue']} - {issue['node_name']} (Node {issue['node_id']})")
                print(f"   {issue['value']}")
                print()
        else:
            print("No significant skew detected")
    
    def get_dataframe_summary(self):
        """Return key metrics as pandas DataFrames for further analysis"""
        return {
            'shuffles': pd.DataFrame(self.extract_shuffle_metrics()),
            'joins': pd.DataFrame(self.extract_join_metrics()),
            'bottlenecks': pd.DataFrame(self.find_bottlenecks(10)),
            'skew_indicators': pd.DataFrame(self.analyze_skew_indicators()),
            'slowest_tasks': pd.DataFrame(self.find_slowest_tasks(20)),
            'task_skew': pd.DataFrame(self.analyze_task_level_skew()),
            'stage_performance': pd.DataFrame([
                {'stage_id': k, **v} for k, v in self.analyze_stage_performance().items()
            ])
        }
    
    # Helper methods for parsing metrics
    def _parse_time_metric(self, time_str):
        """Parse time strings and return seconds"""
        if not time_str or time_str == '0':
            return 0.0
        
        # Handle formats like "1.5 m", "30 s", "2.3 h"
        if isinstance(time_str, str):
            # Extract total time from complex strings
            if 'total' in time_str:
                # Pattern: "total (min, med, max (stageId: taskId))\n24.8 s"
                total_match = re.search(r'total[^0-9]*([0-9.]+)\s*([smhd])', time_str)
                if total_match:
                    value, unit = total_match.groups()
                    return self._convert_to_seconds(float(value), unit)
                
                # Alternative pattern where total comes first
                alt_match = re.search(r'([0-9.]+)\s*([smhd])[^0-9]*total', time_str)
                if alt_match:
                    value, unit = alt_match.groups()
                    return self._convert_to_seconds(float(value), unit)
            
            # Simple format
            match = re.search(r'([0-9.]+)\s*([smhd])', time_str)
            if match:
                value, unit = match.groups()
                return self._convert_to_seconds(float(value), unit)
        
        return 0.0
    
    def _parse_size_metric(self, size_str):
        """Parse size strings and return bytes"""
        if not size_str or size_str == '0':
            return 0
        
        if isinstance(size_str, str):
            # Handle formats like "1.5 GiB", "500 MiB"
            if 'total' in size_str:
                match = re.search(r'total[^0-9]*([0-9.]+)\s*([KMGT]iB|B)', size_str)
            else:
                match = re.search(r'([0-9.]+)\s*([KMGT]iB|B)', size_str)
            
            if match:
                value, unit = match.groups()
                return self._convert_to_bytes(float(value), unit)
        
        return 0
    
    def _parse_numeric_metric(self, num_str):
        """Parse numeric strings with commas"""
        if not num_str:
            return 0
        
        # Remove commas and convert to int
        clean_str = str(num_str).replace(',', '')
        try:
            return int(clean_str)
        except:
            return 0
    
    def _convert_to_seconds(self, value, unit):
        """Convert time to seconds"""
        multipliers = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
        return value * multipliers.get(unit, 1)
    
    def _convert_to_bytes(self, value, unit):
        """Convert size to bytes"""
        multipliers = {
            'B': 1,
            'KiB': 1024,
            'MiB': 1024**2,
            'GiB': 1024**3,
            'TiB': 1024**4
        }
        return int(value * multipliers.get(unit, 1))
    
    def _format_bytes(self, bytes_val):
        """Format bytes for display"""
        for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
            if bytes_val < 1024:
                return f"{bytes_val:.1f} {unit}"
            bytes_val /= 1024
        return f"{bytes_val:.1f} PiB"
    
    def _extract_max_from_time_range(self, time_str):
        """Extract max time from range strings"""
        match = re.search(r'max[^0-9]*([0-9.]+)\s*([smh])', time_str)
        if match:
            value, unit = match.groups()
            return self._convert_to_seconds(float(value), unit)
        return 0
    
    def _extract_med_from_time_range(self, time_str):
        """Extract median time from range strings"""
        match = re.search(r'med[^0-9]*([0-9.]+)\s*([smh])', time_str)
        if match:
            value, unit = match.groups()
            return self._convert_to_seconds(float(value), unit)
        return 0
    
    def _extract_stage_task_info(self, metric_str):
        """Extract stage and task information from metric strings"""
        # Pattern: (stage 8.0: task 1294)
        stage_task_match = re.search(r'\(stage (\d+)\.(\d+): task (\d+)\)', metric_str)
        if stage_task_match:
            return {
                'stage_id': int(stage_task_match.group(1)),
                'stage_attempt': int(stage_task_match.group(2)), 
                'task_id': int(stage_task_match.group(3))
            }
        return None
    
    def analyze_task_level_skew(self):
        """Analyze task-level performance skew using stage/task IDs"""
        task_analysis = []
        
        for node in self.nodes.values():
            metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
            
            for metric_name, metric_value in metrics.items():
                if 'time' in metric_name.lower() and isinstance(metric_value, str):
                    # Parse min, med, max times and their corresponding stage/task info
                    times_info = self._parse_detailed_time_metric(metric_value)
                    if times_info and times_info['max_time'] > 0:
                        skew_ratio = times_info['max_time'] / times_info['med_time'] if times_info['med_time'] > 0 else 0
                        
                        if skew_ratio > 3:  # Significant skew
                            task_analysis.append({
                                'node_id': node['nodeId'],
                                'node_name': node.get('nodeName', ''),
                                'metric_name': metric_name,
                                'min_time': times_info['min_time'],
                                'med_time': times_info['med_time'],
                                'max_time': times_info['max_time'],
                                'skew_ratio': skew_ratio,
                                'slowest_stage': times_info.get('max_stage_id'),
                                'slowest_task': times_info.get('max_task_id'),
                                'total_time': times_info['total_time']
                            })
        
        return sorted(task_analysis, key=lambda x: x['skew_ratio'], reverse=True)
    
    def _parse_detailed_time_metric(self, time_str):
        """Parse detailed time metrics with stage/task info"""
        if not time_str or 'total' not in time_str:
            return None
        
        result = {'total_time': 0, 'min_time': 0, 'med_time': 0, 'max_time': 0}
        
        # Extract total time
        total_match = re.search(r'total[^0-9]*([0-9.]+)\s*([smhd])', time_str)
        if total_match:
            value, unit = total_match.groups()
            result['total_time'] = self._convert_to_seconds(float(value), unit)
        
        # Extract min, med, max with potential stage/task info
        # Pattern: "min, med, max (stage X.Y: task Z)"
        range_pattern = r'\(([0-9.]+)\s*([smhd])[^,]*,\s*([0-9.]+)\s*([smhd])[^,]*,\s*([0-9.]+)\s*([smhd])[^)]*(?:\(stage\s+(\d+)\.(\d+):\s*task\s+(\d+)\))?'
        range_match = re.search(range_pattern, time_str)
        
        if range_match:
            min_val, min_unit, med_val, med_unit, max_val, max_unit = range_match.groups()[:6]
            stage_id, stage_attempt, task_id = range_match.groups()[6:9]
            
            result['min_time'] = self._convert_to_seconds(float(min_val), min_unit)
            result['med_time'] = self._convert_to_seconds(float(med_val), med_unit)
            result['max_time'] = self._convert_to_seconds(float(max_val), max_unit)
            
            if stage_id:
                result['max_stage_id'] = int(stage_id)
                result['max_task_id'] = int(task_id) if task_id else None
        
    def get_cluster_info(self):
        """Get information about the cluster and execution"""
        cluster_info = {
            'total_nodes_in_plan': len(self.nodes),
            'total_edges': len(self.edges),
            'shuffle_nodes': 0,
            'compute_nodes': 0,
            'scan_nodes': 0,
            'join_nodes': 0,
            'unique_stages': set(),
            'unique_tasks': set(),
            'executor_count': None,
            'partition_count': None
        }
        
        # Analyze node types
        for node in self.nodes.values():
            node_name = node.get('nodeName', '')
            
            if 'Exchange' in node_name or 'Shuffle' in node_name:
                cluster_info['shuffle_nodes'] += 1
            elif 'Scan' in node_name:
                cluster_info['scan_nodes'] += 1
            elif 'Join' in node_name:
                cluster_info['join_nodes'] += 1
            elif any(op in node_name for op in ['Project', 'Filter', 'Aggregate', 'Sort']):
                cluster_info['compute_nodes'] += 1
            
            # Extract stage/task info if available
            metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
            for metric_value in metrics.values():
                if isinstance(metric_value, str) and 'stage' in metric_value:
                    stage_task_info = self._extract_stage_task_info(metric_value)
                    if stage_task_info:
                        cluster_info['unique_stages'].add(stage_task_info['stage_id'])
                        cluster_info['unique_tasks'].add(stage_task_info['task_id'])
        
        # Try to infer executor/partition counts from shuffle operations
        for node in self.nodes.values():
            if 'Exchange' in node.get('nodeName', ''):
                metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
                
                # Look for partition count in shuffle metrics
                partitions = metrics.get('partitions', '0')
                if partitions and partitions != '0':
                    try:
                        cluster_info['partition_count'] = int(partitions)
                        break
                    except:
                        pass
        
        # Convert sets to counts
        cluster_info['unique_stages'] = len(cluster_info['unique_stages'])
        cluster_info['unique_tasks'] = len(cluster_info['unique_tasks'])
        
        return cluster_info
    
    def estimate_parallelism_efficiency(self):
        """Estimate how efficiently parallelism is being used"""
        cluster_info = self.get_cluster_info()
        
        # Look for signs of under/over-parallelization
        parallelism_assessment = {
            'estimated_executors': None,
            'estimated_cores_per_executor': None,
            'task_distribution_efficiency': None,
            'recommendations': []
        }
        
        # Try to estimate executor count from task distribution
        if cluster_info['unique_tasks'] > 0:
            # Very rough estimate: assume tasks are distributed reasonably
            # This is speculative without explicit executor metrics
            estimated_parallelism = min(cluster_info['unique_tasks'], 200)  # Cap at reasonable limit
            parallelism_assessment['estimated_total_cores'] = estimated_parallelism
            
            if estimated_parallelism < 10:
                parallelism_assessment['recommendations'].append("Low parallelism detected - consider increasing executor count")
            elif estimated_parallelism > 1000:
                parallelism_assessment['recommendations'].append("Very high parallelism - check for excessive task overhead")
        
        return parallelism_assessment if result['total_time'] > 0 else None
    
    def find_slowest_tasks(self, top_n=10):
        """Find the slowest individual tasks across all stages"""
        slowest_tasks = []
        
        for node in self.nodes.values():
            metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
            
            for metric_name, metric_value in metrics.items():
                if 'time' in metric_name.lower() and isinstance(metric_value, str):
                    stage_task_info = self._extract_stage_task_info(metric_value)
                    if stage_task_info:
                        # Extract the max time for this metric
                        max_time = self._extract_max_from_time_range(metric_value)
                        if max_time > 0:
                            slowest_tasks.append({
                                'node_id': node['nodeId'],
                                'node_name': node.get('nodeName', ''),
                                'metric_name': metric_name,
                                'stage_id': stage_task_info['stage_id'],
                                'task_id': stage_task_info['task_id'],
                                'max_time': max_time,
                                'stage_task_key': f"stage_{stage_task_info['stage_id']}_task_{stage_task_info['task_id']}"
                            })
        
        return sorted(slowest_tasks, key=lambda x: x['max_time'], reverse=True)[:top_n]
    
    def analyze_stage_performance(self):
        """Analyze performance by stage"""
        stage_stats = defaultdict(lambda: {
            'total_time': 0,
            'task_count': 0,
            'max_task_time': 0,
            'operations': [],
            'data_processed': 0
        })
        
        for node in self.nodes.values():
            metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
            
            for metric_name, metric_value in metrics.items():
                if isinstance(metric_value, str) and 'stage' in metric_value:
                    stage_task_info = self._extract_stage_task_info(metric_value)
                    if stage_task_info:
                        stage_id = stage_task_info['stage_id']
                        
                        # Track operation
                        stage_stats[stage_id]['operations'].append({
                            'node_name': node.get('nodeName', ''),
                            'metric': metric_name
                        })
                        
                        # Track timing
                        if 'time' in metric_name.lower():
                            time_info = self._parse_detailed_time_metric(metric_value)
                            if time_info:
                                stage_stats[stage_id]['total_time'] += time_info['total_time']
                                stage_stats[stage_id]['max_task_time'] = max(
                                    stage_stats[stage_id]['max_task_time'],
                                    time_info['max_time']
                                )
                        
                        # Track data size
                        if 'size' in metric_name.lower() or 'bytes' in metric_name.lower():
                            data_size = self._parse_size_metric(metric_value)
                            stage_stats[stage_id]['data_processed'] += data_size
        
        # Convert defaultdict to regular dict and handle empty case
        result = dict(stage_stats)
        if not result:
            # If no stage info found, create a dummy entry to avoid errors
            result = {0: {
                'total_time': 0,
                'task_count': 0,
                'max_task_time': 0,
                'operations': [],
                'data_processed': 0
            }}
        
        return result
        
    def analyze_system_performance(self):
        """Analyze system performance characteristics and identify hardware vs code bottlenecks"""
        performance_analysis = {
            'shuffle_performance': self._analyze_shuffle_throughput(),
            'compute_vs_io_ratio': self._analyze_compute_io_ratio(),
            'memory_pressure_indicators': self._analyze_memory_pressure(),
            'network_efficiency': self._analyze_network_efficiency(),
            'task_parallelism_efficiency': self._analyze_parallelism_efficiency(),
            'system_bottleneck_assessment': {}
        }
        
        # Determine primary bottleneck type
        performance_analysis['system_bottleneck_assessment'] = self._assess_bottleneck_type(performance_analysis)
        
        return performance_analysis
    
    def _analyze_shuffle_throughput(self):
        """Analyze shuffle read/write throughput to assess I/O performance"""
        shuffle_metrics = []
        
        # Track system-wide aggregates
        total_write_bytes = 0
        total_write_time = 0
        total_read_bytes = 0
        total_read_time = 0
        
        for node in self.nodes.values():
            if 'Exchange' in node.get('nodeName', ''):
                metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
                
                # Debug: print the raw metrics for shuffle nodes
                print(f"\nDEBUG: Node {node['nodeId']} ({node.get('nodeName', '')})")
                for name, value in metrics.items():
                    if 'shuffle' in name.lower() or 'bytes' in name.lower() or 'time' in name.lower():
                        print(f"  {name}: {value}")
                
                # Extract shuffle write performance
                write_bytes = self._parse_size_metric(metrics.get('shuffle bytes written', '0'))
                write_time = self._parse_time_metric(metrics.get('shuffle write time', '0'))
                
                # Also try RAPIDS shuffle write time as alternative
                rapids_write_time = self._parse_time_metric(metrics.get('RAPIDS shuffle shuffle write time', '0'))
                if rapids_write_time > 0 and write_time == 0:
                    write_time = rapids_write_time
                
                # Extract shuffle read performance
                read_bytes = self._parse_size_metric(metrics.get('remote bytes read', '0')) + \
                           self._parse_size_metric(metrics.get('local bytes read', '0'))
                read_time = self._parse_time_metric(metrics.get('RAPIDS shuffle shuffle read time', '0'))
                
                # Aggregate for system-wide calculation
                total_write_bytes += write_bytes
                total_write_time = max(total_write_time, write_time)  # Use max time (bottleneck)
                total_read_bytes += read_bytes
                total_read_time = max(total_read_time, read_time)  # Use max time (bottleneck)
                
                # Debug output
                print(f"  PARSED - Write: {self._format_bytes(write_bytes)} in {write_time:.1f}s")
                print(f"  PARSED - Read: {self._format_bytes(read_bytes)} in {read_time:.1f}s")
                
                # Per-node throughput
                if write_time > 0:
                    write_throughput = write_bytes / write_time / (1024**3)  # GiB/s
                else:
                    write_throughput = 0
                
                if read_time > 0:
                    read_throughput = read_bytes / read_time / (1024**3)  # GiB/s
                else:
                    read_throughput = 0
                
                print(f"  CALCULATED - Write throughput: {write_throughput:.3f} GiB/s")
                print(f"  CALCULATED - Read throughput: {read_throughput:.3f} GiB/s")
                
                shuffle_metrics.append({
                    'node_id': node['nodeId'],
                    'node_name': node.get('nodeName', ''),
                    'write_throughput_gib_s': write_throughput,
                    'read_throughput_gib_s': read_throughput,
                    'write_bytes': write_bytes,
                    'read_bytes': read_bytes,
                    'write_time': write_time,
                    'read_time': read_time,
                    'fetch_wait_time': self._parse_time_metric(metrics.get('fetch wait time', '0'))
                })
        
        # Calculate system-wide throughput
        system_write_throughput = total_write_bytes / total_write_time / (1024**3) if total_write_time > 0 else 0
        system_read_throughput = total_read_bytes / total_read_time / (1024**3) if total_read_time > 0 else 0
        
        print(f"\nSYSTEM-WIDE CALCULATION:")
        print(f"Total write: {self._format_bytes(total_write_bytes)} in {total_write_time:.1f}s = {system_write_throughput:.3f} GiB/s")
        print(f"Total read: {self._format_bytes(total_read_bytes)} in {total_read_time:.1f}s = {system_read_throughput:.3f} GiB/s")
        
        if shuffle_metrics:
            # Per-node statistics
            write_throughputs = [m['write_throughput_gib_s'] for m in shuffle_metrics if m['write_throughput_gib_s'] > 0]
            read_throughputs = [m['read_throughput_gib_s'] for m in shuffle_metrics if m['read_throughput_gib_s'] > 0]
            
            return {
                'details': shuffle_metrics,
                'per_node_stats': {
                    'write_throughput_stats': {
                        'min': min(write_throughputs) if write_throughputs else 0,
                        'max': max(write_throughputs) if write_throughputs else 0,
                        'avg': sum(write_throughputs) / len(write_throughputs) if write_throughputs else 0
                    },
                    'read_throughput_stats': {
                        'min': min(read_throughputs) if read_throughputs else 0,
                        'max': max(read_throughputs) if read_throughputs else 0,
                        'avg': sum(read_throughputs) / len(read_throughputs) if read_throughputs else 0
                    }
                },
                'system_wide_stats': {
                    'write_throughput_gib_s': system_write_throughput,
                    'read_throughput_gib_s': system_read_throughput,
                    'total_write_bytes': total_write_bytes,
                    'total_read_bytes': total_read_bytes,
                    'bottleneck_write_time': total_write_time,
                    'bottleneck_read_time': total_read_time
                }
            }
        return {}
    
    def _analyze_compute_io_ratio(self):
        """Analyze the ratio of compute time vs I/O time to identify bottlenecks"""
        total_compute_time = 0
        total_io_time = 0
        
        for node in self.nodes.values():
            metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
            node_name = node.get('nodeName', '')
            
            # Categorize operations
            if any(op in node_name for op in ['Project', 'Filter', 'Join', 'Aggregate', 'Sort']):
                # Compute operations
                op_time = self._parse_time_metric(metrics.get('op time', '0'))
                duration = self._parse_time_metric(metrics.get('duration', '0'))
                total_compute_time += max(op_time, duration)
            
            elif any(op in node_name for op in ['Exchange', 'Scan', 'Broadcast']):
                # I/O operations
                shuffle_write = self._parse_time_metric(metrics.get('shuffle write time', '0'))
                shuffle_read = self._parse_time_metric(metrics.get('RAPIDS shuffle shuffle read time', '0'))
                scan_time = self._parse_time_metric(metrics.get('scan time', '0'))
                total_io_time += shuffle_write + shuffle_read + scan_time
        
        total_time = total_compute_time + total_io_time
        
        return {
            'total_compute_time': total_compute_time,
            'total_io_time': total_io_time,
            'compute_percentage': (total_compute_time / total_time * 100) if total_time > 0 else 0,
            'io_percentage': (total_io_time / total_time * 100) if total_time > 0 else 0,
            'compute_io_ratio': (total_compute_time / total_io_time) if total_io_time > 0 else float('inf')
        }
    
    def _analyze_memory_pressure(self):
        """Look for indicators of memory pressure"""
        memory_indicators = []
        
        for node in self.nodes.values():
            metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
            
            # Look for spill indicators
            spill_indicators = ['spill', 'disk', 'remote bytes read to disk']
            for metric_name, metric_value in metrics.items():
                if any(indicator in metric_name.lower() for indicator in spill_indicators):
                    if isinstance(metric_value, str) and '0.0 B' not in metric_value and '0' != metric_value:
                        memory_indicators.append({
                            'node_id': node['nodeId'],
                            'node_name': node.get('nodeName', ''),
                            'metric': metric_name,
                            'value': metric_value
                        })
        
        return memory_indicators
    
    def _analyze_network_efficiency(self):
        """Analyze network efficiency based on remote vs local data access"""
        network_stats = {
            'total_remote_bytes': 0,
            'total_local_bytes': 0,
            'total_fetch_wait_time': 0,
            'operations_with_remote_reads': 0
        }
        
        for node in self.nodes.values():
            metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
            
            remote_bytes = self._parse_size_metric(metrics.get('remote bytes read', '0'))
            local_bytes = self._parse_size_metric(metrics.get('local bytes read', '0'))
            fetch_wait = self._parse_time_metric(metrics.get('fetch wait time', '0'))
            
            network_stats['total_remote_bytes'] += remote_bytes
            network_stats['total_local_bytes'] += local_bytes
            network_stats['total_fetch_wait_time'] += fetch_wait
            
            if remote_bytes > 0:
                network_stats['operations_with_remote_reads'] += 1
        
        total_bytes = network_stats['total_remote_bytes'] + network_stats['total_local_bytes']
        
        network_stats['remote_read_percentage'] = (
            network_stats['total_remote_bytes'] / total_bytes * 100 
            if total_bytes > 0 else 0
        )
        
        # Calculate effective network throughput
        if network_stats['total_fetch_wait_time'] > 0:
            network_stats['effective_network_throughput_gib_s'] = (
                network_stats['total_remote_bytes'] / network_stats['total_fetch_wait_time'] / (1024**3)
            )
        else:
            network_stats['effective_network_throughput_gib_s'] = 0
        
        return network_stats
    
    def _analyze_parallelism_efficiency(self):
        """Analyze how efficiently parallelism is being utilized"""
        task_efficiency = []
        
        for node in self.nodes.values():
            metrics = {metric['name']: metric['value'] for metric in node.get('metrics', [])}
            
            for metric_name, metric_value in metrics.items():
                if 'time' in metric_name.lower() and isinstance(metric_value, str):
                    time_info = self._parse_detailed_time_metric(metric_value)
                    if time_info and time_info['med_time'] > 0:
                        efficiency_ratio = time_info['med_time'] / time_info['max_time'] if time_info['max_time'] > 0 else 1
                        task_efficiency.append({
                            'node_id': node['nodeId'],
                            'node_name': node.get('nodeName', ''),
                            'metric': metric_name,
                            'efficiency_ratio': efficiency_ratio,
                            'skew_factor': time_info['max_time'] / time_info['med_time'] if time_info['med_time'] > 0 else 1
                        })
        
        if task_efficiency:
            avg_efficiency = sum(t['efficiency_ratio'] for t in task_efficiency) / len(task_efficiency)
            worst_skew = max(t['skew_factor'] for t in task_efficiency)
        else:
            avg_efficiency = 1.0
            worst_skew = 1.0
        
        return {
            'average_task_efficiency': avg_efficiency,
            'worst_skew_factor': worst_skew,
            'details': sorted(task_efficiency, key=lambda x: x['efficiency_ratio'])[:5]  # 5 worst
        }
    
    def _assess_bottleneck_type(self, performance_data):
        """Determine if bottlenecks are primarily system or code related"""
        assessment = {
            'primary_bottleneck': 'unknown',
            'confidence': 'low',
            'recommendations': [],
            'evidence': []
        }
        
        shuffle_perf = performance_data.get('shuffle_performance', {})
        compute_io = performance_data.get('compute_vs_io_ratio', {})
        network = performance_data.get('network_efficiency', {})
        parallelism = performance_data.get('task_parallelism_efficiency', {})
        
        evidence_points = 0
        
        # Analyze shuffle throughput
        if shuffle_perf:
            write_stats = shuffle_perf.get('write_throughput_stats', {})
            avg_write_throughput = write_stats.get('avg', 0)
            
            if avg_write_throughput < 0.5:  # Less than 0.5 GiB/s
                assessment['evidence'].append(f"Low shuffle write throughput: {avg_write_throughput:.2f} GiB/s")
                evidence_points += 2  # Strong indicator of system bottleneck
            elif avg_write_throughput > 2.0:  # More than 2 GiB/s
                assessment['evidence'].append(f"Good shuffle throughput: {avg_write_throughput:.2f} GiB/s")
                evidence_points -= 1  # System performing well
        
        # Analyze compute vs I/O ratio
        if compute_io.get('io_percentage', 0) > 70:
            assessment['evidence'].append(f"I/O dominated: {compute_io['io_percentage']:.1f}% I/O vs {compute_io['compute_percentage']:.1f}% compute")
            evidence_points += 2  # System bottleneck
        elif compute_io.get('compute_percentage', 0) > 70:
            assessment['evidence'].append(f"Compute dominated: {compute_io['compute_percentage']:.1f}% compute vs {compute_io['io_percentage']:.1f}% I/O")
            evidence_points -= 2  # Likely code inefficiency
        
        # Analyze parallelism efficiency
        avg_efficiency = parallelism.get('average_task_efficiency', 1.0)
        worst_skew = parallelism.get('worst_skew_factor', 1.0)
        
        if worst_skew > 10:
            assessment['evidence'].append(f"Severe task skew: {worst_skew:.1f}x difference between fastest and slowest tasks")
            evidence_points -= 1  # Likely code/data distribution issue
        
        if avg_efficiency < 0.3:
            assessment['evidence'].append(f"Poor parallelism efficiency: {avg_efficiency:.2f}")
            evidence_points -= 1  # Code issue
        
        # Analyze network efficiency
        remote_pct = network.get('remote_read_percentage', 0)
        if remote_pct > 80:
            assessment['evidence'].append(f"High remote reads: {remote_pct:.1f}% of data read over network")
            evidence_points += 1  # System/cluster configuration issue
        
        # Make final assessment
        if evidence_points >= 3:
            assessment['primary_bottleneck'] = 'system_hardware'
            assessment['confidence'] = 'high'
            assessment['recommendations'].extend([
                "Consider upgrading storage/network infrastructure",
                "Optimize cluster configuration",
                "Check for resource contention"
            ])
        elif evidence_points <= -3:
            assessment['primary_bottleneck'] = 'code_efficiency'
            assessment['confidence'] = 'high'
            assessment['recommendations'].extend([
                "Optimize query plan and data access patterns",
                "Implement salting for skewed data",
                "Consider data partitioning strategies"
            ])
        elif evidence_points > 0:
            assessment['primary_bottleneck'] = 'system_hardware'
            assessment['confidence'] = 'medium'
        else:
            assessment['primary_bottleneck'] = 'code_efficiency'
            assessment['confidence'] = 'medium'
        
        return assessment

# Usage functions
def analyze_execution_from_dict(execution_dict):
    """
    Analyze execution from a dictionary (e.g., from API response)
    
    Args:
        execution_dict: Dictionary containing execution plan data
    """
    try:
        analyzer = SparkExecutionAnalyzer(execution_dict)
        analyzer.generate_summary_report()
        return analyzer
    except Exception as e:
        print(f"Error during analysis: {e}")
        print("Attempting basic analysis...")
        
        # Create analyzer but skip the full report
        analyzer = SparkExecutionAnalyzer(execution_dict)
        
        # Try basic info
        try:
            print(f"Execution ID: {execution_dict.get('id', 'N/A')}")
            print(f"Status: {execution_dict.get('status', 'N/A')}")
            print(f"Duration: {execution_dict.get('duration', 0) / 1000:.1f} seconds")
            print(f"Number of nodes: {len(execution_dict.get('nodes', []))}")
        except:
            print("Could not extract basic execution info")
        
        return analyzer

def analyze_execution_from_json_file(json_file_path):
    """
    Analyze execution from a JSON file
    
    Args:
        json_file_path: Path to JSON file containing execution data
    """
    with open(json_file_path, 'r') as f:
        execution_data = json.load(f)
    
    return analyze_execution_from_dict(execution_data)

def compare_executions(exec1_dict, exec2_dict, names=["Execution 1", "Execution 2"]):
    """
    Compare two executions side by side
    
    Args:
        exec1_dict, exec2_dict: Execution dictionaries to compare
        names: Names for the executions
    """
    print("=" * 100)
    print("EXECUTION COMPARISON")
    print("=" * 100)
    
    analyzers = [SparkExecutionAnalyzer(exec1_dict), SparkExecutionAnalyzer(exec2_dict)]
    
    # Basic comparison
    for i, (analyzer, name) in enumerate(zip(analyzers, names)):
        duration = analyzer.data.get('duration', 0) / 1000
        status = analyzer.data.get('status', 'N/A')
        print(f"\n{name}:")
        print(f"  Duration: {duration:.1f}s")
        print(f"  Status: {status}")
        
        # Top bottleneck
        bottlenecks = analyzer.find_bottlenecks(1)
        if bottlenecks:
            top = bottlenecks[0]
            print(f"  Top Bottleneck: {top['node_name']} ({top['max_time_seconds']:.1f}s)")
        
        # Shuffle summary
        shuffles = analyzer.extract_shuffle_metrics()
        total_shuffle_time = sum(s['shuffle_write_time'] for s in shuffles)
        total_shuffle_data = sum(s['data_size'] for s in shuffles)
        print(f"  Total Shuffle Time: {total_shuffle_time:.1f}s")
        print(f"  Total Shuffle Data: {analyzer._format_bytes(total_shuffle_data)}")

# Example usage:
"""
# Analyze system performance
execution_data = {...}  # Your JSON data
analyzer = analyze_execution_from_dict(execution_data)

# Get detailed system performance analysis
perf_report = analyzer.generate_system_performance_report()

# Get specific metrics
shuffle_throughput = analyzer.analyze_system_performance()['shuffle_performance']
print(f"Avg shuffle write speed: {shuffle_throughput['write_throughput_stats']['avg']:.2f} GiB/s")

# Check if bottleneck is system or code
assessment = analyzer.analyze_system_performance()['system_bottleneck_assessment']
print(f"Primary bottleneck: {assessment['primary_bottleneck']}")

# Compare two executions for system efficiency
# compare_executions(exec1_data, exec2_data, ["Before Optimization", "After Optimization"])
"""


ui_url = "http://your-url-here"
execution_id = 0
print(analyze_last_execution(ui_url, execution_id=execution_id))

extractor = SparkUIMetricsExtractor(ui_url)
sql_dag = extractor.get_sql_execution_details(execution_id)
analyzer = analyze_execution_from_dict(sql_dag)

perf_analysis = analyzer.analyze_system_performance()
shuffle_perf = perf_analysis['shuffle_performance']

# System-wide performance (this is what you want for overall cluster speed)
system_stats = shuffle_perf['system_wide_stats']
print(f"System-wide shuffle write speed: {system_stats['write_throughput_gib_s']:.2f} GiB/s")
print(f"System-wide shuffle read speed: {system_stats['read_throughput_gib_s']:.2f} GiB/s")
print(f"Total data written: {analyzer._format_bytes(system_stats['total_write_bytes'])}")
print(f"Total data read: {analyzer._format_bytes(system_stats['total_read_bytes'])}")

# Per-node performance (for identifying stragglers)
per_node = shuffle_perf['per_node_stats']
write_stats = per_node['write_throughput_stats']
print(f"\nPer-node write speeds:")
print(f"  Min: {write_stats['min']:.2f} GiB/s")
print(f"  Avg: {write_stats['avg']:.2f} GiB/s") 
print(f"  Max: {write_stats['max']:.2f} GiB/s")
