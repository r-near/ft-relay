#!/usr/bin/env python3
import re
import sys
from collections import defaultdict

def parse_metric_line(line):
    """Parse a [METRIC] log line into structured data"""
    match = re.search(r'\[METRIC\] (.+)', line)
    if not match:
        return None
    
    parts = match.group(1).split()
    data = {}
    for part in parts:
        if '=' in part:
            key, value = part.split('=', 1)
            data[key] = value
    return data

def parse_timestamp(line):
    """Extract timestamp from log line"""
    match = re.search(r'\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z', line)
    if match:
        return match.group(1)
    return None

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 parse_metrics.py <log_file>")
        sys.exit(1)
    
    log_file = sys.argv[1]
    
    # Collect metrics by operation and phase
    # Three phases: empty_queue, queue_full, after_http
    metrics_empty = defaultdict(list)
    metrics_full = defaultdict(list)
    metrics_after = defaultdict(list)
    
    current_phase = 'empty'  # Start with empty queue
    http_started = False
    http_ended = False
    
    with open(log_file, 'r') as f:
        for line in f:
            # Detect phase transitions based on markers
            if 'phase=http_load_start' in line:
                http_started = True
            
            if 'phase=http_load_end' in line:
                http_ended = True
                current_phase = 'after'
            
            # Check for queue depth to detect empty -> full transition
            if not http_ended and '[METRIC] op=queue_depth' in line:
                match = re.search(r'depth=(\d+)', line)
                if match:
                    depth = int(match.group(1))
                    if depth > 2000 and current_phase == 'empty':
                        current_phase = 'full'
            
            # Collect metrics (excluding queue_depth itself)
            if '[METRIC]' in line and 'op=queue_depth' not in line:
                data = parse_metric_line(line)
                if data and 'op' in data:
                    op = data['op']
                    if 'duration_ms' in data:
                        try:
                            duration = int(data['duration_ms'])
                            if current_phase == 'empty':
                                metrics_empty[op].append(duration)
                            elif current_phase == 'full':
                                metrics_full[op].append(duration)
                            else:
                                metrics_after[op].append(duration)
                        except ValueError:
                            pass
    
    # Print analysis
    print("=" * 95)
    print("METRICS COMPARISON: THREE PHASES")
    print("=" * 95)
    print()
    
    # Calculate averages for all three phases
    operations = [
        ('loop_total', 'Full worker loop iteration'),
        ('pop_batch', 'Waiting for messages (XREADGROUP)'),
        ('batch_process', 'Total batch processing time'),
        ('build_transfers', 'Fetching states + building'),
        ('fetch_states', 'Fetching transfer states (pipelined)'),
        ('key_lease', 'Acquiring access key lease'),
        ('rpc_broadcast', 'NEAR RPC broadcast'),
        ('redis_updates', 'Post-RPC Redis updates (pipelined)'),
    ]
    
    print(f"{'Operation':<40s} {'Queue Empty':>15s} {'Queue Full':>15s} {'After HTTP':>15s}")
    print("-" * 95)
    
    for op, description in operations:
        empty_vals = metrics_empty.get(op, [])
        full_vals = metrics_full.get(op, [])
        after_vals = metrics_after.get(op, [])
        
        empty_str = f"{sum(empty_vals) / len(empty_vals):.1f}ms" if empty_vals else "N/A"
        full_str = f"{sum(full_vals) / len(full_vals):.1f}ms" if full_vals else "N/A"
        after_str = f"{sum(after_vals) / len(after_vals):.1f}ms" if after_vals else "N/A"
        
        print(f"{description:<40s} {empty_str:>15s} {full_str:>15s} {after_str:>15s}")
    
    print()
    print("Legend:")
    print("  - Queue Empty: Initial phase when transfer queue has <2000 messages")
    print("  - Queue Full: During HTTP load when queue has >2000 messages")  
    print("  - After HTTP: After all HTTP requests complete")
    print()
    
    # Calculate time breakdown for all phases
    print("=" * 95)
    print("TIME BREAKDOWN BY PHASE")
    print("=" * 95)
    print()
    
    for phase_name, phase_metrics in [
        ("QUEUE EMPTY", metrics_empty), 
        ("QUEUE FULL (DURING HTTP)", metrics_full), 
        ("AFTER HTTP", metrics_after)
    ]:
        if 'loop_total' not in phase_metrics:
            continue
            
        print(f"{phase_name}:")
        loop_avg = sum(phase_metrics['loop_total']) / len(phase_metrics['loop_total'])
        components = []
        
        if 'pop_batch' in phase_metrics:
            pop_avg = sum(phase_metrics['pop_batch']) / len(phase_metrics['pop_batch'])
            components.append(('pop_batch (waiting for work)', pop_avg))
        
        if 'batch_process' in phase_metrics:
            process_avg = sum(phase_metrics['batch_process']) / len(phase_metrics['batch_process'])
            components.append(('batch_process (actual work)', process_avg))
            
            # Break down batch_process
            if 'key_lease' in phase_metrics:
                lease_avg = sum(phase_metrics['key_lease']) / len(phase_metrics['key_lease'])
                components.append(('  ↳ key_lease', lease_avg))
            
            if 'rpc_broadcast' in phase_metrics:
                rpc_avg = sum(phase_metrics['rpc_broadcast']) / len(phase_metrics['rpc_broadcast'])
                components.append(('  ↳ rpc_broadcast', rpc_avg))
            
            if 'redis_updates' in phase_metrics:
                redis_avg = sum(phase_metrics['redis_updates']) / len(phase_metrics['redis_updates'])
                components.append(('  ↳ redis_updates', redis_avg))
        
        print(f"  Total loop time: {loop_avg:.1f}ms")
        print()
        for name, avg_time in components:
            pct = (avg_time / loop_avg) * 100
            print(f"    {name:40s} {avg_time:7.1f}ms ({pct:5.1f}%)")
        print()
    
    # Identify what changed between phases
    print("=" * 95)
    print("WHAT CHANGES BETWEEN PHASES?")
    print("=" * 95)
    print()
    
    for op in ['pop_batch', 'key_lease', 'fetch_states', 'build_transfers', 'redis_updates', 'rpc_broadcast']:
        empty_vals = metrics_empty.get(op, [])
        full_vals = metrics_full.get(op, [])
        after_vals = metrics_after.get(op, [])
        
        # Compare queue full vs after HTTP (both have work available)
        if full_vals and after_vals:
            full_avg = sum(full_vals) / len(full_vals)
            after_avg = sum(after_vals) / len(after_vals)
            diff = full_avg - after_avg
            
            if abs(diff) > 5:  # More than 5ms difference
                diff_pct = (diff / after_avg * 100) if after_avg > 0 else 0
                if diff > 0:
                    print(f"⚠️  {op}: {diff:.0f}ms SLOWER when queue full (+{diff_pct:.0f}%)")
                else:
                    print(f"✓  {op}: {-diff:.0f}ms FASTER when queue full ({diff_pct:.0f}%)")
        
        # Compare empty vs full
        if empty_vals and full_vals:
            empty_avg = sum(empty_vals) / len(empty_vals)
            full_avg = sum(full_vals) / len(full_vals)
            diff = full_avg - empty_avg
            
            if abs(diff) > 5:
                diff_pct = (diff / empty_avg * 100) if empty_avg > 0 else 0
                if diff > 0:
                    print(f"📊 {op}: {diff:.0f}ms SLOWER as queue fills (+{diff_pct:.0f}%)")
    
    print()
    print("The operation(s) slower when queue is full are the bottleneck!")

if __name__ == '__main__':
    main()
