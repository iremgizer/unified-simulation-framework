"""
Core metrics module for the blockchain protocol simulation.

This module provides enhanced metrics collection and reporting functionality for the simulation
with time-series tracking, 95% coverage monitoring, and kick-off formula congestion calculation.
"""

import logging
import time
from typing import Dict, List, Set, Any, Optional, Tuple
import json
import os
import numpy as np
from collections import defaultdict

# FIX: Import ConnectionBandwidthTracker (correct path based on project structure)
from src.simulation.bandwidth_tracker import ConnectionBandwidthTracker

logger = logging.getLogger("Metrics")

class Metrics:
    """Enhanced metrics collection and reporting for the simulation with time-series tracking."""
    
    def __init__(self, node_count: int = 0):
        """
        Initialize enhanced metrics collection.
        
        Args:
            node_count: Number of nodes in the network
        """
        # ========================================
        # EXISTING METRICS (keep as-is)
        # ========================================
        
        # Block propagation metrics
        self.block_propagation_times: Dict[int, Dict[int, float]] = {}  # block_id -> {node_id -> time}
        self.block_completion_times: Dict[int, Dict[int, float]] = {}   # block_id -> {node_id -> time}
        self.block_start_times: Dict[int, float] = {}                  # block_id -> start_time
        self.block_processing_times: Dict[int, Dict[int, float]] = {}  # block_id -> {node_id -> time}
        
        # Chunk metrics
        self.chunks_sent: Dict[int, Dict[int, Set[int]]] = {}          # block_id -> {node_id -> {chunk_ids}}
        self.chunks_received: Dict[int, Dict[int, Set[int]]] = {}      # block_id -> {node_id -> {chunk_ids}}
        self.chunk_propagation_times: Dict[Tuple[int, int], Dict[int, float]] = {}  # (block_id, chunk_id) -> {node_id -> time}
        
        # Bandwidth metrics
        self.bandwidth_usage: Dict[int, Dict[int, int]] = {}           # node_id -> {target_id -> bytes}
        self.total_bandwidth: int = 0
        
        # Bandwidth utilization tracking - FIX: Now properly imported
        self.bandwidth_tracker = ConnectionBandwidthTracker(default_bandwidth=12500000)
        
        # Congestion metrics
        self.congestion_time_series: Dict[Tuple[int, int], List[Tuple[float, float]]] = defaultdict(list)
        
        # Computational delay metrics
        self.computational_delays: Dict[int, List[Tuple[int, float]]] = defaultdict(list)
        
        # Protocol-specific metrics
        self.protocol_metrics: Dict[str, List[Any]] = defaultdict(list)
        
        # Simulation metadata
        self.start_time: float = time.time()
        self.end_time: Optional[float] = None
        self.node_count: int = node_count
        self.protocol_name: str = ""
        self.config: Dict[str, Any] = {}
        
        # Coverage time series
        self.coverage_time_series: Dict[int, List[Tuple[float, float]]] = defaultdict(list)
        
        # ========================================
        # NEW ENHANCED TIME-SERIES TRACKING
        # ========================================
        
        # 1. 95% Coverage Tracking (KICK-OFF REQUIREMENT)
        self.ninety_five_percent_times: Dict[int, float] = {}  # block_id -> timestamp
        self.coverage_milestones: Dict[int, Dict[int, float]] = defaultdict(dict)  # block_id -> {50%: time, 75%: time, 95%: time, 99%: time}
        
        # 2. Latency Time-Series (for graphing)
        self.latency_time_series: Dict[int, List[Tuple[float, float]]] = defaultdict(list)  # block_id -> [(time, avg_latency_so_far), ...]
        
        # 3. Real-time Congestion (KICK-OFF FORMULA)
        self.real_time_congestion: Dict[Tuple[int, int], List[Tuple[float, float]]] = defaultdict(list)  # (node1, node2) -> [(time, congestion_%), ...]
        self.peak_congestion: float = 0.0
        self.average_congestion_over_time: List[Tuple[float, float]] = []  # [(time, system_avg_congestion), ...]
        
        # 4. Additional Time-Series for Graphing
        self.node_completion_timeline: Dict[int, List[Tuple[float, int]]] = defaultdict(list)  # block_id -> [(time, cumulative_nodes_completed), ...]
        self.bandwidth_utilization_timeline: List[Tuple[float, float]] = []  # [(time, system_bandwidth_util%), ...]

        self.early_terminations = {} 


        
        logger.info("Enhanced Metrics collection initialized with time-series tracking")
    
    def set_protocol_name(self, name: str):
        """
        Set the protocol name for this metrics instance.
        
        Args:
            name: Name of the protocol
        """
        self.protocol_name = name
    
    def set_config(self, config: Dict[str, Any]):
        """
        Set the configuration for this metrics instance.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
    
    def get_peak_utilization(self) -> float:
        """
        Get peak bandwidth utilization.
        
        Returns:
            Peak utilization percentage
        """
        return self.bandwidth_tracker.get_peak_utilization()
    
    def get_high_utilization_node_count(self, threshold: float = 80.0) -> int:
        """
        Get count of nodes with high utilization.
        
        Args:
            threshold: Utilization threshold percentage
            
        Returns:
            Number of nodes with utilization above threshold
        """
        return self.bandwidth_tracker.get_high_utilization_node_count(threshold)
    
    def get_connection_utilization_distribution(self) -> List[Dict[str, Any]]:
        """
        Get distribution of connection utilization.
        
        Returns:
            List of utilization ranges with connection counts
        """
        return self.bandwidth_tracker.get_connection_utilization_distribution()
    
    def get_utilization_time_series(self) -> List[Dict[str, Any]]:
        """
        Get time series data for bandwidth utilization.
        
        Returns:
            List of time points with utilization data
        """
        return self.bandwidth_tracker.get_utilization_time_series()
    
    def report_block_start(self, block_id: int, source_node_id: int, current_time: float = None):
        """
        Report the start of a block broadcast.
        
        Args:
            block_id: ID of the block
            source_node_id: ID of the source node
            current_time: Current simulation time (if None, uses time.time())
        """
        if current_time is None:
            current_time = time.time()
            
        self.block_start_times[block_id] = current_time
        
        # Initialize propagation time for source node (0 seconds)
        if block_id not in self.block_propagation_times:
            self.block_propagation_times[block_id] = {}
            
        self.block_propagation_times[block_id][source_node_id] = 0.0
        
        # Initialize coverage time series
        initial_coverage = 1.0 / self.node_count * 100 if self.node_count > 0 else 0.0
        self.coverage_time_series[block_id].append((0.0, initial_coverage))
        
        # ✅ NEW: Initialize milestone tracking
        self.coverage_milestones[block_id] = {}
        
        # ✅ NEW: Initialize node completion timeline
        self.node_completion_timeline[block_id].append((current_time, 1))  # Source node completed
        
        logger.debug(f"Started tracking block {block_id} from node {source_node_id} at {current_time}")
    
    def report_chunk_received(self, node_id: int, block_id: int, chunk_id: int, current_time: float):
        """
        Enhanced chunk received reporting with milestone tracking.
        
        Args:
            node_id: ID of the receiving node
            block_id: ID of the block
            chunk_id: ID of the chunk
            current_time: Current simulation time
        """
        # Record chunk propagation time
        if (block_id, chunk_id) not in self.chunk_propagation_times:
            self.chunk_propagation_times[(block_id, chunk_id)] = {}
            
        if node_id not in self.chunk_propagation_times[(block_id, chunk_id)]:
            self.chunk_propagation_times[(block_id, chunk_id)][node_id] = current_time
            
        # Record first chunk reception time for propagation metrics
        if block_id in self.block_start_times and node_id not in self.block_propagation_times.get(block_id, {}):
            propagation_time = current_time - self.block_start_times[block_id]
            
            if block_id not in self.block_propagation_times:
                self.block_propagation_times[block_id] = {}
                
            self.block_propagation_times[block_id][node_id] = propagation_time
            
            # ✅ ENHANCED: Coverage calculation with milestone tracking
            nodes_reached = len(self.block_propagation_times[block_id])
            coverage = (nodes_reached / self.node_count) * 100 if self.node_count > 0 else 0.0
            
            # Update coverage time series
            self.coverage_time_series[block_id].append((current_time, coverage))
            
            # ✅ NEW: Track coverage milestones
            self._track_coverage_milestones(block_id, coverage, current_time)
            
            # ✅ NEW: Update latency time-series
            self._update_latency_time_series(block_id, current_time)
            
            # ✅ NEW: Update node completion timeline
            self.node_completion_timeline[block_id].append((current_time, nodes_reached))
            
            logger.debug(f"Node {node_id} received first chunk of block {block_id} in {propagation_time:.6f}s (Coverage: {coverage:.1f}%)")
    
    def _track_coverage_milestones(self, block_id: int, coverage: float, current_time: float):
        """Track important coverage milestones for analysis."""
        milestones = [50, 75, 90, 95, 99]  # Coverage percentages to track
        
        if block_id not in self.coverage_milestones:
            self.coverage_milestones[block_id] = {}
        
        for milestone in milestones:
            if coverage >= milestone and milestone not in self.coverage_milestones[block_id]:
                self.coverage_milestones[block_id][milestone] = current_time
                
                # ✅ KICK-OFF REQUIREMENT: 95% coverage time
                if milestone == 95:
                    self.ninety_five_percent_times[block_id] = current_time
                    logger.info(f"🎯 Block {block_id} reached 95% coverage at {current_time:.3f}s")
    
    def _update_latency_time_series(self, block_id: int, current_time: float):
        """Update latency time-series for real-time graphing."""
        if block_id in self.block_propagation_times:
            propagation_times = list(self.block_propagation_times[block_id].values())
            if propagation_times:
                avg_latency = sum(propagation_times) / len(propagation_times)
                self.latency_time_series[block_id].append((current_time, avg_latency))
    
    def report_block_processed(self, node_id: int, block_id: int, current_time: float):
        """
        Report processing of a block.
        
        Args:
            node_id: ID of the processing node
            block_id: ID of the block
            current_time: Current simulation time
        """
        # Record block processing time
        if block_id not in self.block_processing_times:
            self.block_processing_times[block_id] = {}
            
        # Calculate processing time (from first chunk received to processing complete)
        if block_id in self.block_propagation_times and node_id in self.block_propagation_times[block_id]:
            start_time = self.block_start_times.get(block_id, current_time)
            first_chunk_time = start_time + self.block_propagation_times[block_id][node_id]
            processing_time = current_time - first_chunk_time
            
            self.block_processing_times[block_id][node_id] = processing_time
            
            # Record computational delay
            self.computational_delays[node_id].append((block_id, processing_time))
            
            logger.debug(f"Node {node_id} processed block {block_id} in {processing_time:.6f}s")
    
    def report_message_sent(self, source_id: int, target_id: int, message_type: str, message_size: int, current_time: float):
        """
        Report a message being sent between nodes.
        
        Args:
            source_id: ID of the source node
            target_id: ID of the target node
            message_type: Type of message being sent
            message_size: Size of the message in bytes
            current_time: Current simulation time
            
        Returns:
            Current utilization percentage for this connection
        """
        # Track message count by type (for protocol-specific metrics)
        message_key = f"message_{message_type.lower()}"
        self.add_protocol_metric(message_key, {"source": source_id, "target": target_id, "time": current_time})
        
        # Report bandwidth usage
        return self.report_bandwidth_usage(source_id, target_id, message_size, current_time)
    
    def report_bandwidth_usage(self, source_id: int, target_id: int, bytes_sent: int, current_time: float):
        """
        Enhanced bandwidth reporting with kick-off formula congestion.
        
        Args:
            source_id: ID of the source node
            target_id: ID of the target node
            bytes_sent: Number of bytes sent
            current_time: Current simulation time
            
        Returns:
            Current utilization percentage for this connection
        """
        # Update bandwidth tracker
        utilization = self.bandwidth_tracker.report_message(source_id, target_id, bytes_sent, current_time)
        
        # Update total bandwidth usage
        self.total_bandwidth += bytes_sent
        
        # Update node-specific bandwidth usage
        if source_id not in self.bandwidth_usage:
            self.bandwidth_usage[source_id] = {}
        self.bandwidth_usage[source_id][target_id] = self.bandwidth_usage[source_id].get(target_id, 0) + bytes_sent
        
        # ✅ NEW: Calculate real-time congestion using KICK-OFF FORMULA
        self._calculate_real_time_congestion(source_id, target_id, bytes_sent, current_time)
        
        # ✅ NEW: Update system-wide metrics timeline
        self._update_system_timeline(current_time)
        
        return utilization
    
    def _calculate_real_time_congestion(self, source_id: int, target_id: int, bytes_sent: int, current_time: float):
        """Calculate congestion using kick-off formula: Traffic Load / Available Bandwidth × 100."""
        
        # Ensure consistent node ordering
        if source_id > target_id:
            source_id, target_id = target_id, source_id
        
        connection = (source_id, target_id)
        
        # Get current traffic load and available bandwidth for this connection
        try:
            current_usage = self.bandwidth_tracker.get_current_usage_for_connection(source_id, target_id)
            available_bandwidth = self.bandwidth_tracker.get_available_bandwidth_for_connection(source_id, target_id)
        except AttributeError:
            # Fallback if methods don't exist in bandwidth_tracker
            current_usage = bytes_sent
            available_bandwidth = 12500000  # Default 100 Mbps
        
        if available_bandwidth > 0:
            # ✅ KICK-OFF FORMULA: Traffic Load / Available Bandwidth × 100
            congestion_percentage = (current_usage / available_bandwidth) * 100
            
            # Cap at 100% for realistic representation
            congestion_percentage = min(congestion_percentage, 100.0)
            
            # Update time series
            self.real_time_congestion[connection].append((current_time, congestion_percentage))
            
            # Update peak congestion
            self.peak_congestion = max(self.peak_congestion, congestion_percentage)
            
            #logger.debug(f"Connection {source_id}-{target_id}: {congestion_percentage:.1f}% congestion "
             #           f"(Load: {current_usage}, Available: {available_bandwidth})")
    
    def _update_system_timeline(self, current_time: float):
        """Update system-wide timeline metrics."""
        
        # System-wide bandwidth utilization
        system_utilization = self.bandwidth_tracker.get_system_average_utilization()
        self.bandwidth_utilization_timeline.append((current_time, system_utilization))
        
        # System-wide average congestion
        if self.real_time_congestion:
            recent_congestions = []
            for connection_series in self.real_time_congestion.values():
                if connection_series:
                    # Get most recent congestion value for each connection
                    recent_congestions.append(connection_series[-1][1])
            
            if recent_congestions:
                avg_system_congestion = sum(recent_congestions) / len(recent_congestions)
                self.average_congestion_over_time.append((current_time, avg_system_congestion))
    
    def report_congestion(self, node1: int, node2: int, congestion: float, current_time: float):
        """
        Report congestion for a connection.
        
        Args:
            node1: ID of the first node
            node2: ID of the second node
            congestion: Congestion factor
            current_time: Current simulation time
        """
        # Ensure node order is consistent
        if node1 > node2:
            node1, node2 = node2, node1
            
        self.congestion_time_series[(node1, node2)].append((current_time, congestion))
    
    def add_protocol_metric(self, metric_name: str, value: Any):
        """
        Add a protocol-specific metric.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
        """
        self.protocol_metrics[metric_name].append(value)
    
    def get_coverage(self, block_id: int) -> float:
        """
        Get coverage percentage for a block.
        
        Args:
            block_id: ID of the block
            
        Returns:
            Coverage percentage (0-100)
        """
        if block_id not in self.block_propagation_times:
            return 0.0
            
        nodes_reached = len(self.block_propagation_times[block_id])
        return (nodes_reached / self.node_count) * 100 if self.node_count > 0 else 0.0
    
    def get_average_coverage(self) -> float:
        """
        Get average coverage percentage across all blocks.
        
        Returns:
            Average coverage percentage (0-100)
        """
        if not self.block_propagation_times:
            return 0.0
            
        total_coverage = sum(self.get_coverage(block_id) for block_id in self.block_propagation_times)
        return total_coverage / len(self.block_propagation_times)
    
    def get_latency(self, block_id: int) -> float:
        """
        Get average propagation latency for a block.
        
        Args:
            block_id: ID of the block
            
        Returns:
            Average latency in seconds
        """
        if block_id not in self.block_propagation_times:
            return 0.0
            
        propagation_times = list(self.block_propagation_times[block_id].values())
        if not propagation_times:
            return 0.0
            
        return sum(propagation_times) / len(propagation_times)
    
    def get_average_latency(self) -> float:
        """
        Get average propagation latency across all blocks.
        
        Returns:
            Average latency in seconds
        """
        if not self.block_propagation_times:
            return 0.0
            
        total_latency = sum(self.get_latency(block_id) for block_id in self.block_propagation_times)
        return total_latency / len(self.block_propagation_times)
    
    def get_bandwidth_utilization(self) -> Dict[Tuple[int, int], float]:
        """
        Get average bandwidth utilization for each connection.
        
        Returns:
            Map of (node1, node2) to average utilization percentage
        """
        result = {}
        for connection in self.bandwidth_tracker.get_all_connections():
            utilization = self.bandwidth_tracker.get_utilization(connection[0], connection[1])
            result[connection] = utilization
                
        return result
    
    def get_average_bandwidth_utilization(self) -> float:
        """
        Get average bandwidth utilization across all connections.
        
        Returns:
            Average utilization percentage (0-100)
        """
        return self.bandwidth_tracker.get_system_average_utilization()
    
    def get_congestion(self) -> Dict[Tuple[int, int], float]:
        """
        Get average congestion for each connection.
        
        Returns:
            Map of (node1, node2) to average congestion factor
        """
        result = {}
        for connection, congestion_data in self.congestion_time_series.items():
            if congestion_data:
                avg_congestion = sum(c for _, c in congestion_data) / len(congestion_data)
                result[connection] = avg_congestion
                
        return result
    
    def get_average_congestion(self) -> float:
        """
        Get average congestion across all connections.
        
        Returns:
            Average congestion factor
        """
        congestion_data = self.get_congestion()
        if not congestion_data:
            return 1.0  # Default congestion factor
            
        return sum(congestion_data.values()) / len(congestion_data)
    
    def get_computational_delay(self) -> Dict[int, float]:
        """
        Get average computational delay for each node.
        
        Returns:
            Map of node_id to average delay in seconds
        """
        result = {}
        for node_id, delay_data in self.computational_delays.items():
            if delay_data:
                avg_delay = sum(d for _, d in delay_data) / len(delay_data)
                result[node_id] = avg_delay
                
        return result
    
    def get_average_computational_delay(self) -> float:
        """
        Get average computational delay across all nodes.
        
        Returns:
            Average delay in seconds
        """
        delay_data = self.get_computational_delay()
        if not delay_data:
            return 0.0
            
        return sum(delay_data.values()) / len(delay_data)
    
    def get_coverage_time_series(self) -> Dict[int, List[Tuple[float, float]]]:
        """
        Get coverage time series for each block.
        
        Returns:
            Map of block_id to list of (time, coverage) tuples
        """
        return dict(self.coverage_time_series)
    
    def get_congestion_time_series(self) -> Dict[Tuple[int, int], List[Tuple[float, float]]]:
        """
        Get congestion time series for each connection.
        
        Returns:
            Map of (node1, node2) to list of (time, congestion) tuples
        """
        return dict(self.congestion_time_series)
    
    # ========================================
    # NEW ENHANCED GETTER METHODS
    # ========================================
    
    def get_95_percent_coverage_time(self, block_id: int) -> Optional[float]:
        """Get time when block reached 95% coverage (KICK-OFF REQUIREMENT)."""
        return self.ninety_five_percent_times.get(block_id)
    
    def get_average_95_percent_coverage_time(self) -> float:
        """Get average time to reach 95% coverage across all blocks."""
        times = [t for t in self.ninety_five_percent_times.values()]
        return sum(times) / len(times) if times else 0.0
    
    def get_coverage_milestones(self, block_id: int) -> Dict[int, float]:
        """Get all coverage milestones for a block."""
        return self.coverage_milestones.get(block_id, {})
    
    def get_latency_time_series_data(self, block_id: int) -> List[Tuple[float, float]]:
        """Get latency progression over time for a block."""
        return self.latency_time_series.get(block_id, [])
    
    def get_real_time_congestion_data(self) -> Dict[Tuple[int, int], List[Tuple[float, float]]]:
        """Get real-time congestion data for all connections."""
        return dict(self.real_time_congestion)
    
    def get_peak_congestion(self) -> float:
        """Get peak congestion percentage across all connections."""
        return self.peak_congestion
    
    def get_average_congestion_kick_off_formula(self) -> float:
        """Get average congestion using kick-off formula."""
        if not self.average_congestion_over_time:
            return 0.0
        
        congestion_values = [cong for _, cong in self.average_congestion_over_time]
        return sum(congestion_values) / len(congestion_values)
    
    def get_system_timeline_data(self) -> Dict[str, List[Tuple[float, float]]]:
        """Get all system timeline data for graphing."""
        return {
            "bandwidth_utilization": self.bandwidth_utilization_timeline,
            "average_congestion": self.average_congestion_over_time,
            "coverage_by_block": dict(self.coverage_time_series),
            "latency_by_block": dict(self.latency_time_series),
            "node_completion_by_block": dict(self.node_completion_timeline)
        }
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of all metrics.
        
        Returns:
            Dictionary of metric summaries
        """
        return {
            "coverage": self.get_average_coverage(),
            "latency": self.get_average_latency(),
            "bandwidth_utilization": self.get_average_bandwidth_utilization(),
            "congestion": self.get_average_congestion(),
            "computational_delay": self.get_average_computational_delay(),
            "block_count": len(self.block_propagation_times),
            "node_count": self.node_count
        }
    
    def get_enhanced_summary(self) -> Dict[str, Any]:
        """Get enhanced summary including kick-off requirements."""
        basic_summary = self.get_summary()
        
        enhanced_summary = {
            **basic_summary,
            
            # ✅ KICK-OFF REQUIREMENTS
            "coverage_95_percent": {
                "average_time": self.get_average_95_percent_coverage_time(),
                "per_block": {str(bid): time for bid, time in self.ninety_five_percent_times.items()}
            },
            
            "congestion_kick_off_formula": {
                "peak_percentage": self.peak_congestion,
                "average_percentage": self.get_average_congestion_kick_off_formula(),
                "formula": "Traffic Load / Available Bandwidth × 100"
            },
            
            # ✅ TIME-SERIES DATA FOR GRAPHING
            "time_series_data": self.get_system_timeline_data(),
            
            # ✅ COVERAGE MILESTONES
            "coverage_milestones": {
                str(bid): milestones for bid, milestones in self.coverage_milestones.items()
            }
        }
        
        return enhanced_summary
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get all metrics in a structured format.
        
        Returns:
            Dictionary containing all metrics
        """
        # Calculate end time if not already set
        if self.end_time is None:
            self.end_time = time.time()
            
        # Calculate simulation duration
        duration = self.end_time - self.start_time
        
        # Build enhanced metrics dictionary
        metrics = {
            "metadata": {
                "protocol": self.protocol_name,
                "node_count": self.node_count,
                "start_time": self.start_time,
                "end_time": self.end_time,
                "duration": duration,
                "config": self.config
            },
            "summary": self.get_enhanced_summary(),  # ✅ NEW: Use enhanced summary
            "coverage": {
                "average": self.get_average_coverage(),
                "per_block": {str(block_id): self.get_coverage(block_id) 
                             for block_id in self.block_propagation_times},
                "time_series": {str(block_id): [(t, c) for t, c in series]
                               for block_id, series in self.coverage_time_series.items()},
                # ✅ NEW: 95% coverage tracking
                "ninety_five_percent_times": {str(bid): time for bid, time in self.ninety_five_percent_times.items()},
                "milestones": {str(bid): milestones for bid, milestones in self.coverage_milestones.items()}
            },
            "latency": {
                "average": self.get_average_latency(),
                "per_block": {str(block_id): self.get_latency(block_id)
                             for block_id in self.block_propagation_times},
                # ✅ NEW: Latency time series
                "time_series": {str(block_id): [(t, l) for t, l in series]
                               for block_id, series in self.latency_time_series.items()}
            },
            "bandwidth": {
                "total_bytes": self.total_bandwidth,
                "average_utilization": self.get_average_bandwidth_utilization(),
                "peak_utilization": self.get_peak_utilization(),
                "high_utilization_nodes": self.get_high_utilization_node_count(),
                "utilization_distribution": self.get_connection_utilization_distribution(),
                "utilization_time_series": self.get_utilization_time_series(),
                # ✅ NEW: System timeline
                "system_timeline": self.bandwidth_utilization_timeline
            },
            "congestion": {
                "average": self.get_average_congestion(),
                "per_connection": {f"{node1}_{node2}": cong 
                                  for (node1, node2), cong in self.get_congestion().items()},
                "time_series": {f"{node1}_{node2}": [(t, c) for t, c in series]
                               for (node1, node2), series in self.congestion_time_series.items()},
                # ✅ NEW: Kick-off formula congestion
                "kick_off_formula": {
                    "peak_percentage": self.peak_congestion,
                    "average_percentage": self.get_average_congestion_kick_off_formula(),
                    "real_time_data": {f"{node1}_{node2}": [(t, c) for t, c in series]
                                      for (node1, node2), series in self.real_time_congestion.items()},
                    "system_timeline": self.average_congestion_over_time
                }
            },
            "computational_delay": {
                "average": self.get_average_computational_delay(),
                "per_node": {str(node_id): delay 
                            for node_id, delay in self.get_computational_delay().items()}
            },
            "protocol_metrics": self.protocol_metrics
        }
        
        return metrics
    
    def _ensure_string_keys(self, obj):
        """
        Recursively ensure all dictionary keys are strings for MongoDB compatibility.
        
        Args:
            obj: Object to process
            
        Returns:
            Object with string keys
        """
        if isinstance(obj, dict):
            return {str(k): self._ensure_string_keys(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._ensure_string_keys(item) for item in obj]
        elif obj is None:
            return None  # Preserve None values
        else:
            return obj
    
    def to_json(self) -> Dict[str, Any]:
        """
        Convert metrics to JSON-serializable format.
        
        Returns:
            JSON-serializable dictionary
        """
        # Ensure all keys are strings for MongoDB compatibility
        return self._ensure_string_keys(self.get_metrics())
    
    def finalize(self, current_time: float):
        """
        Finalize metrics collection at the end of simulation.
        
        Args:
            current_time: Current simulation time
        """
        self.end_time = current_time
        
        # Update bandwidth tracker with final time
        self.bandwidth_tracker._update_utilization(current_time)
        
        logger.info(f"Enhanced metrics finalized at time {current_time}")
    
    def save_results(self, filename: str = None):
        """
        Save metrics results to a JSON file.
        
        Args:
            filename: Name of the file to save to, or None for default
        """
        if filename is None:
            # Create a default filename based on protocol and timestamp
            timestamp = int(time.time())
            filename = f"results_{self.protocol_name}_{self.node_count}nodes_{timestamp}.json"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
        
        # Save enhanced results
        with open(filename, 'w') as f:
            json.dump(self.get_enhanced_summary(), f, indent=2)
        
        logger.info(f"Enhanced results saved to {filename}")
    
    def save_to_mongodb(self, db):
        """
        Save metrics results to MongoDB.
        
        Args:
            db: MongoDB database connection
        """
        try:
            # Convert to JSON-serializable format with string keys
            data = self.to_json()
            
            # Add timestamp
            data["timestamp"] = time.time()
            
            # Insert into database
            result = db.simulation_results.insert_one(data)
            logger.info(f"Enhanced results saved to MongoDB with ID {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            logger.error(f"Failed to store simulation result: {str(e)}")
            return None
    
    def save_to_file(self, filename: str):
        """
        Save metrics to a JSON file.
        
        Args:
            filename: Path to save the metrics
        """
        # Create a serializable representation of metrics
        data = self.to_json()
        
        # Save to file
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
            
        logger.info(f"Enhanced metrics saved to {filename}")

    # ========================================
    # COUGAR PROTOCOL EXTENSIONS
    # ========================================
    
    def report_header_received(self, node_id: int, block_id: int, current_time: float):
        """
        Report receipt of a block header (Cougar-specific).
        
        Args:
            node_id: ID of the receiving node
            block_id: ID of the block
            current_time: Current simulation time
        """
        # Record header propagation time (equivalent to first chunk in Kadcast)
        if block_id in self.block_start_times and node_id not in self.block_propagation_times.get(block_id, {}):
            propagation_time = current_time - self.block_start_times[block_id]
            
            if block_id not in self.block_propagation_times:
                self.block_propagation_times[block_id] = {}
                
            self.block_propagation_times[block_id][node_id] = propagation_time
            
            # Update coverage time series
            nodes_reached = len(self.block_propagation_times[block_id])
            coverage = (nodes_reached / self.node_count) * 100 if self.node_count > 0 else 0.0
            self.coverage_time_series[block_id].append((current_time, coverage))
            
            # ✅ NEW: Track coverage milestones for headers too
            self._track_coverage_milestones(block_id, coverage, current_time)
            
            # Track protocol-specific header metrics
            self.add_protocol_metric("header_received", {
                "node_id": node_id,
                "block_id": block_id,
                "propagation_time": propagation_time,
                "time": current_time
            })
            
            logger.debug(f"Node {node_id} received header of block {block_id} in {propagation_time:.6f}s")

    def report_body_request_sent(self, node_id: int, block_id: int, target_peer_id: int, current_time: float):
        """
        Report sending of a body request (Cougar-specific).
        
        Args:
            node_id: ID of the requesting node
            block_id: ID of the block
            target_peer_id: ID of the peer being requested from
            current_time: Current simulation time
        """
        # Track body request metrics
        self.add_protocol_metric("body_request_sent", {
            "requester_id": node_id,
            "target_peer_id": target_peer_id,
            "block_id": block_id,
            "time": current_time
        })
        
        logger.debug(f"Node {node_id} sent body request for block {block_id} to peer {target_peer_id}")

    def report_body_response_sent(self, node_id: int, block_id: int, requester_id: int, body_size: int, current_time: float):
        """
        Report sending of a body response (Cougar-specific).
        
        Args:
            node_id: ID of the responding node
            block_id: ID of the block
            requester_id: ID of the requesting node
            body_size: Size of the body in bytes
            current_time: Current simulation time
        """
        # Track body response metrics
        self.add_protocol_metric("body_response_sent", {
            "responder_id": node_id,
            "requester_id": requester_id,
            "block_id": block_id,
            "body_size": body_size,
            "time": current_time
        })
        
        logger.debug(f"Node {node_id} sent body response for block {block_id} to node {requester_id} ({body_size} bytes)")

    def report_body_completed(self, node_id: int, block_id: int, current_time: float):
        """
        Report completion of body reception and validation (Cougar-specific).
        
        Args:
            node_id: ID of the node completing the block
            block_id: ID of the block
            current_time: Current simulation time
        """
        # Record block completion time (same as Kadcast block_processed)
        if block_id not in self.block_completion_times:
            self.block_completion_times[block_id] = {}
            
        self.block_completion_times[block_id][node_id] = current_time
        
        # Calculate total completion time (from block start to body validation complete)
        if block_id in self.block_start_times:
            total_completion_time = current_time - self.block_start_times[block_id]
            
            # Track protocol-specific completion metrics
            self.add_protocol_metric("block_completed", {
                "node_id": node_id,
                "block_id": block_id,
                "total_completion_time": total_completion_time,
                "time": current_time
            })
            
            # Record computational delay (body validation time)
            if block_id in self.block_propagation_times and node_id in self.block_propagation_times[block_id]:
                header_received_time = self.block_start_times[block_id] + self.block_propagation_times[block_id][node_id]
                body_processing_time = current_time - header_received_time
                self.computational_delays[node_id].append((block_id, body_processing_time))
            
            logger.debug(f"Node {node_id} completed block {block_id} in {total_completion_time:.6f}s")

    def report_header_validation_delay(self, node_id: int, block_id: int, validation_time: float, current_time: float):
        """
        Report header validation computational delay (Cougar-specific).
        
        Args:
            node_id: ID of the validating node
            block_id: ID of the block
            validation_time: Time spent validating header
            current_time: Current simulation time
        """
        self.add_protocol_metric("header_validation", {
            "node_id": node_id,
            "block_id": block_id,
            "validation_time": validation_time,
            "time": current_time
        })

    def report_body_validation_delay(self, node_id: int, block_id: int, validation_time: float, current_time: float):
        """
        Report body validation computational delay (Cougar-specific).
        
        Args:
            node_id: ID of the validating node
            block_id: ID of the block
            validation_time: Time spent validating body
            current_time: Current simulation time
        """
        self.add_protocol_metric("body_validation", {
            "node_id": node_id,
            "block_id": block_id,
            "validation_time": validation_time,
            "time": current_time
        })

    def report_body_request_timeout(self, node_id: int, block_id: int, peer_id: int, current_time: float):
        """
        Report body request timeout (Cougar-specific).
        
        Args:
            node_id: ID of the requesting node
            block_id: ID of the block
            peer_id: ID of the peer that timed out
            current_time: Current simulation time
        """
        self.add_protocol_metric("body_request_timeout", {
            "node_id": node_id,
            "block_id": block_id,
            "peer_id": peer_id,
            "time": current_time
        })
        
        logger.debug(f"Node {node_id} experienced timeout for block {block_id} from peer {peer_id}")

    def get_cougar_specific_metrics(self) -> Dict[str, Any]:
        """
        Get Cougar-specific metrics summary.
        
        Returns:
            Dictionary of Cougar-specific metrics
        """
        header_metrics = self.protocol_metrics.get("header_received", [])
        body_request_metrics = self.protocol_metrics.get("body_request_sent", [])
        body_response_metrics = self.protocol_metrics.get("body_response_sent", [])
        completion_metrics = self.protocol_metrics.get("block_completed", [])
        timeout_metrics = self.protocol_metrics.get("body_request_timeout", [])
        
        # Calculate Cougar-specific statistics
        if header_metrics:
            header_propagation_times = [m["propagation_time"] for m in header_metrics]
            avg_header_propagation = sum(header_propagation_times) / len(header_propagation_times)
        else:
            avg_header_propagation = 0.0
        
        if completion_metrics:
            total_completion_times = [m["total_completion_time"] for m in completion_metrics]
            avg_total_completion = sum(total_completion_times) / len(total_completion_times)
        else:
            avg_total_completion = 0.0
        
        # Calculate bandwidth efficiency (header vs body ratio)
        total_header_count = len(header_metrics)
        total_body_request_count = len(body_request_metrics)
        total_body_response_count = len(body_response_metrics)
        
        # Calculate timeout rate
        timeout_rate = len(timeout_metrics) / total_body_request_count if total_body_request_count > 0 else 0.0
        
        return {
            "protocol": "cougar",
            "header_propagation": {
                "avg_time": avg_header_propagation,
                "total_headers": total_header_count
            },
            "body_transfer": {
                "requests_sent": total_body_request_count,
                "responses_sent": total_body_response_count,
                "timeout_rate": timeout_rate,
                "success_rate": 1.0 - timeout_rate
            },
            "completion": {
                "avg_total_time": avg_total_completion,
                "completed_blocks": len(completion_metrics)
            },
            "efficiency": {
                "request_response_ratio": total_body_response_count / total_body_request_count if total_body_request_count > 0 else 0.0,
                "header_to_completion_ratio": total_header_count / len(completion_metrics) if completion_metrics else 0.0
            }
        }

    def get_cougar_comparison_metrics(self) -> Dict[str, Any]:
        """
        Get metrics formatted for comparison with Kadcast.
        
        Returns:
            Dictionary of metrics comparable with Kadcast
        """
        base_metrics = self.get_summary()
        cougar_metrics = self.get_cougar_specific_metrics()
        
        return {
            # Standard metrics (comparable with Kadcast)
            "coverage": base_metrics["coverage"],
            "latency": base_metrics["latency"],
            "bandwidth_utilization": base_metrics["bandwidth_utilization"],
            
            # Cougar-specific metrics
            "header_propagation_time": cougar_metrics["header_propagation"]["avg_time"],
            "body_transfer_success_rate": cougar_metrics["body_transfer"]["success_rate"],
            "total_completion_time": cougar_metrics["completion"]["avg_total_time"],
            
            # Protocol comparison metrics
            "message_efficiency": {
                "headers_sent": cougar_metrics["header_propagation"]["total_headers"],
                "body_requests": cougar_metrics["body_transfer"]["requests_sent"],
                "body_responses": cougar_metrics["body_transfer"]["responses_sent"],
            },
            
            # Network efficiency
            "protocol_overhead": {
                "timeout_rate": cougar_metrics["body_transfer"]["timeout_rate"],
                "redundant_requests": 0  # To be calculated based on parallelism
            }
        }
    # ========================================
# MERCURY PROTOCOL EXTENSIONS (ADD TO END OF metrics.py)
# ========================================

    def report_block_completed(self, node_index: int, block_id: int, current_time: float):
        """
        Report completion of block reconstruction (Mercury-specific).
    
        Args:
            node_id: ID of the node completing the block
            block_id: ID of the block
            current_time: Current simulation time
        """
        # Record block completion time (same as Kadcast block_processed)
        if block_id not in self.block_completion_times:
            self.block_completion_times[block_id] = {}
        
        self.block_completion_times[block_id][node_index] = current_time
    
        # Calculate total completion time (from block start to reconstruction complete)
        if block_id in self.block_start_times:
            total_completion_time = current_time - self.block_start_times[block_id]
        
        # Track protocol-specific completion metrics
            self.add_protocol_metric("block_completed", {
                "node_id": node_index,
                "block_id": block_id,
                "total_completion_time": total_completion_time,
                "time": current_time
            })
        
            # Record computational delay (block reconstruction time)
            if block_id in self.block_propagation_times and node_index in self.block_propagation_times[block_id]:
                first_chunk_time = self.block_start_times[block_id] + self.block_propagation_times[block_id][node_index]
                reconstruction_time = current_time - first_chunk_time
                self.computational_delays[node_index].append((block_id, reconstruction_time))
        
            logger.debug(f"Node {node_index} completed Mercury block {block_id} in {total_completion_time:.6f}s")

    def report_vivaldi_update(self, node_index: int, round_number: int, coordinate_change: float, 
                         error_before: float, error_after: float, current_time: float):
        """
         Report Vivaldi coordinate update (Mercury-specific).
    
        Args:
            node_id: ID of the updating node
            round_number: VCS convergence round number
            coordinate_change: Magnitude of coordinate change
            error_before: Error indicator before update
            error_after: Error indicator after update
            current_time: Current simulation time
        """
        self.add_protocol_metric("vivaldi_update", {
            "node_id": node_index,
            "round_number": round_number,
            "coordinate_change": coordinate_change,
            "error_before": error_before,
            "error_after": error_after,
            "time": current_time
        })
        logger.debug(f"Node {node_index} VCS update round {round_number}: error {error_before:.3f} → {error_after:.3f}")

    def report_cluster_assignment(self, node_index: int, cluster_id: int, cluster_size: int, 
                            close_neighbors: int, random_neighbors: int, current_time: float):
        """
        Report cluster assignment (Mercury-specific).
    
        Args:
           node_id: ID of the node being assigned
           cluster_id: Assigned cluster ID
           cluster_size: Size of the assigned cluster
           close_neighbors: Number of close neighbors in cluster
           random_neighbors: Number of random neighbors across clusters
           current_time: Current simulation time
        """
        self.add_protocol_metric("cluster_assignment", {
            "node_id": node_index,
            "cluster_id": cluster_id,
            "cluster_size": cluster_size,
            "close_neighbors": close_neighbors,
            "random_neighbors": random_neighbors,
            "time": current_time
        })
    
        logger.debug(f"Node {node_index} assigned to cluster {cluster_id} (size: {cluster_size}, "
                    f"close: {close_neighbors}, random: {random_neighbors})")

    def report_early_outburst(self, node_index: int, block_id: int, fanout_size: int, 
                         immediate_coverage: float, current_time: float):
        """
        Report early outburst fanout (Mercury-specific).
    
        Args:
            node_id: ID of the source node
            block_id: ID of the block being broadcasted
            fanout_size: Number of peers in early outburst
            immediate_coverage: Percentage of network reached immediately
            current_time: Current simulation time
        """
        self.add_protocol_metric("early_outburst", {
            "node_id": node_index,
            "block_id": block_id,
            "fanout_size": fanout_size,
            "immediate_coverage": immediate_coverage,
            "time": current_time
        })
    
        logger.debug(f"Node {node_index} early outburst for block {block_id}: "
                f"fanout {fanout_size}, coverage {immediate_coverage:.1f}%")

    def report_chunk_request(self, node_index: int, block_id: int, chunk_count: int, 
                            peer_id: int, current_time: float):
        """
        Report chunk request (Mercury-specific).
        
        Args:
            node_id: ID of the requesting node
            block_id: ID of the block
            chunk_count: Number of chunks requested
            peer_id: ID of the peer being requested from
            current_time: Current simulation time
        """
        self.add_protocol_metric("chunk_request", {
            "node_id": node_index,
            "block_id": block_id,
            "chunk_count": chunk_count,
            "peer_id": peer_id,
            "time": current_time
        })

    def report_chunk_response(self, node_index: int, block_id: int, chunk_count: int, 
                            requester_id: int, current_time: float):
        """
        Report chunk response (Mercury-specific).
        
        Args:
            node_id: ID of the responding node
            block_id: ID of the block
            chunk_count: Number of chunks sent
            requester_id: ID of the requesting node
            current_time: Current simulation time
        """
        self.add_protocol_metric("chunk_response", {
            "node_id": node_index,
            "block_id": block_id,
            "chunk_count": chunk_count,
            "requester_id": requester_id,
            "time": current_time
        })

    def get_mercury_specific_metrics(self) -> Dict[str, Any]:
        """
        Get Mercury-specific metrics summary.
        
        Returns:
            Dictionary of Mercury-specific metrics
        """
        vivaldi_metrics = self.protocol_metrics.get("vivaldi_update", [])
        cluster_metrics = self.protocol_metrics.get("cluster_assignment", [])
        outburst_metrics = self.protocol_metrics.get("early_outburst", [])
        completion_metrics = self.protocol_metrics.get("block_completed", [])
        chunk_request_metrics = self.protocol_metrics.get("chunk_request", [])
        chunk_response_metrics = self.protocol_metrics.get("chunk_response", [])
        
        # Calculate VCS convergence statistics
        if vivaldi_metrics:
            error_improvements = []
            coordinate_changes = []
            
            for metric in vivaldi_metrics:
                error_before = metric.get("error_before", 1.0)
                error_after = metric.get("error_after", 1.0)
                error_improvements.append(error_before - error_after)
                coordinate_changes.append(metric.get("coordinate_change", 0.0))
            
            avg_error_improvement = sum(error_improvements) / len(error_improvements)
            avg_coordinate_change = sum(coordinate_changes) / len(coordinate_changes)
            convergence_rate = len([e for e in error_improvements if e > 0]) / len(error_improvements)
        else:
            avg_error_improvement = 0.0
            avg_coordinate_change = 0.0
            convergence_rate = 0.0
        
        # Calculate clustering effectiveness
        if cluster_metrics:
            cluster_sizes = [m["cluster_size"] for m in cluster_metrics]
            close_neighbor_counts = [m["close_neighbors"] for m in cluster_metrics]
            
            avg_cluster_size = sum(cluster_sizes) / len(cluster_sizes)
            avg_close_neighbors = sum(close_neighbor_counts) / len(close_neighbor_counts)
            unique_clusters = len(set(m["cluster_id"] for m in cluster_metrics))
        else:
            avg_cluster_size = 0.0
            avg_close_neighbors = 0.0
            unique_clusters = 0
        
        # Calculate early outburst effectiveness
        if outburst_metrics:
            fanout_sizes = [m["fanout_size"] for m in outburst_metrics]
            immediate_coverages = [m["immediate_coverage"] for m in outburst_metrics]
            
            avg_fanout_size = sum(fanout_sizes) / len(fanout_sizes)
            avg_immediate_coverage = sum(immediate_coverages) / len(immediate_coverages)
            outburst_efficiency = avg_immediate_coverage / avg_fanout_size if avg_fanout_size > 0 else 0.0
        else:
            avg_fanout_size = 0.0
            avg_immediate_coverage = 0.0
            outburst_efficiency = 0.0
        
        # Calculate completion statistics
        if completion_metrics:
            completion_times = [m["total_completion_time"] for m in completion_metrics]
            avg_completion_time = sum(completion_times) / len(completion_times)
        else:
            avg_completion_time = 0.0
        
        return {
            "protocol": "mercury",
            "vcs_convergence": {
                "total_updates": len(vivaldi_metrics),
                "avg_error_improvement": avg_error_improvement,
                "avg_coordinate_change": avg_coordinate_change,
                "convergence_rate": convergence_rate
            },
            "clustering": {
                "unique_clusters": unique_clusters,
                "avg_cluster_size": avg_cluster_size,
                "avg_close_neighbors": avg_close_neighbors,
                "nodes_clustered": len(cluster_metrics)
            },
            "early_outburst": {
                "total_outbursts": len(outburst_metrics),
                "avg_fanout_size": avg_fanout_size,
                "avg_immediate_coverage": avg_immediate_coverage,
                "outburst_efficiency": outburst_efficiency
            },
            "block_transfer": {
                "chunk_requests": len(chunk_request_metrics),
                "chunk_responses": len(chunk_response_metrics),
                "request_response_ratio": len(chunk_response_metrics) / len(chunk_request_metrics) if chunk_request_metrics else 0.0
            },
            "completion": {
                "avg_completion_time": avg_completion_time,
                "completed_blocks": len(completion_metrics)
            }
        }

    def get_mercury_comparison_metrics(self) -> Dict[str, Any]:
        """
        Get metrics formatted for comparison with Kadcast and Cougar.
        
        Returns:
            Dictionary of metrics comparable with other protocols
        """
        base_metrics = self.get_summary()
        mercury_metrics = self.get_mercury_specific_metrics()
        
        return {
            # Standard metrics (comparable with Kadcast and Cougar)
            "coverage": base_metrics["coverage"],
            "latency": base_metrics["latency"],
            "bandwidth_utilization": base_metrics["bandwidth_utilization"],
            "congestion": base_metrics["congestion"],
            
            # Mercury-specific metrics
            "vcs_convergence_rate": mercury_metrics["vcs_convergence"]["convergence_rate"],
            "clustering_efficiency": mercury_metrics["clustering"]["avg_cluster_size"],
            "early_outburst_effectiveness": mercury_metrics["early_outburst"]["outburst_efficiency"],
            "coordinate_stability": 1.0 - mercury_metrics["vcs_convergence"]["avg_coordinate_change"],
            
            # Protocol comparison metrics
            "algorithm_overhead": {
                "vcs_updates": mercury_metrics["vcs_convergence"]["total_updates"],
                "clustering_operations": mercury_metrics["clustering"]["nodes_clustered"],
                "early_outburst_fanout": mercury_metrics["early_outburst"]["avg_fanout_size"]
            },
            
            # Network efficiency (NO FEC advantage)
            "mercury_advantages": {
                "no_fec_overhead": True,
                "coordinate_based_routing": True,
                "early_outburst_coverage": mercury_metrics["early_outburst"]["avg_immediate_coverage"]
            }
        }
    
    def report_early_termination(self, block_id: int, coverage: float, 
                           termination_time: float, events_terminated: int = 0):
        """95% coverage sonrası erken durdurma kaydet"""
        self.early_terminations[block_id] = {
            "time": termination_time,
            "coverage": coverage,
            "events_terminated": events_terminated
        }
        
        logger.info(f"🎯 Early termination recorded for block {block_id}: "
                f"{coverage:.1f}% coverage at {termination_time:.3f}s "
                f"({events_terminated} events terminated)")

    def get_termination_statistics(self) -> Dict[str, Any]:
        """Erken durdurma istatistikleri"""
        if not self.early_terminations:
            return {"enabled": False}
            
        total_terminated_events = sum(
            data["events_terminated"] for data in self.early_terminations.values()
        )
        
        avg_termination_coverage = sum(
            data["coverage"] for data in self.early_terminations.values()
        ) / len(self.early_terminations)
        
        return {
            "enabled": True,
            "blocks_terminated": len(self.early_terminations),
            "total_events_saved": total_terminated_events,
            "average_termination_coverage": avg_termination_coverage,
            "termination_times": [data["time"] for data in self.early_terminations.values()]
        }

