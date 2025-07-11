"""
Bandwidth tracking and congestion modeling for the blockchain simulation.

This module provides classes for tracking bandwidth usage and modeling
network congestion based on real data flow rather than synthetic formulas.

🔧 FIXED VERSION: Corrected bandwidth utilization calculations and overflow issues
"""

import time
import logging
from typing import Dict, List, Tuple, Optional, Any
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

class ConnectionBandwidthTracker:
    """
    🔧 FIXED: Tracks bandwidth usage for network connections with proper utilization calculations.
    
    Provides real-time bandwidth utilization data for calculating
    realistic congestion effects without overflow issues.
    """
    
    def __init__(self, default_bandwidth: float = 12500000):
        """
        Initialize the bandwidth tracker.
        
        Args:
            default_bandwidth: Default bandwidth in bytes per second (100 Mbps = 12.5 MB/s)
        """
        self.default_bandwidth = default_bandwidth  # 100 Mbps default
        self.connection_bandwidths = {}  # (node1, node2) -> bandwidth
        self.current_usage = defaultdict(float)  # (node1, node2) -> current usage in bytes/sec
        self.usage_history = defaultdict(list)  # (node1, node2) -> [(time, usage), ...]
        self.message_queue = defaultdict(list)  # (node1, node2) -> [(message_size, arrival_time), ...]
        
        # 🔧 NEW: Time-based usage tracking for accurate calculations
        self.usage_window = 1.0  # 1 second sliding window
        self.recent_transmissions = defaultdict(deque)  # (node1, node2) -> deque of (timestamp, bytes)
        
        # 🔧 NEW: System-wide metrics tracking
        self.total_bytes_transmitted = 0
        self.peak_utilization = 0.0
        self.last_cleanup_time = 0.0
        self.cleanup_interval = 5.0  # Clean up old data every 5 seconds
        
        logger.debug(f"BandwidthTracker initialized with default bandwidth: {default_bandwidth/1000000:.1f} Mbps")
        
    def set_connection_bandwidth(self, node1: int, node2: int, bandwidth: float):
        """
        Set bandwidth for a specific connection.
        
        Args:
            node1: First node ID
            node2: Second node ID
            bandwidth: Bandwidth in bytes per second
        """
        # Validate node IDs
        if node1 is None or node2 is None:
            logger.warning(f"Invalid node IDs for bandwidth tracking: node1={node1}, node2={node2}")
            return
            
        # Ensure node order is consistent
        if node1 > node2:
            node1, node2 = node2, node1
            
        self.connection_bandwidths[(node1, node2)] = max(1.0, bandwidth)  # Minimum 1 byte/sec
        #logger.debug(f"Set bandwidth for connection {node1}-{node2}: {bandwidth/1000000:.1f} Mbps")
        
    def get_connection_bandwidth(self, node1: int, node2: int) -> float:
        """
        Get bandwidth for a specific connection.
        
        Args:
            node1: First node ID
            node2: Second node ID
            
        Returns:
            Bandwidth in bytes per second
        """
        # Ensure node order is consistent
        if node1 > node2:
            node1, node2 = node2, node1
            
        return self.connection_bandwidths.get((node1, node2), self.default_bandwidth)
        
    def report_message(self, src_id: int, dst_id: int, message_size: int, current_time: float) -> float:
        """
        🔧 FIXED: Report a message being sent over a connection with proper utilization calculation.
        
        Args:
            src_id: Source node ID
            dst_id: Destination node ID
            message_size: Size of the message in bytes
            current_time: Current simulation time
            
        Returns:
            Current utilization percentage for this connection
        """
        # Ensure node order is consistent
        if src_id > dst_id:
            src_id, dst_id = dst_id, src_id
            
        connection = (src_id, dst_id)
        
        # Clean up old data periodically
        if current_time - self.last_cleanup_time > self.cleanup_interval:
            self._cleanup_old_data(current_time)
            self.last_cleanup_time = current_time
        
        # Add transmission to recent history
        self.recent_transmissions[connection].append((current_time, message_size))
        
        # Update total bytes counter
        self.total_bytes_transmitted += message_size
        
        # Calculate current usage based on recent transmissions
        self._update_current_usage(connection, current_time)
        
        # Calculate and return current utilization
        utilization = self.get_utilization(src_id, dst_id)
        
        # Update peak utilization
        self.peak_utilization = max(self.peak_utilization, utilization)
        
        # Record usage history
        current_usage = self.current_usage[connection]
        self.usage_history[connection].append((current_time, current_usage))
        
        # Limit history size to prevent memory growth
        if len(self.usage_history[connection]) > 1000:
            self.usage_history[connection] = self.usage_history[connection][-500:]
        
         #logger.debug(f"Reported {message_size} bytes on connection {src_id}-{dst_id}, "
                    # f"utilization: {utilization:.1f}%")
        
        return utilization
        
    def _update_current_usage(self, connection: Tuple[int, int], current_time: float):
        """
        🔧 NEW: Update current usage based on recent transmissions within the time window.
        
        Args:
            connection: (node1, node2) tuple
            current_time: Current simulation time
        """
        transmissions = self.recent_transmissions[connection]
        
        # Remove transmissions outside the window
        window_start = current_time - self.usage_window
        while transmissions and transmissions[0][0] < window_start:
            transmissions.popleft()
        
        # Calculate current usage rate (bytes per second)
        if transmissions:
            total_bytes = sum(size for _, size in transmissions)
            # Usage rate = total bytes in window / window duration
            # For current usage, we use the full window duration to smooth out spikes
            self.current_usage[connection] = total_bytes / self.usage_window
        else:
            self.current_usage[connection] = 0.0
            
    def _cleanup_old_data(self, current_time: float):
        """
        🔧 NEW: Clean up old transmission data to prevent memory growth.
        
        Args:
            current_time: Current simulation time
        """
        cleanup_threshold = current_time - (self.usage_window * 2)  # Keep 2x window for safety
        
        for connection in list(self.recent_transmissions.keys()):
            transmissions = self.recent_transmissions[connection]
            
            # Remove old transmissions
            while transmissions and transmissions[0][0] < cleanup_threshold:
                transmissions.popleft()
            
            # Remove empty deques
            if not transmissions:
                del self.recent_transmissions[connection]
                self.current_usage[connection] = 0.0
        
        logger.debug(f"Cleaned up bandwidth tracking data at time {current_time:.3f}")
        
    def get_current_usage(self, node1: int, node2: int) -> float:
        """
        Get current bandwidth usage for a connection.
        
        Args:
            node1: First node ID
            node2: Second node ID
            
        Returns:
            Current usage in bytes per second
        """
        # Ensure node order is consistent
        if node1 > node2:
            node1, node2 = node2, node1
            
        return self.current_usage.get((node1, node2), 0.0)
        
    def get_utilization(self, node1: int, node2: int) -> float:
        """
        🔧 FIXED: Get current utilization percentage for a connection with proper bounds.
        
        Args:
            node1: First node ID
            node2: Second node ID
            
        Returns:
            Utilization as a percentage (0-100), properly clamped
        """
        # Ensure node order is consistent
        if node1 > node2:
            node1, node2 = node2, node1
            
        connection = (node1, node2)
        bandwidth = self.get_connection_bandwidth(node1, node2)
        usage = self.get_current_usage(node1, node2)
        
        if bandwidth <= 0:
            return 0.0
        
        # Calculate utilization and clamp to [0, 100] range
        utilization = (usage / bandwidth) * 100.0
        utilization = max(0.0, min(100.0, utilization))  # Clamp to [0, 100]
        
        return utilization
        
    def calculate_transmission_delay(self, node1: int, node2: int, message_size: int) -> float:
        """
        🔧 FIXED: Calculate transmission delay based on current bandwidth utilization.
        
        Args:
            node1: First node ID
            node2: Second node ID
            message_size: Size of the message in bytes
            
        Returns:
            Transmission delay in milliseconds
        """
        # Ensure node order is consistent
        if node1 > node2:
            node1, node2 = node2, node1
            
        bandwidth = self.get_connection_bandwidth(node1, node2)
        current_usage = self.get_current_usage(node1, node2)
        
        # Calculate available bandwidth (minimum 10% of total to prevent infinite delays)
        min_available = 0.1 * bandwidth
        available_bandwidth = max(min_available, bandwidth - current_usage)
        
        # Calculate transmission time based on available bandwidth
        transmission_time_seconds = message_size / available_bandwidth
        transmission_time_ms = transmission_time_seconds * 1000.0  # Convert to milliseconds
        
        # Sanity check: limit maximum delay to prevent unrealistic values
        max_delay_ms = 10000.0  # 10 seconds maximum
        transmission_time_ms = min(transmission_time_ms, max_delay_ms)
        
        return transmission_time_ms
        
    def get_usage_history(self, node1: int, node2: int) -> List[Tuple[float, float]]:
        """
        Get usage history for a connection.
        
        Args:
            node1: First node ID
            node2: Second node ID
            
        Returns:
            List of (time, usage) tuples
        """
        # Ensure node order is consistent
        if node1 > node2:
            node1, node2 = node2, node1
            
        return self.usage_history.get((node1, node2), [])
        
    def get_all_connections(self) -> List[Tuple[int, int]]:
        """
        Get all connections being tracked.
        
        Returns:
            List of (node1, node2) tuples
        """
        # Combine connections from bandwidth config and active usage
        all_connections = set(self.connection_bandwidths.keys())
        all_connections.update(self.current_usage.keys())
        return list(all_connections)
    
    def get_system_average_utilization(self) -> float:
        """
        🔧 NEW: Get average bandwidth utilization across all connections.
        
        Returns:
            Average utilization percentage (0-100)
        """
        connections = self.get_all_connections()
        if not connections:
            return 0.0
        
        total_utilization = 0.0
        for node1, node2 in connections:
            total_utilization += self.get_utilization(node1, node2)
        
        return total_utilization / len(connections)
    
    def get_peak_utilization(self) -> float:
        """
        🔧 NEW: Get peak bandwidth utilization observed.
        
        Returns:
            Peak utilization percentage (0-100)
        """
        return min(100.0, self.peak_utilization)  # Ensure it's clamped
    
    def get_high_utilization_node_count(self, threshold: float = 80.0) -> int:
        """
        🔧 NEW: Get count of connections with high utilization.
        
        Args:
            threshold: Utilization threshold percentage
            
        Returns:
            Number of connections with utilization above threshold
        """
        count = 0
        for node1, node2 in self.get_all_connections():
            if self.get_utilization(node1, node2) > threshold:
                count += 1
        return count
    
    def get_connection_utilization_distribution(self) -> List[Dict[str, Any]]:
        """
        🔧 NEW: Get distribution of connection utilization.
        
        Returns:
            List of utilization ranges with connection counts
        """
        ranges = [
            (0, 25, "Low (0-25%)"),
            (25, 50, "Medium (25-50%)"),
            (50, 75, "High (50-75%)"),
            (75, 100, "Very High (75-100%)")
        ]
        
        distribution = []
        for min_util, max_util, label in ranges:
            count = 0
            for node1, node2 in self.get_all_connections():
                util = self.get_utilization(node1, node2)
                if min_util <= util < max_util:
                    count += 1
            
            distribution.append({
                "range": label,
                "min_utilization": min_util,
                "max_utilization": max_util,
                "connection_count": count
            })
        
        return distribution
    
    def get_utilization_time_series(self) -> List[Dict[str, Any]]:
        """
        🔧 NEW: Get time series data for bandwidth utilization.
        
        Returns:
            List of time points with utilization data
        """
        time_series = []
        
        # Aggregate utilization data across all connections at each time point
        all_times = set()
        for history in self.usage_history.values():
            all_times.update(time for time, _ in history)
        
        # Sample every 1 second to avoid too much data
        sorted_times = sorted(all_times)
        if sorted_times:
            start_time = sorted_times[0]
            end_time = sorted_times[-1]
            
            current_time = start_time
            while current_time <= end_time:
                avg_utilization = 0.0
                connection_count = 0
                
                for connection in self.get_all_connections():
                    # Find utilization at this time point
                    history = self.usage_history.get(connection, [])
                    if history:
                        # Find closest time point
                        closest_time = min(history, key=lambda x: abs(x[0] - current_time))
                        if abs(closest_time[0] - current_time) <= 1.0:  # Within 1 second
                            bandwidth = self.get_connection_bandwidth(*connection)
                            if bandwidth > 0:
                                util = min(100.0, (closest_time[1] / bandwidth) * 100.0)
                                avg_utilization += util
                                connection_count += 1
                
                if connection_count > 0:
                    time_series.append({
                        "timestamp": current_time,
                        "average_utilization": avg_utilization / connection_count,
                        "active_connections": connection_count
                    })
                
                current_time += 1.0  # 1 second intervals
        
        return time_series
    
    def get_system_congestion_metrics(self) -> Dict[str, Any]:
        """
        🔧 NEW: Get comprehensive system congestion metrics.
        
        Returns:
            Dictionary with system-wide congestion information
        """
        connections = self.get_all_connections()
        if not connections:
            return {
                "average_utilization": 0.0,
                "peak_utilization": 0.0,
                "high_congestion_connections": 0,
                "total_connections": 0,
                "total_bytes_transmitted": self.total_bytes_transmitted
            }
        
        utilizations = []
        high_congestion_count = 0
        
        for node1, node2 in connections:
            utilization = self.get_utilization(node1, node2)
            utilizations.append(utilization)
            if utilization > 80.0:
                high_congestion_count += 1
        
        return {
            "average_utilization": sum(utilizations) / len(utilizations),
            "peak_utilization": max(utilizations) if utilizations else 0.0,
            "high_congestion_connections": high_congestion_count,
            "total_connections": len(connections),
            "total_bytes_transmitted": self.total_bytes_transmitted,
            "utilization_distribution": self.get_connection_utilization_distribution()
        }
    
    def reset_metrics(self):
        """
        🔧 NEW: Reset all metrics and tracking data.
        """
        self.current_usage.clear()
        self.usage_history.clear()
        self.message_queue.clear()
        self.recent_transmissions.clear()
        self.total_bytes_transmitted = 0
        self.peak_utilization = 0.0
        self.last_cleanup_time = 0.0
        
        logger.info("Bandwidth tracker metrics reset")
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """
        🔧 NEW: Get a comprehensive summary of bandwidth tracking statistics.
        
        Returns:
            Dictionary with summary statistics
        """
        connections = self.get_all_connections()
        
        if not connections:
            return {
                "total_connections": 0,
                "average_utilization": 0.0,
                "peak_utilization": 0.0,
                "total_bytes_transmitted": self.total_bytes_transmitted
            }
        
        utilizations = [self.get_utilization(*conn) for conn in connections]
        
        return {
            "total_connections": len(connections),
            "active_connections": len([u for u in utilizations if u > 0]),
            "average_utilization": sum(utilizations) / len(utilizations),
            "peak_utilization": max(utilizations),
            "min_utilization": min(utilizations),
            "total_bytes_transmitted": self.total_bytes_transmitted,
            "high_utilization_connections": len([u for u in utilizations if u > 80.0]),
            "congestion_summary": self.get_system_congestion_metrics()
        }