"""
Network delay calculation system for the blockchain simulation.

This module provides separated delay calculations for different phases:
1. Network transmission delays (RTT + Bandwidth) - applied during message sending
2. Computational processing delays - applied during message processing
3. Specialized delays for different operations (generation, reconstruction, etc.)
"""

import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from .computational_delay import ComputationalDelayCalculator
from .bandwidth_tracker import ConnectionBandwidthTracker
from src.data.geo_data_provider import GeoDataProvider

logger = logging.getLogger("NetworkDelay")

class NetworkDelayCalculator:
    """
    Network delay calculator with separated delay components.
    
    This calculator provides separate methods for:
    - Network transmission delays (used during message sending)
    - Computational processing delays (used during message processing)
    - Specialized operation delays (generation, reconstruction, etc.)
    """

    def __init__(self, geo_provider: GeoDataProvider, default_bandwidth: float = 12500000, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the network delay calculator.
        
        Args:
            geo_provider: Geographic data provider for RTT calculations
            default_bandwidth: Default bandwidth in bytes/second (100 Mbps = 12.5 MB/s)
            config: Configuration for computational delay calculator
        """
        self.geo_provider = geo_provider
        self.computational_calculator = ComputationalDelayCalculator(config)
        self.bandwidth_tracker = ConnectionBandwidthTracker(default_bandwidth)

        # Cache for RTT values
        self.rtt_cache: Dict[Tuple[str, str], float] = {}

        logger.info("NetworkDelayCalculator initialized with separated delay architecture")

    # ===========================================
    # NETWORK TRANSMISSION DELAYS
    # (Applied during message sending phase)
    # ===========================================
    
    def calculate_transmission_delay(self, source_node: int, target_node: int,
                                   source_city: str, target_city: str,
                                   message_size: int, current_time: float) -> Dict[str, float]:
        """
        Calculate network transmission delay (RTT + Bandwidth).
        
        Used when sending messages between nodes.
        
        Args:
            source_node: Source node ID
            target_node: Target node ID
            source_city: Source city name
            target_city: Target city name
            message_size: Size of the message in bytes
            current_time: Current simulation time
            
        Returns:
            Dictionary with transmission delay breakdown
        """
        rtt_delay = self.calculate_rtt_delay(source_city, target_city)
        if rtt_delay is None:
            rtt_delay = 0.0
            logger.warning(f"RTT calculation failed for {source_city} -> {target_city}, using 0.0")
            
        bandwidth_delay = self.calculate_bandwidth_delay(source_node, target_node, message_size, current_time)
        
        transmission_delay = rtt_delay + bandwidth_delay
        
        result = {
            "rtt_delay": rtt_delay,
            "bandwidth_delay": bandwidth_delay,
            "transmission_delay": transmission_delay
        }
        
        #logger.debug(f"Transmission delay {source_node} -> {target_node}: {transmission_delay:.4f}s "
                    # f"(rtt: {rtt_delay:.4f}s, bw: {bandwidth_delay:.4f}s)")
        return result
    
    def calculate_rtt_delay(self, source_city: str, target_city: str) -> Optional[float]:
        """
        Calculate RTT delay between two cities.
        
        Args:
            source_city: Source city name
            target_city: Target city name
            
        Returns:
            RTT delay in seconds, None if cities not found
        """
        cache_key = (source_city, target_city)
        if cache_key in self.rtt_cache:
            return self.rtt_cache[cache_key]
            
        rtt_ms = self.geo_provider.get_rtt(source_city, target_city)
        if rtt_ms is None:
            logger.error(f"[RTT ERROR] No RTT value found for {source_city} -> {target_city}")
            return None 

        rtt_seconds = rtt_ms / 1000.0
        self.rtt_cache[cache_key] = rtt_seconds
        #logger.debug(f"RTT delay {source_city} -> {target_city}: {rtt_seconds:.4f}s")
        return rtt_seconds
    
    def calculate_bandwidth_delay(self, source_node: int, target_node: int, message_size: int, current_time: float) -> float:
        """
        Calculate bandwidth transmission delay based on current network congestion.
        
        Args:
            source_node: Source node ID
            target_node: Target node ID  
            message_size: Size of the message in bytes
            current_time: Current simulation time
            
        Returns:
            Bandwidth delay in seconds
        """
        utilization = self.bandwidth_tracker.report_message(
            source_node, target_node, message_size, current_time
        )
        delay_ms = self.bandwidth_tracker.calculate_transmission_delay(
            source_node, target_node, message_size
        )
        delay_seconds = delay_ms / 1000.0
        #logger.debug(f"Bandwidth delay {source_node} -> {target_node}: {delay_seconds:.4f}s (utilization: {utilization:.1f}%)")
        return delay_seconds

    # ===========================================
    # COMPUTATIONAL PROCESSING DELAYS
    # (Applied during message processing phase)
    # ===========================================
    
    def calculate_chunk_processing_delay(self, node_id: int, chunk_size: int) -> float:
        """
        Calculate computational delay for processing a received chunk.
        
        Used in receive_chunk() method.
        
        Args:
            node_id: Node ID processing the chunk
            chunk_size: Size of the chunk in bytes
            
        Returns:
            Processing delay in seconds
        """
        delay = self.computational_calculator.calculate_chunk_processing_delay(
            node_id, chunk_size
        )
        #logger.debug(f"Chunk processing delay for node {node_id}: {delay:.6f}s")
        return delay
    
    def calculate_block_processing_delay(self, node_id: int, block_size: int, transactions: List[Dict]) -> float:
        """
        Calculate computational delay for processing/validating a complete block.
        
        Used in _process_complete_block() method.
        
        Args:
            node_id: Node ID processing the block
            block_size: Size of the block in bytes
            transactions: List of transaction dictionaries with 'type' field
            
        Returns:
            Processing delay in seconds
        """
        delay = self.computational_calculator.calculate_block_processing_delay(
            node_id, block_size, transactions
        )
        #logger.debug(f"Block processing delay for node {node_id}: {delay:.4f}s")
        return delay
    
    def calculate_block_reconstruction_delay(self, node_id: int, chunk_count: int, original_size: int, fec_used: bool = True) -> float:
        """
        Calculate computational delay for reconstructing a block from chunks.
        
        Used before block processing in _process_complete_block() method.
        
        Args:
            node_id: Node ID performing reconstruction
            chunk_count: Number of chunks to process
            original_size: Original block size in bytes
            fec_used: Whether FEC decoding is needed
            
        Returns:
            Reconstruction delay in seconds
        """
        delay = self.computational_calculator.calculate_block_reconstruction_delay(
            node_id, chunk_count, original_size, fec_used
        )
        logger.debug(f"Block reconstruction delay for node {node_id}: {delay:.4f}s")
        return delay
    
    def calculate_transaction_validation_delay(self, node_id: int, tx_type, tx_data: Dict) -> float:
        """
        Calculate computational delay for validating a single transaction.
        
        Used for individual transaction processing.
        
        Args:
            node_id: Node ID validating the transaction
            tx_type: Type of transaction (TxType enum)
            tx_data: Transaction data dictionary
            
        Returns:
            Validation delay in seconds
        """
        delay = self.computational_calculator.calculate_transaction_validation_delay(
            node_id, tx_type, tx_data
        )
        logger.debug(f"Transaction validation delay for node {node_id}: {delay:.6f}s")
        return delay

    # ===========================================
    # SPECIALIZED OPERATION DELAYS
    # (Applied during specific blockchain operations)
    # ===========================================
    
    def calculate_block_generation_delay(self, node_id: int, transactions: List[Dict], block_size: int) -> float:
        """
        Calculate computational delay for generating a new block.
        
        Used in _handle_block_generate_event() method.
        
        Args:
            node_id: Node ID generating the block
            transactions: List of transaction dictionaries with 'type' field
            block_size: Size of the final block in bytes
            
        Returns:
            Generation delay in seconds
        """
        delay = self.computational_calculator.calculate_block_generation_delay(
            node_id, transactions, block_size
        )
        logger.debug(f"Block generation delay for node {node_id}: {delay:.4f}s")
        return delay
    
    def calculate_chunkify_delay(self, node_id: int, block_size: int, chunk_size: int, fec_ratio: float) -> float:
        """
        Calculate computational delay for splitting a block into chunks with FEC.
        
        Used during block broadcasting initialization.
        
        Args:
            node_id: Node ID performing chunkification
            block_size: Size of the block in bytes
            chunk_size: Size of individual chunks in bytes
            fec_ratio: FEC overhead ratio (e.g., 0.2 for 20% overhead)
            
        Returns:
            Chunkify delay in seconds
        """
        delay = self.computational_calculator.calculate_chunkify_delay(
            node_id, block_size, chunk_size, fec_ratio
        )
        logger.debug(f"Chunkify delay for node {node_id}: {delay:.4f}s")
        return delay

    # ===========================================
    # UTILITY AND MONITORING METHODS
    # ===========================================
    
    def get_node_performance_summary(self, node_id: int) -> Dict[str, Any]:
        """
        Get performance summary for a specific node.
        
        Args:
            node_id: Node ID
            
        Returns:
            Dictionary with node performance information
        """
        return self.computational_calculator.get_node_performance_summary(node_id)

    def get_bandwidth_utilization(self, source_node: int, target_node: int) -> float:
        """Get current bandwidth utilization for a connection."""
        return self.bandwidth_tracker.get_utilization(source_node, target_node)

    def get_system_congestion_metrics(self) -> Dict[str, Any]:
        """Get system-wide congestion metrics."""
        all_connections = self.bandwidth_tracker.get_all_connections()
        if not all_connections:
            return {
                "average_utilization": 0.0,
                "peak_utilization": 0.0,
                "high_congestion_connections": 0,
                "total_connections": 0
            }
        utilizations = []
        high_congestion_count = 0
        for source_node, target_node in all_connections:
            utilization = self.bandwidth_tracker.get_utilization(source_node, target_node)
            utilizations.append(utilization)
            if utilization > 80.0:
                high_congestion_count += 1
        return {
            "average_utilization": sum(utilizations) / len(utilizations),
            "peak_utilization": max(utilizations),
            "high_congestion_connections": high_congestion_count,
            "total_connections": len(all_connections)
        }

    def set_connection_bandwidth(self, source_node: int, target_node: int, bandwidth: float):
        """Set bandwidth capacity for a specific connection."""
        self.bandwidth_tracker.set_connection_bandwidth(source_node, target_node, bandwidth)

    def get_delay_statistics(self) -> Dict[str, Any]:
        """Get comprehensive delay statistics."""
        congestion_metrics = self.get_system_congestion_metrics()
        return {
            "congestion_metrics": congestion_metrics,
            "rtt_cache_size": len(self.rtt_cache),
            "bandwidth_connections": len(self.bandwidth_tracker.get_all_connections()),
            "delay_architecture": "separated",
            "supported_delays": {
                "transmission_delays": ["rtt", "bandwidth"],
                "processing_delays": ["chunk_processing", "block_processing", "block_reconstruction"],
                "operation_delays": ["block_generation", "chunkify", "transaction_validation"]
            }
        }
    
    def calculate_header_validation_delay(self, node_id: int, header_data: Dict[str, Any]) -> float:
       """
       Calculate computational delay for header validation.
    
       Used in Cougar and other protocols for block header validation.
    
       Args:
         header_data: Block header data dictionary
        
       Returns:
           Header validation delay in seconds
       """
       delay = self.computational_calculator.calculate_header_validation_delay(
           node_id, header_data
       )
       #logger.debug(f"Header validation delay for node {node_id}: {delay:.6f}s")
       return delay