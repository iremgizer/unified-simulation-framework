"""
Mercury Protocol Node Implementation - COMPLETE KADCAST-STYLE

Node responsibilities:
- Handle VCS convergence (DISTRIBUTED)
- Store and serve chunks  
- Handle digest/chunk messages
- Block reconstruction with Mercury dechunking
- Relay to peers
- NO block generation (simulator does this)
"""

import random
import time
import logging
import math
import hashlib
from typing import Dict, List, Set, Tuple, Optional, Any, Union
from collections import defaultdict
import numpy as np

from src.protocols.mercury.messages import (
    MercuryVivaldiUpdateMessage, MercuryVivaldiResponseMessage,
    MercuryBlockDigestMessage, MercuryChunkRequestMessage, 
    MercuryChunkResponseMessage, MercuryBlockCompleteMessage,
    MercuryConfig, MercuryMessageType
)

logger = logging.getLogger("MercuryNode")


class VirtualCoordinate:
    """3D Virtual coordinate with error indicator (Vivaldi algorithm)."""
    
    def __init__(self, dimensions: int = 3):
        self.dimensions = dimensions
        # Initialize near origin with small random variation
        self.coordinates = [random.uniform(-1.0, 1.0) for _ in range(dimensions)]
        self.error_indicator = 1.0  # ei - initial high error
        self.history = []
        self.update_count = 0
        
    def euclidean_distance(self, other: 'VirtualCoordinate') -> float:
        """Calculate Euclidean distance to another coordinate."""
        if len(self.coordinates) != len(other.coordinates):
            raise ValueError("Coordinate dimensions mismatch")
        
        return math.sqrt(sum((a - b) ** 2 for a, b in 
                           zip(self.coordinates, other.coordinates)))
    
    def update_vivaldi(self, other_coord: 'VirtualCoordinate', measured_rtt: float, 
                      cc: float = 0.25, ce: float = 0.25) -> None:
        """
        Update coordinate using Vivaldi algorithm (Mercury Algorithm 1).
        
        Args:
            other_coord: Other node's coordinate
            measured_rtt: Measured RTT to other node  
            cc: Adaptive timestep (default 0.25)
            ce: Error factor (default 0.25)
        """
        # Calculate predicted distance
        predicted_distance = self.euclidean_distance(other_coord)
        
        # Calculate error
        error = abs(predicted_distance - measured_rtt)
        
        # Update error indicator
        self.error_indicator = ce * error + (1 - ce) * self.error_indicator
        
        # Calculate weight
        weight = self.error_indicator / (self.error_indicator + other_coord.error_indicator)
        
        # Calculate timestep
        timestep = cc * weight
        
        # Calculate force direction (unit vector)
        if predicted_distance > 0:
            force_direction = [
                (self.coordinates[i] - other_coord.coordinates[i]) / predicted_distance
                for i in range(self.dimensions)
            ]
        else:
            # Random direction if coordinates are identical
            force_direction = [random.uniform(-1, 1) for _ in range(self.dimensions)]
            magnitude = math.sqrt(sum(x*x for x in force_direction))
            if magnitude > 0:
                force_direction = [x/magnitude for x in force_direction]
        
        # Calculate force magnitude
        force_magnitude = timestep * (measured_rtt - predicted_distance)
        
        # Update coordinates
        for i in range(self.dimensions):
            self.coordinates[i] += force_magnitude * force_direction[i]
        
        self.update_count += 1
        
        # Store in history for debugging
        self.history.append({
            'update': self.update_count,
            'error': error,
            'error_indicator': self.error_indicator,
            'coordinates': self.coordinates.copy()
        })
    
    def apply_newton_rules(self, config: MercuryConfig) -> None:
        """Apply Newton security rules to prevent coordinate manipulation."""
        if not config.enable_newton_rules:
            return
        
        # Limit coordinate magnitude
        max_coord = 1000.0  # Reasonable coordinate space limit
        for i in range(self.dimensions):
            self.coordinates[i] = max(-max_coord, min(max_coord, self.coordinates[i]))
        
        # Apply gravity force towards origin
        if config.gravity_rho > 0:
            distance_from_origin = math.sqrt(sum(x*x for x in self.coordinates))
            if distance_from_origin > 0:
                gravity_force = (distance_from_origin / config.gravity_rho) ** 2
                gravity_direction = [-x / distance_from_origin for x in self.coordinates]
                
                for i in range(self.dimensions):
                    self.coordinates[i] += gravity_force * gravity_direction[i] * 0.01  # Small gravity step
        
        # Limit error indicator
        self.error_indicator = min(self.error_indicator, config.error_limit * 4)


class MercuryNode:
    """
    Mercury protocol node implementation - COMPLETE KADCAST-STYLE.
    
    Features:
    - Distributed VCS convergence (Vivaldi algorithm)
    - Centralized cluster assignment (from simulator)
    - Early outburst strategy for source nodes
    - Complete block chunk handling and reconstruction
    - Mercury-specific chunk handling with proper verification
    - Message handling via simulator.send_message()
    """
    
    def __init__(self, node_index: int, node_id: str = None, city: str = None,
                 coordinates: Tuple[float, float] = None, performance_type: str = "average_performance",
                 config: MercuryConfig = None):
        """
        Initialize Mercury node.
        
        Args:
            node_index: Node index in simulation
            node_id: Unique node identifier
            city: Geographic location
            coordinates: (latitude, longitude)
            performance_type: Node performance category
            config: Mercury protocol configuration
        """
        # Basic node properties
        self.node_index = node_index
        self.node_id = node_id or f"mercury_{node_index}"
        self.city = city or "unknown"
        self.coordinates = coordinates or (0.0, 0.0)
        self.performance_type = performance_type
        
        # Mercury configuration
        self.config = config or MercuryConfig()
        
        # Virtual Coordinate System
        self.virtual_coordinate = VirtualCoordinate(self.config.vcs_dimensions)
        self.peer_coordinates: Dict[str, VirtualCoordinate] = {}
        self.peer_rtts: Dict[str, float] = {}
        self.coordinate_frozen = False
        
        # VCS convergence tracking
        self.vcs_convergence_complete = False
        self.vivaldi_updates_count = 0
        
        # Centralized cluster assignment (from simulator)
        self.my_cluster_id: Optional[int] = None
        self.cluster_peers: List[str] = []  # Same cluster peers
        
        # Network knowledge (centralized approach)
        self.known_peers: Set[str] = set()
        self.simulation = None  # Reference to simulator
        
        # Block broadcasting state - complete chunking support
        self.block_chunks: Dict[int, Dict[int, bytes]] = {}  # block_id -> {chunk_id -> chunk_data}
        self.block_metadata: Dict[int, Dict[str, Any]] = {}  # block_id -> metadata
        self.completed_blocks: Set[int] = set()
        self.block_completion_times: Dict[int, float] = {}
        self.block_original_data: Dict[int, bytes] = {}  # Store original data for verification
        
        # Message handling state
        self.pending_chunk_requests: Dict[int, Set[int]] = {}  # block_id -> {chunk_ids}
        self.chunk_request_timestamps: Dict[int, float] = {}  # block_id -> request_time
        
        # Block generation capabilities for source nodes (simulator fills this)
        self.generated_blocks: Set[int] = set()  # Blocks this node generated
        self.block_generation_times: Dict[int, float] = {}  # block_id -> generation_time

        self._previous_coordinates = [0.0, 0.0, 0.0]
        
        logger.debug(f"Mercury node {node_index} initialized: ID={node_id}, City={city}")
    
    def set_simulation(self, simulation):
        """Set simulation reference for message sending."""
        self.simulation = simulation
    
    def add_all_network_peers(self, peer_ids: List[str]):
        """Add all network peers (centralized approach)."""
        self.known_peers.update(peer_ids)
        logger.debug(f"Node {self.node_index}: Added {len(peer_ids)} network peers")
    
    def set_peer_rtt(self, peer_id: str, rtt: float):
        """Set RTT to a specific peer."""
        self.peer_rtts[peer_id] = rtt
    
    def set_cluster_assignment(self, cluster_id: int, cluster_peer_ids: List[str]):
        """Set cluster assignment from centralized clustering."""
        self.my_cluster_id = cluster_id
        self.cluster_peers = cluster_peer_ids
        
        # ✅ NEW: Report cluster assignment to simulation metrics
        if self.simulation and hasattr(self.simulation, 'metrics_collector'):
            close_neighbors = min(self.config.dcluster, len(cluster_peer_ids))
            random_neighbors = self.config.dmax - close_neighbors
            
            current_time = getattr(self.simulation, 'event_queue', type('obj', (object,), {'current_time': 0.0})).current_time
            
            self.simulation.metrics_collector.report_cluster_assignment(
                self.node_index, cluster_id, len(cluster_peer_ids) + 1,  # +1 for self
                close_neighbors, random_neighbors, current_time
            )
        
        logger.debug(f"Node {self.node_index}: Assigned to cluster {cluster_id} with {len(cluster_peer_ids)} peers")

    # ===============================================================
    # VCS CONVERGENCE - DISTRIBUTED (Each node updates own coordinate)
    # ===============================================================
    
    def vivaldi_convergence_round(self, round_number: int) -> List[MercuryVivaldiUpdateMessage]:
        """
        Perform one round of Vivaldi convergence - DISTRIBUTED.
        
        Args:
            round_number: Current convergence round
            
        Returns:
            List of Vivaldi update messages to send
        """
        if self.coordinate_frozen:
            return []
        
        # Select random peers for RTT measurement (16 peers per round)
        available_peers = [pid for pid in self.known_peers if pid != self.node_id]
        num_measurements = min(self.config.measurements_per_round, len(available_peers))
        
        if num_measurements == 0:
            return []
        
        selected_peers = random.sample(available_peers, num_measurements)
        
        # Create Vivaldi update messages
        update_messages = []
        for peer_id in selected_peers:
            message = MercuryVivaldiUpdateMessage(
                sender_id=self.node_id,
                coordinate=self.virtual_coordinate.coordinates.copy(),
                error_indicator=self.virtual_coordinate.error_indicator,
                query_peers=[peer_id]  # Query this specific peer
            )
            update_messages.append(message)
            
            # Send message via simulation
            self.send_message(peer_id, message)

        if self.simulation and hasattr(self.simulation, 'metrics_collector'):
            # Calculate coordinate change from previous round
            if hasattr(self, '_previous_coordinates'):
                coord_change = sum((a - b) ** 2 for a, b in 
                                zip(self.virtual_coordinate.coordinates, self._previous_coordinates)) ** 0.5
            else:
                coord_change = 0.0
            
            # Store current coordinates for next round
            self._previous_coordinates = self.virtual_coordinate.coordinates.copy()
            
            # Report update
            current_time = self.simulation.event_queue.current_time
            self.simulation.metrics_collector.report_vivaldi_update(
                self.node_index, round_number, coord_change,
                self.virtual_coordinate.error_indicator, self.virtual_coordinate.error_indicator,
                current_time
            )   
            
        # Check convergence
        if (round_number >= self.config.coordinate_rounds - 1 or 
            self.virtual_coordinate.error_indicator < self.config.stability_threshold):
            self.vcs_convergence_complete = True
            self.freeze_coordinates()
        
        #logger.debug(f"Node {self.node_index}: VCS round {round_number}, error={self.virtual_coordinate.error_indicator:.3f}")
        
        return update_messages
    
    def handle_vivaldi_update(self, message: MercuryVivaldiUpdateMessage) -> MercuryVivaldiResponseMessage:
        """
        Handle Vivaldi update message and respond with coordinate and RTT.
        
        Args:
            message: Vivaldi update message
            
        Returns:
            Vivaldi response message
        """
        if self.coordinate_frozen:
            # Still respond but don't update
            measured_rtt = self.peer_rtts.get(message.sender_id, 50.0)  # Default RTT
            
            response = MercuryVivaldiResponseMessage(
                sender_id=self.node_id,
                coordinate=self.virtual_coordinate.coordinates.copy(),
                error_indicator=self.virtual_coordinate.error_indicator,
                measured_rtt=measured_rtt
            )
            
            self.send_message(message.sender_id, response)
            return response
        
        # Get RTT to sender
        measured_rtt = self.peer_rtts.get(message.sender_id, 50.0)  # Default RTT if not available
        
        # Create peer coordinate
        peer_coord = VirtualCoordinate(self.config.vcs_dimensions)
        peer_coord.coordinates = message.coordinate.copy()
        peer_coord.error_indicator = message.error_indicator
        
        # Update our coordinate using Vivaldi algorithm
        self.virtual_coordinate.update_vivaldi(
            peer_coord, 
            measured_rtt,
            self.config.adaptive_timestep,
            self.config.error_limit
        )
        
        # Apply Newton security rules
        self.virtual_coordinate.apply_newton_rules(self.config)

        if self.simulation and hasattr(self.simulation, 'metrics_collector'):
            current_time = getattr(self.simulation, 'event_queue', type('obj', (object,), {'current_time': 0.0})).current_time
            
            # Report the update interaction (could track RTT accuracy, etc.)
            self.simulation.metrics_collector.add_protocol_metric("vivaldi_interaction", {
                "node_id": self.node_index,
                "peer_id": self._get_node_index_from_id(message.sender_id),
                "measured_rtt": measured_rtt,
                "predicted_distance": peer_coord.euclidean_distance(self.virtual_coordinate) if hasattr(self, 'virtual_coordinate') else 0.0,
                "time": current_time
            })


        
        # Store peer coordinate
        self.peer_coordinates[message.sender_id] = peer_coord
        
        # Update metrics
        self.vivaldi_updates_count += 1
        
        # Create response
        response = MercuryVivaldiResponseMessage(
            sender_id=self.node_id,
            coordinate=self.virtual_coordinate.coordinates.copy(),
            error_indicator=self.virtual_coordinate.error_indicator,
            measured_rtt=measured_rtt
        )
        
        self.send_message(message.sender_id, response)
        return response
    
    def handle_vivaldi_response(self, message: MercuryVivaldiResponseMessage):
        """
        Handle Vivaldi response message.
        
        Args:
            message: Vivaldi response message
        """
        if self.coordinate_frozen:
            return
        
        # Create peer coordinate
        peer_coord = VirtualCoordinate(self.config.vcs_dimensions)
        peer_coord.coordinates = message.coordinate.copy()
        peer_coord.error_indicator = message.error_indicator
        
        # Update our coordinate using Vivaldi algorithm
        self.virtual_coordinate.update_vivaldi(
            peer_coord, 
            message.measured_rtt,
            self.config.adaptive_timestep,
            self.config.error_limit
        )
        
        # Apply Newton security rules
        self.virtual_coordinate.apply_newton_rules(self.config)
        
        # Store peer coordinate
        self.peer_coordinates[message.sender_id] = peer_coord
        
        # Update metrics
        self.vivaldi_updates_count += 1
    
    def freeze_coordinates(self) -> None:
        """Freeze coordinates after convergence phase."""
        self.coordinate_frozen = True
        logger.debug(f"Node {self.node_index}: Coordinates frozen at {self.virtual_coordinate.coordinates}")
    
    # ===============================================================
    # PEER SELECTION - Mercury's close + random strategy
    # ===============================================================
    
    def select_relay_peers(self, is_source: bool = False) -> Set[str]:
        """
        Select relay peers using Mercury's close + random strategy.
        
        Args:
            is_source: Whether this node is the source (early outburst)
            
        Returns:
            Set of peer IDs to relay to
        """
        if is_source and self.config.enable_early_outburst:
            # Early outburst: source node sends to many random peers
            available_peers = [p for p in self.known_peers if p != self.node_id]
            if not available_peers:
                return set()
            
            selected_count = min(self.config.source_fanout, len(available_peers))
            selected = random.sample(available_peers, selected_count)
            logger.debug(f"Node {self.node_index}: Early outburst to {len(selected)} peers")
            return set(selected)
        
        selected_peers = set()
        
        # 1. Select close neighbors from same cluster
        if self.cluster_peers:
            close_count = min(self.config.dcluster, len(self.cluster_peers))
            if close_count > 0:
                # Select closest peers in cluster based on coordinate distance
                if self.peer_coordinates:
                    cluster_peers_with_coords = [
                        (pid, self.peer_coordinates.get(pid))
                        for pid in self.cluster_peers
                        if pid in self.peer_coordinates
                    ]
                    
                    if cluster_peers_with_coords:
                        # Sort by coordinate distance
                        cluster_peers_with_coords.sort(
                            key=lambda x: self.virtual_coordinate.euclidean_distance(x[1]) 
                            if x[1] else float('inf')
                        )
                        selected_peers.update(
                            pid for pid, _ in cluster_peers_with_coords[:close_count]
                        )
                
                # Fill remaining with random cluster peers
                remaining_cluster = [p for p in self.cluster_peers if p not in selected_peers]
                if remaining_cluster and len(selected_peers) < close_count:
                    additional_count = min(close_count - len(selected_peers), len(remaining_cluster))
                    additional = random.sample(remaining_cluster, additional_count)
                    selected_peers.update(additional)
        
        # 2. Select random neighbors from entire network
        remaining_random = self.config.dmax - len(selected_peers)
        if remaining_random > 0:
            available_random = [
                p for p in self.known_peers 
                if p != self.node_id and p not in selected_peers
            ]
            if available_random:
                random_count = min(remaining_random, len(available_random))
                random_peers = random.sample(available_random, random_count)
                selected_peers.update(random_peers)
        
        logger.debug(f"Node {self.node_index}: Selected {len(selected_peers)} relay peers "
                    f"(close: {len([p for p in selected_peers if p in self.cluster_peers])}, "
                    f"random: {len([p for p in selected_peers if p not in self.cluster_peers])})")
        
        return selected_peers
    
    # ===============================================================
    # BLOCK HANDLING - Digest, Chunk Request/Response, Reconstruction
    # ===============================================================
    
    def handle_block_digest(self, message: MercuryBlockDigestMessage) -> Optional[MercuryChunkRequestMessage]:
        """
        Handle block digest message and request chunks if needed.
        
        Args:
            message: Block digest message
            
        Returns:
            Chunk request message if chunks are needed
        """
        block_id = message.block_id
        
        # Check if we already have this block
        if block_id in self.completed_blocks:
            #logger.debug(f"Node {self.node_index}: Already have block {block_id}")
            return None
        
        # Check if this is our own block (generated by this node)
        if block_id in self.generated_blocks:
            logger.debug(f"Node {self.node_index}: Ignoring own block {block_id}")
            return None
        
        # Check if we already have this block's chunks
        if block_id in self.block_chunks and len(self.block_chunks[block_id]) == message.total_chunks:
            #logger.debug(f"Node {self.node_index}: Already have all chunks for block {block_id}")
            # Try to complete if we somehow missed it
            self._complete_block(block_id)
            return None
        
        # Store block metadata
        self.block_metadata[block_id] = {
            'hash': message.block_hash,
            'total_chunks': message.total_chunks,
            'block_size': message.block_size,
            'source': message.sender_id
        }
        
        # Determine which chunks we need
        existing_chunks = set(self.block_chunks.get(block_id, {}).keys())
        needed_chunks = set(range(message.total_chunks)) - existing_chunks
        
        if needed_chunks:
            # Create chunk request
            chunk_request = MercuryChunkRequestMessage(
                sender_id=self.node_id,
                block_id=block_id,
                chunk_ids=list(needed_chunks)
            )
            
            # Send chunk request back to sender
            self.send_message(message.sender_id, chunk_request)

            if self.simulation and hasattr(self.simulation, 'metrics_collector'):
                self.simulation.metrics_collector.report_chunk_request(
                    self.node_index, block_id, len(needed_chunks),
                    self._get_node_index_from_id(message.sender_id),
                    self.simulation.event_queue.current_time
                )
            
            # Track pending request
            self.pending_chunk_requests[block_id] = needed_chunks
            self.chunk_request_timestamps[block_id] = self.simulation.event_queue.current_time
            
            #logger.debug(f"Node {self.node_index}: Requested {len(needed_chunks)} chunks for block {block_id}")
            return chunk_request
        
        return None
    
    def handle_chunk_request(self, message: MercuryChunkRequestMessage):
        """
        Handle chunk request message and send requested chunks.
        
        Args:
            message: Chunk request message
        """
        block_id = message.block_id
        
        # Check if we have the requested block
        if block_id not in self.block_chunks:
            logger.warning(f"Node {self.node_index}: Don't have block {block_id} for chunk request")
            return
        
        # Send requested chunks
        available_chunks = self.block_chunks[block_id]
        metadata = self.block_metadata.get(block_id, {})
        total_chunks = metadata.get('total_chunks', 0)
        
        chunks_sent = 0
        for chunk_id in message.chunk_ids:
            if chunk_id in available_chunks:
                chunk_response = MercuryChunkResponseMessage(
                    sender_id=self.node_id,
                    block_id=block_id,
                    chunk_id=chunk_id,
                    chunk_data=available_chunks[chunk_id],
                    total_chunks=total_chunks
                )
                
                self.send_message(message.sender_id, chunk_response)
                chunks_sent += 1
            else:
                logger.warning(f"Node {self.node_index}: Missing chunk {chunk_id} for block {block_id}")
        
        #logger.debug(f"Node {self.node_index}: Sent {chunks_sent}/{len(message.chunk_ids)} chunks for block {block_id}")
        
        if chunks_sent < len(message.chunk_ids):
            logger.warning(f"Node {self.node_index}: Could only send {chunks_sent} out of {len(message.chunk_ids)} requested chunks")
        
        if chunks_sent > 0 and self.simulation and hasattr(self.simulation, 'metrics_collector'):
            self.simulation.metrics_collector.report_chunk_response(
                self.node_index, block_id, chunks_sent,
                self._get_node_index_from_id(message.sender_id),
                self.simulation.event_queue.current_time
            )
        
        #logger.debug(f"Node {self.node_index}: Sent {chunks_sent}/{len(message.chunk_ids)} chunks for block {block_id}")


    
    def handle_chunk_response(self, message: MercuryChunkResponseMessage):
        """
        Handle chunk response message and reconstruct block if complete.
        
        Args:
            message: Chunk response message
        """
        block_id = message.block_id
        chunk_id = message.chunk_id
        
        # Ignore if we already completed this block
        if block_id in self.completed_blocks:
            #logger.debug(f"Node {self.node_index}: Ignoring chunk for already completed block {block_id}")
            return
        
        # Store chunk
        if block_id not in self.block_chunks:
            self.block_chunks[block_id] = {}
        
        self.block_chunks[block_id][chunk_id] = message.chunk_data

        if self.simulation and hasattr(self.simulation, 'metrics_collector'):
            current_time = getattr(self.simulation, 'event_queue', type('obj', (object,), {'current_time': 0.0})).current_time
            
            # Report as first chunk received (for coverage tracking)
            self.simulation.metrics_collector.report_chunk_received(
                self.node_index, block_id, chunk_id, current_time
            )
            
        # Update pending requests
        if block_id in self.pending_chunk_requests:
            self.pending_chunk_requests[block_id].discard(chunk_id)
        
        # Update metadata if not exists
        if block_id not in self.block_metadata:
            self.block_metadata[block_id] = {
                'total_chunks': message.total_chunks,
                'block_size': 0,  # Will be updated after reconstruction
                'source': message.sender_id
            }
        
        # Check if block is complete
        metadata = self.block_metadata.get(block_id, {})
        total_chunks = metadata.get('total_chunks', message.total_chunks)
        
        current_chunks = len(self.block_chunks[block_id])
        #logger.debug(f"Node {self.node_index}: Block {block_id} progress: {current_chunks}/{total_chunks} chunks")
        
        if current_chunks == total_chunks:
            # Block is complete - reconstruct and validate
            logger.info(f"Node {self.node_index}: All chunks received for block {block_id}, starting reconstruction")
            self._complete_block(block_id)
        else:
            # Check if we're still missing chunks that we requested
            if block_id in self.pending_chunk_requests and self.pending_chunk_requests[block_id]:
                missing_chunks = len(self.pending_chunk_requests[block_id])
                #logger.debug(f"Node {self.node_index}: Block {block_id} still missing {missing_chunks} chunks")
    
    def _complete_block(self, block_id: int):
        """
        Complete Mercury block reconstruction with proper verification.
        
        Args:
            block_id: ID of the completed block
        """
        try:
            # Import here to avoid circular imports
            from src.data.realistic_payload import RealisticChunkGenerator
            
            # Get all chunks for this block
            chunk_dict = self.block_chunks.get(block_id, {})
            metadata = self.block_metadata.get(block_id, {})
            
            expected_chunks = metadata.get('total_chunks', 0)
            
            if len(chunk_dict) != expected_chunks:
                logger.warning(f"Node {self.node_index}: Incomplete chunks for block {block_id}: "
                             f"{len(chunk_dict)}/{expected_chunks}")
                return
            
            # Convert chunks to ordered list for Mercury dechunking
            chunks = []
            for i in range(expected_chunks):
                if i not in chunk_dict:
                    logger.error(f"Node {self.node_index}: Missing chunk {i} for block {block_id}")
                    return
                chunks.append(chunk_dict[i])
            
            logger.debug(f"Node {self.node_index}: Starting Mercury dechunking for block {block_id}")
            
            # Mercury-specific dechunking (NO FEC)
            reconstructed_data, verified_block_id = RealisticChunkGenerator.dechunkify_block_mercury(chunks)
            
            # Verify block ID integrity
            if verified_block_id != block_id:
                raise ValueError(f"Block ID mismatch: expected {block_id}, got {verified_block_id}")
            
            # Verify block hash if available
            expected_hash = metadata.get('hash', '')
            if expected_hash:
                actual_hash = hashlib.sha256(reconstructed_data).hexdigest()
                if expected_hash != actual_hash:
                    raise ValueError(f"Block hash mismatch for block {block_id}: "
                                   f"expected {expected_hash}, got {actual_hash}")
            
            # Store reconstructed data
            self.block_original_data[block_id] = reconstructed_data
            
            # Update metadata with actual block size
            metadata.update({
                'block_size': len(reconstructed_data),
                'hash': hashlib.sha256(reconstructed_data).hexdigest()
            })
            
            # Mark block as completed
            self.completed_blocks.add(block_id)
            current_time = self.simulation.event_queue.current_time
            if self.simulation and hasattr(self.simulation, 'event_queue'):
               current_time = self.simulation.event_queue.current_time
            else:
                current_time = time.time()   
        
            self.block_completion_times[block_id] = current_time

            if self.simulation and hasattr(self.simulation, 'metrics_collector'):
                self.simulation.metrics_collector.report_block_completed(
                   self.node_index, block_id, current_time
            )
        


            
            # Calculate completion latency if we have request timestamp
            if block_id in self.chunk_request_timestamps:
                latency = current_time - self.chunk_request_timestamps[block_id]
                logger.debug(f"Node {self.node_index}: Block {block_id} completion latency: {latency:.3f}s")
            
            # Clean up pending requests
            self.pending_chunk_requests.pop(block_id, None)
            self.chunk_request_timestamps.pop(block_id, None)
            
            # Notify simulation of completion
            if self.simulation and hasattr(self.simulation, 'on_block_completed'):
                self.simulation.on_block_completed(self.node_index, block_id, current_time)
            
            # Start relaying block to peers (if not source)
            if block_id not in self.generated_blocks:
                self._relay_block(block_id)
            else:
                logger.debug(f"Node {self.node_index}: Not relaying own block {block_id}")
            
            logger.info(f"✅ Node {self.node_index}: Completed Mercury block {block_id} "
                       f"({len(reconstructed_data)} bytes, {expected_chunks} chunks)")
            
        except Exception as e:
            logger.error(f"❌ Node {self.node_index}: Mercury block {block_id} completion failed: {e}")
            # Don't mark as completed if reconstruction failed
            if block_id in self.completed_blocks:
                self.completed_blocks.remove(block_id)
            # Also clean up any partial state
            self.pending_chunk_requests.pop(block_id, None)
            self.chunk_request_timestamps.pop(block_id, None)
    
    def _relay_block(self, block_id: int):
        """
        Relay completed block to selected peers.
        
        Args:
            block_id: ID of the block to relay
        """
        try:
            # Select relay peers (not as source)
            relay_peers = self.select_relay_peers(is_source=False)

            if self.simulation and hasattr(self.simulation, 'metrics_collector'):
                immediate_coverage = (len(relay_peers) / len(self.known_peers)) * 100 if self.known_peers else 0.0
                current_time = getattr(self.simulation, 'event_queue', type('obj', (object,), {'current_time': 0.0})).current_time
                
                # This is a relay, not an early outburst, but still track it
                self.simulation.metrics_collector.add_protocol_metric("block_relay", {
                    "node_id": self.node_index,
                    "block_id": block_id,
                    "relay_peers": len(relay_peers),
                    "relay_coverage": immediate_coverage,
                    "time": current_time
                })
                
            if not relay_peers:
                logger.debug(f"Node {self.node_index}: No peers to relay block {block_id}")
                return
            
            # Create block digest message
            metadata = self.block_metadata.get(block_id, {})
            if not metadata:
                logger.error(f"Node {self.node_index}: No metadata for block {block_id} to relay")
                return
            
            digest_msg = MercuryBlockDigestMessage(
                sender_id=self.node_id,
                block_id=block_id,
                block_hash=metadata.get('hash', f'hash_{block_id}'),
                total_chunks=metadata.get('total_chunks', 0),
                block_size=metadata.get('block_size', 0)
            )
            
            # Send digest to relay peers
            relayed_count = 0
            for peer_id in relay_peers:
                self.send_message(peer_id, digest_msg)
                relayed_count += 1
            
            logger.debug(f"Node {self.node_index}: Relayed block {block_id} digest to {relayed_count} peers")
            
        except Exception as e:
            logger.error(f"❌ Node {self.node_index}: Error relaying block {block_id}: {e}")
    
    # ===============================================================
    # MESSAGING AND UTILITIES
    # ===============================================================
    
    def send_message(self, target_id: str, message):
        """Send message via simulation."""
        if self.simulation and hasattr(self.simulation, "send_message"):
            self.simulation.send_message(self.node_id, target_id, message)
        else:
            logger.warning(f"Node {self.node_index}: No simulation reference for message sending")
    
    # ===============================================================
    # VERIFICATION AND STATS
    # ===============================================================
    
    def verify_block_integrity(self, block_id: int) -> bool:
        """
        Verify the integrity of a completed block.
        
        Args:
            block_id: Block to verify
            
        Returns:
            True if block is valid, False otherwise
        """
        try:
            if block_id not in self.completed_blocks:
                return False
            
            if block_id not in self.block_original_data:
                return False
            
            metadata = self.block_metadata.get(block_id, {})
            expected_hash = metadata.get('hash', '')
            
            if expected_hash:
                actual_hash = hashlib.sha256(self.block_original_data[block_id]).hexdigest()
                return expected_hash == actual_hash
            
            return True  # No hash to verify against
            
        except Exception as e:
            logger.error(f"❌ Node {self.node_index}: Error verifying block {block_id}: {e}")
            return False
    
    def get_block_completion_progress(self, block_id: int) -> float:
        """
        Get completion progress for a block.
        
        Args:
            block_id: Block to check
            
        Returns:
            Progress as percentage (0.0 to 100.0)
        """
        if block_id in self.completed_blocks:
            return 100.0
        
        if block_id not in self.block_chunks:
            return 0.0
        
        metadata = self.block_metadata.get(block_id, {})
        total_chunks = metadata.get('total_chunks', 1)
        current_chunks = len(self.block_chunks[block_id])
        
        return (current_chunks / total_chunks) * 100.0
    
    def get_node_stats(self) -> Dict[str, Any]:
        """Get comprehensive node statistics."""
        return {
            "node_index": self.node_index,
            "node_id": self.node_id,
            "city": self.city,
            "cluster_id": self.my_cluster_id,
            "cluster_peers_count": len(self.cluster_peers),
            "known_peers_count": len(self.known_peers),
            
            # Block statistics
            "completed_blocks_count": len(self.completed_blocks),
            "generated_blocks_count": len(self.generated_blocks),
            "pending_blocks_count": len(self.pending_chunk_requests),
            "blocks_in_progress": len(self.block_chunks) - len(self.completed_blocks),
            
            # VCS statistics
            "vivaldi_updates_count": self.vivaldi_updates_count,
            "vcs_convergence_complete": self.vcs_convergence_complete,
            "coordinate_frozen": self.coordinate_frozen,
            "virtual_coordinate": {
                "coordinates": self.virtual_coordinate.coordinates,
                "error_indicator": self.virtual_coordinate.error_indicator,
                "update_count": self.virtual_coordinate.update_count
            },
            
            # Mercury protocol statistics
            "mercury_config": {
                "K": self.config.K,
                "dcluster": self.config.dcluster,
                "dmax": self.config.dmax,
                "chunk_size": self.config.chunk_size,
                "enable_early_outburst": self.config.enable_early_outburst,
                "source_fanout": self.config.source_fanout
            }
        }
    

    def _get_node_index_from_id(self, node_id: str) -> int:
        """Get node index from node ID (helper for metrics)."""
        if self.simulation and hasattr(self.simulation, 'nodes_by_id'):
            node = self.simulation.nodes_by_id.get(node_id)
            if node:
                return node.node_index
        
        # Fallback: extract from node_id if possible
        try:
            if node_id.startswith("mercury_"):
                return hash(node_id) % 10000  # Simple hash-based index
        except:
            pass
        
        return -1  # Unknown node

