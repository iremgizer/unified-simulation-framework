# ===============================================================
# MERCURY COMPLETE IMPLEMENTATION - KADCAST STYLE
# ===============================================================

#  SIMULATOR.PY - COMPLETE KADCAST-STYLE IMPLEMENTATION
"""
Mercury Protocol Simulator - COMPLETE KADCAST-STYLE IMPLEMENTATION

Clean implementation following Kadcast's architectural pattern exactly.
Key Flow: run_simulation() → initialize_network() → start_broadcasting()



import logging
import random
import time
import uuid
import sys
import os
import math
import hashlib
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict
import numpy as np
from sklearn.cluster import KMeans

# Core simulation imports
from simulation.event_queue import EventQueue, Event, EventType, SimulationEngine
from simulation.network_delay import NetworkDelayCalculator
from simulation.metrics import Metrics
from data.geo_data_provider import GeoDataProvider
from data.realistic_payload import RealisticBlockGenerator, RealisticChunkGenerator

# Mercury-specific imports
from .node import MercuryNode
from .messages import (
    MercuryVivaldiUpdateMessage, MercuryVivaldiResponseMessage,
    MercuryBlockDigestMessage, MercuryChunkRequestMessage,
    MercuryChunkResponseMessage, MercuryBlockCompleteMessage,
    MercuryConfig, MercuryMessageType
)

logger = logging.getLogger("MercurySimulator")


class MercurySimulator:
    """
    Mercury protocol simulator - EXACT KADCAST STYLE IMPLEMENTATION
   
    """
    
    def __init__(self, config: Dict[str, Any]):

        current_dir = os.path.dirname(os.path.abspath(__file__))
        src_dir = os.path.join(current_dir, '..', '..')
        data_dir = os.path.join(src_dir, 'data')

        """Initialize Mercury simulator."""
        self.config = self._setup_default_config(config)
        self.simulation_id = str(uuid.uuid4())
        self.node_count = self.config.get("nodeCount", 100)
        
        # Broadcasting configuration  
        self.block_size = self.config.get("blockSize", 1024 * 1024)  # 1MB
        self.broadcast_frequency = self.config.get("broadcastFrequency", 10)  # blocks/minute
        self.broadcasting_duration = self.config.get("duration", 300.0)  # 5 minutes
        
        # Mercury protocol configuration
        self.mercury_config = MercuryConfig.from_dict(self.config)
        
        # Simulation components
        self.event_queue = EventQueue()
        self.engine = SimulationEngine(self.event_queue)
        
        # Geo data and block generation
        self.geo_provider = GeoDataProvider(data_dir)
        self.block_generator = RealisticBlockGenerator()

        delay_config = {
         "node_performance_variation": self.config.get("nodePerformanceVariation", 0.1),
         "performance_distribution": self.config.get("nodePerformanceDistribution", {}),
         "random_seed": self.config.get("randomSeed", 42) if self.config.get("deterministicPerformance", True) else None
        }
        
        self.delay_calculator = NetworkDelayCalculator(self.geo_provider, config=delay_config)

        self.metrics_collector = Metrics(self.node_count)
        self.metrics_collector.set_protocol_name("mercury")
        
        # Network state
        self.nodes: Dict[int, MercuryNode] = {}
        self.nodes_by_id: Dict[str, MercuryNode] = {}
        self.running = False
        self.phase = "initialization"
        
        #  VCS convergence tracking (replaces peer_discovery_flags)
        self.vcs_convergence_flags: Dict[int, bool] = {}
        self.vcs_convergence_complete = False
        
        #  Centralized clustering results  
        self.cluster_assignments: Dict[int, int] = {}  # node_index -> cluster_id
        self.cluster_peers: Dict[int, List[str]] = {}  # cluster_id -> [node_ids]
        
        # Performance assignments
        self.node_performance_assignments: Dict[int, str] = {}
        self.performance_distribution = {
            "high_performance": 0.2,
            "average_performance": 0.6,
            "low_performance": 0.2
        }
        
        # Metrics and tracking
        self.blocks_scheduled = []
        self.vcs_convergence_metrics = {}
        self.clustering_metrics = {}
        self.simulation_callbacks = []
        
     
        self.block_completions_by_block = {}  # block_id -> {node_index: completion_time}
        self.coverage_snapshots = {}  # block_id -> [(time, coverage_percentage)]
        self.block_metrics = {}  # Store block metadata for processing delays
        
        self.generated_blocks_content = {} 


        logger.info(f"Mercury simulator initialized with {self.node_count} nodes")

    def _setup_default_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Setup default configuration values."""
        defaults = {
            "nodeCount": 100,
            "blockSize": 1024 * 1024,  # 1MB
            "broadcastFrequency": 10,  # blocks/minute
            "duration": 300.0,         # 5 minutes
            
            # Mercury VCS parameters
            "coordinate_rounds": 100,
            "measurements_per_round": 16,
            "vcs_dimensions": 3,
            "stability_threshold": 0.4,
            
            # Mercury clustering parameters  
            "K": 8,                    # Number of clusters
            "dcluster": 4,             # Close neighbors per cluster
            "dmax": 8,                 # Total relay peers
            
            # Mercury broadcasting parameters
            "enable_early_outburst": True,
            "source_fanout": 128,      # Early outburst fanout
            "chunk_size": 1024,        # 1KB chunks
            "enable_newton_rules": True,
            "fec_ratio": 0.0,          # No FEC for Mercury
            
            # Performance distribution
            "nodePerformanceDistribution": {
                "high_performance": 0.2,
                "average_performance": 0.6,
                "low_performance": 0.2
            }
        }
        
        for key, value in defaults.items():
            if key not in config:
                config[key] = value
        
        return config

    
    def run_simulation(self) -> bool:
        """Run the complete Mercury simulation (centralized approach) - EXACT KADCAST STYLE."""
        try:
            self.running = True
            logger.info(f"🎯 Starting CENTRALIZED Mercury simulation {self.simulation_id}")

            # Register event handlers
            self._register_event_handlers()

            # Phase 1: Initialize network (centralized) 
            if not self.initialize_network():
                raise Exception("Network initialization failed")

            # Phase 2: Skip peer discovery, go directly to broadcasting
            if not self.start_broadcasting():
                raise Exception("Broadcasting start failed")

            # Run event loop
            total_simulation_time = self.broadcasting_duration + 20.0  # 5 minutes + buffer
            logger.info(f"Running Mercury simulation for {total_simulation_time} seconds...")

            self.engine.run(max_time=total_simulation_time)

            logger.info(" Mercury simulation completed successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Mercury simulation failed: {e}")
            self.running = False
            return False
        finally:
            self.running = False

  
    def initialize_network(self) -> bool:
        """Initialize network - EXACT KADCAST STYLE but with Mercury features."""
        try:
            self.phase = "network_initialization"
            logger.info(" Initializing Mercury network...")

            # Assign performance types to nodes (same as Kadcast)
            self.node_performance_assignments = self._assign_node_performances()

            # Update delay calculator with performance assignments (same as Kadcast)
            delay_config = self.delay_calculator.computational_calculator.config.copy()
            delay_config["node_performance_assignments"] = self.node_performance_assignments
            self.delay_calculator.computational_calculator.set_node_performance_assignments(
                self.node_performance_assignments
            )

            # Geographic assignments (same as Kadcast)
            node_assignments = self.geo_provider.assign_nodes_to_regions_and_cities(self.node_count)
            used_ids = set()

            def generate_mercury_node_id() -> str:
                """Generate unique Mercury node ID - similar to Kadcast 64-bit."""
                while True:
                    random_id = random.randint(1, 0xFFFFFFFFFFFFFFFF)  # 64-bit range
                    node_id = f"mercury_{random_id:016x}"  # Mercury prefix
                    
                    if node_id not in used_ids:
                        used_ids.add(node_id)
                        return node_id

            # Create Mercury nodes (similar to Kadcast)
            all_node_ids = []
            for node_index in range(self.node_count):
                assignment = node_assignments[node_index]

                # Generate Mercury node ID
                node_id = generate_mercury_node_id()
                all_node_ids.append(node_id)

                # Get performance type for this node
                performance_type = self.node_performance_assignments.get(node_index, "average_performance")

                #  CREATE MERCURY NODE (instead of KadcastNode)
                node = MercuryNode(
                    node_index=node_index,
                    node_id=node_id,
                    city=assignment["city"],
                    coordinates=assignment["coordinates"],
                    performance_type=performance_type,
                    config=self.mercury_config
                )
                
                node.set_simulation(self)
                self.nodes[node_index] = node
                self.nodes_by_id[node_id] = node

                # Initialize VCS convergence flag (replaces bootstrap logic)
                self.vcs_convergence_flags[node_index] = False

                # Debug log (first 10 nodes)
                if node_index < 10:
                    logger.debug(f"Mercury Node {node_index}: ID={node_id}, City={assignment['city']}, "
                               f"Performance={performance_type}")

            #  CENTRALIZED APPROACH: Give all nodes complete network knowledge (same as Kadcast)
            logger.info(" CENTRALIZED APPROACH: Giving all nodes complete network knowledge...")
            for node_index, node in self.nodes.items():
                # Give each node all other node IDs
                other_node_ids = [nid for nid in all_node_ids if nid != node.node_id]
                node.add_all_network_peers(other_node_ids)

            # Setup network connections
            self._setup_network_connections()

            #  START VCS CONVERGENCE + CLUSTERING (Mercury-specific)
            self._start_vcs_convergence_and_clustering()

            #  Send network initialization update (same structure as Kadcast)
            self._send_update("network_initialized", {
                "nodeCount": self.node_count,
                "geoDistribution": node_assignments,
                "performanceDistribution": {
                    "assignments": self.node_performance_assignments,
                    "distribution": self.performance_distribution
                },
                "mercuryConfig": {
                    "K": self.mercury_config.K,
                    "dcluster": self.mercury_config.dcluster,
                    "dmax": self.mercury_config.dmax,
                    "coordinate_rounds": self.mercury_config.coordinate_rounds,
                    "source_fanout": self.mercury_config.source_fanout
                },
                "broadcastConfig": {
                    "blockSize": self.block_size,
                    "frequency": self.broadcast_frequency,
                    "duration": self.broadcasting_duration
                }
            })

            logger.info(f" Mercury network initialized with {self.node_count} nodes")
            return True

        except Exception as e:
            logger.error(f" Failed to initialize Mercury network: {e}")
            return False

    def _assign_node_performances(self) -> Dict[int, str]:
        """Assign performance types to nodes based on distribution - SAME AS KADCAST."""
        assignments = {}
        performance_types = list(self.performance_distribution.keys())
        probabilities = list(self.performance_distribution.values())
        
        for node_index in range(self.node_count):
            performance_type = random.choices(performance_types, weights=probabilities)[0]
            assignments[node_index] = performance_type
        
        return assignments

    def _setup_network_connections(self):
        """Setup network connections and RTT matrix - SAME AS KADCAST."""
        try:
            # Initialize RTT matrix for all node pairs
            for i, node_i in self.nodes.items():
                for j, node_j in self.nodes.items():
                    if i != j:
                        # Calculate RTT based on geographic distance
                        rtt = self.delay_calculator.calculate_rtt_delay(
                            node_i.city, node_j.city
                        )
                        node_i.set_peer_rtt(node_j.node_id, rtt)
            
            logger.info("Network connections and RTT matrix established")
            
        except Exception as e:
            logger.error(f" Failed to setup network connections: {e}")

    #  MERCURY-SPECIFIC: VCS CONVERGENCE + CLUSTERING
    def _start_vcs_convergence_and_clustering(self):
        """Start VCS convergence and schedule clustering after completion."""
        try:
            logger.info(" Starting VCS convergence phase...")
            self.phase = "vcs_convergence"
            
            # Schedule VCS convergence rounds
            for round_num in range(self.mercury_config.coordinate_rounds):
                self.event_queue.schedule_event(
                    EventType.MERCURY_VCS_CONVERGENCE,
                    node_id=-1,  # Global event
                    delay=round_num * 0.1,  # 100ms between rounds
                    data={
                        "round_number": round_num,
                        "total_rounds": self.mercury_config.coordinate_rounds
                    }
                )
            
            # Schedule clustering after VCS convergence
            clustering_delay = self.mercury_config.coordinate_rounds * 0.1 + 1.0  # After VCS + 1s buffer
            self.event_queue.schedule_event(
                EventType.MERCURY_CLUSTERING,
                node_id=-1,
                delay=clustering_delay,
                data={"action": "perform_centralized_clustering"}
            )
            
            logger.info(f" Scheduled {self.mercury_config.coordinate_rounds} VCS rounds + clustering")
            
        except Exception as e:
            logger.error(f" Failed to start VCS convergence: {e}")

    #  EXACT KADCAST STYLE BROADCASTING START
    def start_broadcasting(self) -> bool:
        """Start Mercury block broadcasting phase - EXACT KADCAST STYLE."""
        try:
            logger.info("📡 Starting Mercury broadcasting phase...")
            self.phase = "broadcasting"
            
            # Calculate broadcasting schedule (same as Kadcast)
            duration = self.config.get('duration', 300.0)
            total_blocks = int((duration / 60.0) * self.broadcast_frequency)
            block_interval = duration / total_blocks if total_blocks > 0 else 60.0
            
            logger.info(f" Mercury broadcasting configuration:")
            logger.info(f"   - Duration: {duration}s")
            logger.info(f"   - Frequency: {self.broadcast_frequency} blocks/minute")
            logger.info(f"   - Total blocks: {total_blocks}")
            logger.info(f"   - Block interval: {block_interval:.2f}s")
            
            #  Schedule Mercury block generation events (same pattern as Kadcast)
            current_time = 1.0  # Start after 1 second
            for block_id in range(total_blocks):
                # Select random source node
                source_node_index = random.randint(0, self.node_count - 1)
                
                self.event_queue.schedule_event(
                    EventType.MERCURY_BLOCK_GENERATE,
                    node_id=source_node_index,
                    delay=current_time,
                    data={
                        "block_id": block_id,
                        "source_node_index": source_node_index,
                        "block_size": self.block_size
                    }
                )
                
                self.blocks_scheduled.append({
                    "block_id": block_id,
                    "source_node": source_node_index,
                    "scheduled_time": current_time
                })
                
                current_time += block_interval

            # Schedule broadcasting completion timeout (same as Kadcast)
            self.event_queue.schedule_event(
                EventType.TIMEOUT,
                node_id=-1,
                delay=self.broadcasting_duration + 10.0,  # 10 second buffer
                data={"timeout_type": "broadcasting_complete"}
            )

            # Send broadcasting start update (same structure as Kadcast)
            self._send_update("broadcasting_started", {
                "duration": self.broadcasting_duration,
                "frequency": self.broadcast_frequency,
                "totalBlocks": total_blocks,
                "phase": "broadcasting"
            })

            logger.info(f" Scheduled {total_blocks} Mercury block broadcasts")
            return True
            
        except Exception as e:
            logger.error(f" Mercury broadcasting start failed: {e}")
            return False

    #  REGISTER EVENT HANDLERS
    def _register_event_handlers(self):
        """Register event handlers with the simulation engine - Mercury-specific."""
        try:
            # Mercury protocol events
            self.engine.register_handler(EventType.MERCURY_BLOCK_GENERATE, self._handle_block_generate_event)
            self.engine.register_handler(EventType.MERCURY_MESSAGE_RECEIVED, self._handle_message_received_event)
            self.engine.register_handler(EventType.MERCURY_VCS_CONVERGENCE, self._handle_vcs_convergence_event)
            self.engine.register_handler(EventType.MERCURY_CLUSTERING, self._handle_clustering_event)
            self.engine.register_handler(EventType.MERCURY_BLOCK_DIGEST, self._handle_digest_event)
            self.engine.register_handler(EventType.MERCURY_CHUNK_RESPONSE, self._handle_chunk_response_event)
            self.engine.register_handler(EventType.MERCURY_BLOCK_COMPLETE, self._handle_mercury_block_complete_event)
            
            # Simulation control events
            self.engine.register_handler(EventType.TIMEOUT, self._handle_timeout_event)
            
            logger.info(" Mercury event handlers registered")
            
        except Exception as e:
            logger.error(f" Failed to register event handlers: {e}")

    def _handle_message_received_event(self, event: Event):
        """Handle Mercury message reception with processing delays."""
        try:
            message_type = event.data.get("message_type")
            receiver_node_id = event.data.get("receiver_node_id")
            message = event.data.get("message")
            sender_id = event.data.get("sender_id")
            
            # Get receiver node
            receiver_node = self._get_node_by_id(receiver_node_id)
            if not receiver_node:
                logger.warning(f"Receiver node not found: {receiver_node_id}")
                return
            
            # Message processing with computational delays
            if isinstance(message, MercuryBlockDigestMessage):
                # Add digest processing delay
                processing_delay = self.delay_calculator.calculate_chunk_processing_delay(
                    receiver_node.node_index, message.size
                )
                
                # Schedule digest processing
                self.event_queue.schedule_event(
                    EventType.MERCURY_BLOCK_DIGEST,
                    node_id=receiver_node.node_index,
                    delay=processing_delay,
                    data={
                        "sender_id": sender_id,
                        "receiver_node_id": receiver_node_id,
                        "message": message,
                        "action": "process_digest"
                    }
                )
                
            elif isinstance(message, MercuryChunkRequestMessage):
                # Direct processing (no significant delay)
                receiver_node.handle_chunk_request(message)
                
            elif isinstance(message, MercuryChunkResponseMessage):
                # Add chunk processing delay
                chunk_size = len(message.chunk_data) if hasattr(message, 'chunk_data') else 1024
                processing_delay = self.delay_calculator.calculate_chunk_processing_delay(
                    receiver_node.node_index, chunk_size
                )
                
                # Schedule chunk processing
                self.event_queue.schedule_event(
                    EventType.MERCURY_CHUNK_RESPONSE,
                    node_id=receiver_node.node_index,
                    delay=processing_delay,
                    data={
                        "sender_id": sender_id,
                        "receiver_node_id": receiver_node_id,
                        "message": message,
                        "action": "process_chunk"
                    }
                )
                
            elif isinstance(message, MercuryVivaldiUpdateMessage):
                receiver_node.handle_vivaldi_update(message)
                
            elif isinstance(message, MercuryVivaldiResponseMessage):
                receiver_node.handle_vivaldi_response(message)
                
            else:
                logger.warning(f"Unknown Mercury message type: {type(message).__name__}")
                
        except Exception as e:
            logger.error(f"Error handling Mercury message: {e}")

    def _handle_vcs_convergence_event(self, event: Event):
        """Handle VCS convergence round."""
        try:
            round_number = event.data.get("round_number")
            total_rounds = event.data.get("total_rounds")
            
            logger.debug(f"VCS convergence round {round_number + 1}/{total_rounds}")
            
            # Each node performs Vivaldi updates
            for node_index, node in self.nodes.items():
                if hasattr(node, 'virtual_coordinate'):
                    # Track error before update
                    error_before = node.virtual_coordinate.error_indicator
                    coord_before = node.virtual_coordinate.coordinates.copy()
                
                    node.vivaldi_convergence_round(round_number)

                    error_after = node.virtual_coordinate.error_indicator
                    coord_after = node.virtual_coordinate.coordinates

                    coord_change = 0.0
                    if len(coord_before) == len(coord_after):
                        coord_change = sum((a - b) ** 2 for a, b in zip(coord_after, coord_before)) ** 0.5
                  
                    # Report VCS update
                    self.metrics_collector.report_vivaldi_update(
                        node_index, round_number, coord_change,
                        error_before, error_after, self.event_queue.current_time
                    )                
            
            # Check if this is the last round
            if round_number == total_rounds - 1:
                logger.info("VCS convergence rounds completed")
                
                # Mark convergence flags
                for node_index, node in self.nodes.items():
                    if (hasattr(node, 'virtual_coordinate') and 
                        node.virtual_coordinate.error_indicator < self.mercury_config.stability_threshold):
                        self.vcs_convergence_flags[node_index] = True
                
                self.vcs_convergence_complete = True
                
        except Exception as e:
            logger.error(f" Error in VCS convergence: {e}")

    def _handle_clustering_event(self, event: Event):
        """Handle centralized clustering."""
        try:
            action = event.data.get("action")
            
            if action == "perform_centralized_clustering":
                logger.info(" Performing centralized K-means clustering...")
                
                # Collect stable node coordinates
                stable_coordinates = []
                stable_node_indices = []
                
                for node_index, node in self.nodes.items():
                    if (hasattr(node, 'virtual_coordinate') and 
                        node.virtual_coordinate.error_indicator < self.mercury_config.stability_threshold):
                        stable_coordinates.append(node.virtual_coordinate.coordinates)
                        stable_node_indices.append(node_index)
                
                if len(stable_coordinates) < self.mercury_config.K:
                    # Use all nodes if not enough stable ones
                    stable_coordinates = []
                    stable_node_indices = []
                    
                    for node_index, node in self.nodes.items():
                        if hasattr(node, 'virtual_coordinate'):
                            stable_coordinates.append(node.virtual_coordinate.coordinates)
                            stable_node_indices.append(node_index)
                
                # Perform K-means clustering
                effective_K = min(self.mercury_config.K, len(stable_coordinates))
                if effective_K < 2:
                    effective_K = 2
                
                kmeans = KMeans(n_clusters=effective_K, random_state=42, n_init=10)
                cluster_labels = kmeans.fit_predict(stable_coordinates)
                
                # Assign clusters to nodes
                for i, node_index in enumerate(stable_node_indices):
                    cluster_id = cluster_labels[i]
                    self.cluster_assignments[node_index] = cluster_id
                    
                    if cluster_id not in self.cluster_peers:
                        self.cluster_peers[cluster_id] = []
                    self.cluster_peers[cluster_id].append(self.nodes[node_index].node_id)
                
                # Assign unstable nodes to random clusters
                for node_index, node in self.nodes.items():
                    if node_index not in self.cluster_assignments:
                        random_cluster = random.randint(0, effective_K - 1)
                        self.cluster_assignments[node_index] = random_cluster
                        self.cluster_peers[random_cluster].append(node.node_id)
                
                # Give cluster assignments to all nodes
                for node_index, node in self.nodes.items():
                    cluster_id = self.cluster_assignments[node_index]
                    cluster_peer_ids = [pid for pid in self.cluster_peers[cluster_id] if pid != node.node_id]
                    node.set_cluster_assignment(cluster_id, cluster_peer_ids)

                    # Calculate close vs random neighbors
                    close_neighbors = min(self.mercury_config.dcluster, len(cluster_peer_ids))
                    random_neighbors = self.mercury_config.dmax - close_neighbors
                
                    self.metrics_collector.report_cluster_assignment(
                        node_index, cluster_id, len(self.cluster_peers[cluster_id]),
                        close_neighbors, random_neighbors, self.event_queue.current_time
                    )

                # Store clustering metrics
                cluster_sizes = [len(peers) for peers in self.cluster_peers.values()]
                self.clustering_metrics = {
                    "clusters_formed": len(self.cluster_peers),
                    "expected_clusters": self.mercury_config.K,
                    "actual_clusters": effective_K,
                    "cluster_sizes": cluster_sizes,
                    "stable_nodes_clustered": len(stable_node_indices),
                    "total_nodes": self.node_count
                }
                
                logger.info(f" Clustering completed: {effective_K} clusters, sizes: {cluster_sizes}")
                
        except Exception as e:
            logger.error(f" Error in clustering: {e}")

    def _handle_digest_event(self, event: Event):
        """Handle Mercury block digest processing."""
        try:
            action = event.data.get("action")
            
            if action == "process_digest":
                message = event.data.get("message")
                receiver_node_id = event.data.get("receiver_node_id")
                
                receiver_node = self._get_node_by_id(receiver_node_id)
                if receiver_node:
                    chunk_request = receiver_node.handle_block_digest(message)
                    if chunk_request:
                        self.metrics_collector.report_chunk_request(
                           receiver_node.node_index, message.block_id,
                           len(chunk_request.chunk_ids), message.sender_id,
                           self.event_queue.current_time
                       )
                        logger.debug(f" Node {receiver_node.node_index}: Requested chunks for block {message.block_id}")
                        
        except Exception as e:
            logger.error(f" Error processing digest: {e}")

    def _handle_chunk_response_event(self, event: Event):
        """Handle Mercury chunk response processing."""
        try:
            action = event.data.get("action")
            
            if action == "process_chunk":
                message = event.data.get("message")
                receiver_node_id = event.data.get("receiver_node_id")
                sender_id = event.data.get("sender_id")
                
                receiver_node = self._get_node_by_id(receiver_node_id)
                sender_node = self._get_node_by_id(sender_id)

                if receiver_node and sender_node:
                    self.metrics_collector.report_chunk_response(
                       sender_node.node_index, message.block_id,
                       1, receiver_node.node_index,  # 1 chunk sent
                       self.event_queue.current_time
                   )

                    receiver_node.handle_chunk_response(message)
                    
        except Exception as e:
            logger.error(f" Error processing chunk: {e}")

    def _handle_timeout_event(self, event: Event):
        """Handle timeout events."""
        try:
            timeout_type = event.data.get("timeout_type")
            
            if timeout_type == "broadcasting_complete":
                logger.info(" Broadcasting timeout reached")
                
        except Exception as e:
            logger.error(f" Error handling timeout: {e}")

    # MESSAGING SYSTEM
    def send_message(self, sender_id: str, receiver_id: str, message):
        """Send message with transmission delays - SAME AS KADCAST."""
        try:
            sender_node = self._get_node_by_id(sender_id)
            receiver_node = self._get_node_by_id(receiver_id)
            
            if not sender_node or not receiver_node:
                logger.warning(f"Invalid sender ({sender_id}) or receiver ({receiver_id})")
                return
            
            # Calculate transmission delay
            message_size = getattr(message, 'size', 64)
            transmission_delays = self.delay_calculator.calculate_transmission_delay(
                sender_node.node_index,
                receiver_node.node_index,
                sender_node.city,
                receiver_node.city,
                message_size,
                self.event_queue.current_time
            )
            
            delay = transmission_delays.get("transmission_delay", 0.001)
            if delay <= 0:
                delay = 0.001  # Minimum 1ms delay
            
            # Schedule message reception
            self.event_queue.schedule_event(
                EventType.MERCURY_MESSAGE_RECEIVED,
                node_id=receiver_node.node_index,
                delay=delay,
                data={
                    "sender_id": sender_id,
                    "receiver_node_id": receiver_id,
                    "message": message,
                    "message_type": message.message_type
                }
            )
            
            # Track bandwidth usage
            self.metrics_collector.report_message_sent(
              sender_node.node_index,       # source_id
              receiver_node.node_index,     # target_id
              message.message_type,   # message_type (string olarak)
              message_size,                 # message_size
              self.event_queue.current_time                  # current_time
            )
        except Exception as e:
            logger.error(f" Error sending Mercury message: {e}")

    def _get_node_by_id(self, node_id: str) -> Optional[MercuryNode]:
        """Get node by ID."""
        return self.nodes_by_id.get(node_id)

 
    def calculate_current_coverage(self, block_id: int) -> float:
        """
        Calculate current coverage percentage for a block.
        
        Args:
            block_id: Block to calculate coverage for
            
        Returns:
            Coverage percentage (0.0 to 100.0)
        """
        if block_id not in self.block_completions_by_block:
            return 0.0
        
        completed_nodes = len(self.block_completions_by_block[block_id])
        total_nodes = self.node_count
        
        if total_nodes == 0:
            return 0.0
        
        coverage = (completed_nodes / total_nodes) * 100.0
        return coverage

    def calculate_final_coverage_metrics(self) -> Dict[str, Any]:
        """Calculate final coverage metrics for all blocks."""
        if not self.block_completions_by_block:
            return {
                "average_coverage": 0.0,
                "max_coverage": 0.0,
                "min_coverage": 0.0,
                "blocks_with_full_coverage": 0,
                "total_blocks": 0,
                "coverage_distribution": []
            }
        
        coverage_percentages = []
        blocks_with_full_coverage = 0
        
        for block_id, completions in self.block_completions_by_block.items():
            coverage = (len(completions) / self.node_count) * 100.0
            coverage_percentages.append(coverage)
            
            if coverage >= 100.0:
                blocks_with_full_coverage += 1
        
        if coverage_percentages:
            avg_coverage = sum(coverage_percentages) / len(coverage_percentages)
            max_coverage = max(coverage_percentages)
            min_coverage = min(coverage_percentages)
        else:
            avg_coverage = max_coverage = min_coverage = 0.0
        
        return {
            "average_coverage": avg_coverage,
            "max_coverage": max_coverage,
            "min_coverage": min_coverage,
            "blocks_with_full_coverage": blocks_with_full_coverage,
            "total_blocks": len(self.block_completions_by_block),
            "coverage_distribution": coverage_percentages
        }

    
    # ===============================================================
# MERCURY SIMULATOR.PY - LATENCY CALCULATION FIX
# ===============================================================

    def on_block_completed(self, node_index: int, block_id: int, completion_time: float):
        """
        Handle block completion callback from Mercury nodes - FIXED VERSION WITH LATENCY.
        
        Args:
            node_index: Index of the node that completed the block
            block_id: ID of the completed block  
            completion_time: Simulation time when block was completed (NOT real time)
        """
        try:
            # Log the completion
            logger.debug(f" Mercury Block {block_id} completed on Node {node_index} at {completion_time:.3f}s")
            
           
            if block_id not in self.block_completions_by_block:
                self.block_completions_by_block[block_id] = {}
            
            self.block_completions_by_block[block_id][node_index] = completion_time
            
         
            if block_id not in self.block_metrics:
                # Initialize if missing
                self.block_metrics[block_id] = {
                    'generation_time': 0.0,
                    'completion_times': {},
                    'source_node': -1
                }
            
            # Ensure completion_times exists
            if 'completion_times' not in self.block_metrics[block_id]:
                self.block_metrics[block_id]['completion_times'] = {}
            
            # Store completion time for this node
            self.block_metrics[block_id]['completion_times'][node_index] = completion_time
            
            
            current_coverage = self.calculate_current_coverage(block_id)
            
       
            if block_id not in self.coverage_snapshots:
                self.coverage_snapshots[block_id] = []
            self.coverage_snapshots[block_id].append((completion_time, current_coverage))
            
            logger.debug(f"Mercury Block {block_id} coverage: {current_coverage:.1f}% ({len(self.block_completions_by_block[block_id])}/{self.node_count} nodes)")
            
            # Rest of the method stays the same...
            # [Processing delays and event scheduling code here]
            
        except Exception as e:
            logger.error(f"Error in Mercury block completion callback: {e}")

    def _handle_block_generate_event(self, event: Event):
        """Handle Mercury block generation with chunking and Real Block Data Capture - LATENCY FIX."""
        try:
            source_node_index = event.node_id
            source_node = self.nodes[source_node_index]
            block_id = event.data.get("block_id", 0)
            block_size = event.data.get("block_size", self.block_size)
        
            logger.info(f" Generating Mercury block {block_id} from node {source_node_index}")
            
            # ... [Block generation code] ...
            
            #  Store block metrics with proper completion_times structure
            current_time = self.event_queue.current_time
            self.block_metrics[block_id] = {
                'transactions': transactions,
                'block_size': actual_size,
                'total_chunks': len(chunks),
                'generation_time': current_time,  
                'source_node': source_node_index,
                'completion_times': {}  
            }
            
            #  CRITICAL FIX: Source node completion should be recorded in metrics too
            self.block_metrics[block_id]['completion_times'][source_node_index] = current_time
            
            logger.info(f" Mercury block {block_id}: {actual_size} bytes → {len(chunks)} chunks (NO FEC)")
            
            # Rest of method...
            
        except Exception as e:
            logger.error(f"Error in Mercury block generation: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")


    # MERCURY_BLOCK_COMPLETE event handler
    def _handle_mercury_block_complete_event(self, event: Event):
        """Handle Mercury block completion after processing delays."""
        try:
            node_index = event.data.get("node_index")
            block_id = event.data.get("block_id")
            final_completion_time = event.data.get("completion_time")
        
            logger.info(f" Mercury Block {block_id} FULLY PROCESSED on Node {node_index} at {final_completion_time:.3f}s")
        
            # Update metrics with final completion
            if hasattr(self, 'metrics_collector'):
                self.metrics_collector.report_block_processed(node_index, block_id, final_completion_time)
        
            # Send final completion update
            self._send_update("mercury_block_fully_completed", {
                "node_index": node_index,
                "block_id": block_id,
                "final_completion_time": final_completion_time,
                "protocol": "mercury"
            })
        
        except Exception as e:
            logger.error(f" Error handling Mercury block complete event: {e}")

    def _handle_block_generate_event(self, event: Event):
        """Handle Mercury block generation with chunking and Real Block Data Capture."""
        try:
            source_node_index = event.node_id
            source_node = self.nodes[source_node_index]
            block_id = event.data.get("block_id", 0)
            block_size = event.data.get("block_size", self.block_size)
        
            logger.info(f" Generating Mercury block {block_id} from node {source_node_index}")
        
            # Initialize variables with default values
            actual_size = block_size  # Default fallback
            block_data = b''  # Default empty data
            transactions = []  # Default empty transactions
            header_data = {}  # Default empty header
        
            try:
                # 1. Generate realistic block using RealisticBlockGenerator
                prev_hash = f"0x{'0' * 64}" if block_id == 0 else f"0x{hash(str(block_id - 1)):016x}{'0' * 48}"
                header_data, transactions = self.block_generator.generate_block(
                    block_id=block_id,
                    prev_hash=prev_hash,
                    block_size_bytes=block_size
                )
            
                # 2. Serialize block
                block_data = self.block_generator.serialize_block(header_data, transactions)
                actual_size = len(block_data)
                
                import hashlib
                block_hash = hashlib.sha256(block_data).hexdigest()
                
                self.generated_blocks_content[block_id] = {
                    'block_id': block_id,
                    'block_hash': block_hash,
                    'transactions': transactions,     # GERÇEK!
                    'header_data': header_data,       # GERÇEK!
                    'source': 'RealisticBlockGenerator',
                    'data_type': 'real_simulation_data',
                    'protocol': 'mercury'
                }
                
                logger.info(f"MERCURY REAL BLOCK DATA CAPTURED for block {block_id} - {len(transactions)} transactions")
            
            except Exception as e:
                logger.error(f" Error generating block data for Mercury block {block_id}: {e}")
                # Use fallback values
                block_data = f"fallback_block_{block_id}".encode('utf-8') * (block_size // 20)
                actual_size = len(block_data)
                transactions = []  # Empty transactions for fallback
                header_data = {"fallback": True}
                logger.warning(f"Using fallback data for Mercury block {block_id}: {actual_size} bytes")
        
            #  3. MERCURY CHUNKING (NO FEC) - SIMULATOR DOES THIS
            try:
                from data.realistic_payload import RealisticChunkGenerator
                chunks = RealisticChunkGenerator.chunkify_block_mercury(
                    data=block_data,
                    chunk_size=self.mercury_config.chunk_size,
                    block_id=block_id
                )
            except Exception as e:
                logger.error(f" Error chunking Mercury block {block_id}: {e}")
                # Fallback chunking
                chunk_size = self.mercury_config.chunk_size
                chunks = [block_data[i:i+chunk_size] for i in range(0, len(block_data), chunk_size)]
                if not chunks:  # Ensure at least one chunk
                    chunks = [block_data]
                logger.warning(f"Using fallback chunking for Mercury block {block_id}: {len(chunks)} chunks")
        
            # Calculate final block hash
            import hashlib
            final_block_hash = hashlib.sha256(block_data).hexdigest()
        
            #  4. Store chunks in source node
            source_node.block_chunks[block_id] = {i: chunk for i, chunk in enumerate(chunks)}
            source_node.block_metadata[block_id] = {
                'hash': final_block_hash,
                'total_chunks': len(chunks),
                'block_size': actual_size,
                'source': source_node.node_id,
                'generation_time': self.event_queue.current_time
            }
            source_node.block_original_data[block_id] = block_data
            source_node.generated_blocks.add(block_id)
        
            #  Source node should complete its own block immediately
            current_time = self.event_queue.current_time
            source_node.completed_blocks.add(block_id)
            source_node.block_completion_times[block_id] = current_time
            
            # Trigger completion callback for source node
            logger.info(f" Source Node {source_node_index}: Auto-completed own block {block_id}")
            self.on_block_completed(source_node_index, block_id, current_time)
        
            # Store block metrics for completion delays
            self.block_metrics[block_id] = {
                'transactions': transactions,
                'block_size': actual_size,
                'total_chunks': len(chunks),
                'generation_time': current_time,
                'source_node': source_node_index
            }
        
            logger.info(f" Mercury block {block_id}: {actual_size} bytes → {len(chunks)} chunks (NO FEC)")
            
            #  REPORT BLOCK START TO METRICS (with current_time)
            if hasattr(self, 'metrics_collector'):
                self.metrics_collector.report_block_start(
                    block_id, source_node_index, current_time
                )
        
            
            try:
                generation_delay = self.delay_calculator.calculate_block_generation_delay(
                    source_node_index, transactions, actual_size
                )
                chunkify_delay = self.delay_calculator.calculate_chunkify_delay(
                    source_node_index, actual_size, self.mercury_config.chunk_size, 0.0  # No FEC
                )
                total_generation_delay = generation_delay + chunkify_delay
            except Exception as e:
                logger.error(f" Error calculating delays for Mercury block {block_id}: {e}")
                total_generation_delay = 0.001  # Minimal delay fallback
        
         
            try:
                relay_peers = source_node.select_relay_peers(is_source=True)
            
                
                immediate_coverage = (len(relay_peers) / self.node_count) * 100
                if hasattr(self, 'metrics_collector'):
                    self.metrics_collector.report_early_outburst(
                        source_node_index, block_id, len(relay_peers), 
                        immediate_coverage, current_time
                    )
            
                # Create block digest message
                from .messages import MercuryBlockDigestMessage
                digest_msg = MercuryBlockDigestMessage(
                    sender_id=source_node.node_id,
                    block_id=block_id,
                    block_hash=final_block_hash,
                    total_chunks=len(chunks),
                    block_size=actual_size
                )
            
               
                for peer_id in relay_peers:
                    receiver_node = self._get_node_by_id(peer_id)
                    if receiver_node:
                        self.event_queue.schedule_event(
                            EventType.MERCURY_MESSAGE_RECEIVED,
                            node_id=receiver_node.node_index,
                            delay=total_generation_delay,
                            data={
                                "sender_id": source_node.node_id,
                                "receiver_node_id": peer_id,
                                "message": digest_msg,
                                "message_type": "BLOCK_DIGEST"
                            }
                        )
            
                logger.info(f"📡 Mercury block {block_id} digest scheduled to {len(relay_peers)} peers")
            
            except Exception as e:
                logger.error(f"Error in Mercury block broadcasting: {e}")
            
        except Exception as e:
            logger.error(f" Error in Mercury block generation: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")


   
    def _send_update(self, event_type: str, data: Dict[str, Any]):
        """Send simulation update to callbacks."""
        update = {
            "event": event_type,
            "timestamp": time.time(),
            "simulation_id": self.simulation_id,
            "data": data
        }
        
        for callback in self.simulation_callbacks:
            try:
                callback(update)
            except Exception as e:
                logger.error(f"Error in simulation callback: {e}")

    def add_simulation_callback(self, callback):
        """Add simulation callback."""
        self.simulation_callbacks.append(callback)

   
    def get_simulation_results(self) -> Dict[str, Any]:
        """Get complete simulation results with coverage metrics."""
        
        coverage_metrics = self.calculate_final_coverage_metrics()
        
        return {
            "simulation_id": self.simulation_id,
            "protocol": "mercury",
            "config": self.config,
            "mercury_config": {
                "K": self.mercury_config.K,
                "dcluster": self.mercury_config.dcluster,
                "dmax": self.mercury_config.dmax,
                "coordinate_rounds": self.mercury_config.coordinate_rounds,
                "source_fanout": self.mercury_config.source_fanout
            },
            "metrics": self.metrics_collector.get_all_metrics(),
            "coverage_metrics": coverage_metrics,  
            "vcs_convergence": self.vcs_convergence_metrics,
            "clustering": self.clustering_metrics,
            "node_count": self.node_count,
            "blocks_scheduled": len(self.blocks_scheduled)
        }
    



    def get_real_block_data(self, block_id: int) -> Optional[Dict[str, Any]]:
        """
        Get real block data for specific block ID (Mercury version).
        
        Returns:
            Real block data including transactions, headers, and source info
        """
        return self.generated_blocks_content.get(block_id)

    def get_all_real_blocks(self) -> Dict[int, Dict[str, Any]]:
        """
        Get all real block data (Mercury version).
        
        Returns:
            Complete dictionary of all captured real block data
        """
        return self.generated_blocks_content.copy()

    def export_simulation_blocks(self) -> List[Dict[str, Any]]:
        """
        Export simulation blocks for JSON export (Mercury version).
        
        Returns:
            Formatted block data for JSON export with real content
        """
        try:
            exported_blocks = []
            
            for block_id, real_data in self.generated_blocks_content.items():
                # Get simulation metrics for this block
                block_metrics = self.block_metrics.get(block_id, {})
                
                # Combine real data with simulation metrics
                exported_block = {
                    'block_id': block_id,
                    'broadcast_time': block_metrics.get('generation_time', 0.0),
                    'block_size': block_metrics.get('block_size', 0),
                    'content': {
                        'transactions': real_data['transactions'],    # REAL
                        'header_data': real_data['header_data'],      # REAL
                        'block_hash': real_data['block_hash'],        # REAL
                        'source': 'RealisticBlockGenerator',          # MARK
                        'data_type': 'real_simulation_data'           # VERIFY
                    },
                    'mercury_metrics': {
                        'source_node': block_metrics.get('source_node'),
                        'completion_times': block_metrics.get('completion_times', {}),
                        'nodes_completed': block_metrics.get('nodes_completed', 0),
                        'vcs_convergence_data': block_metrics.get('vcs_convergence_data', {}),
                        'clustering_data': block_metrics.get('clustering_data', {})
                    },
                    'node_completions': self._format_mercury_completions(block_id)
                }
                
                exported_blocks.append(exported_block)
            
            logger.info(f"Exported {len(exported_blocks)} Mercury blocks with real data")
            return exported_blocks
            
        except Exception as e:
            logger.error(f"Error exporting Mercury simulation blocks: {e}")
            return []

    def _format_mercury_completions(self, block_id: int) -> List[Dict[str, Any]]:
        """Format node completions for Mercury block export."""
        try:
            completions = []
            block_metrics = self.block_metrics.get(block_id, {})
            completion_times = block_metrics.get('completion_times', {})
            
            for node_index, completion_time in completion_times.items():
                completion_data = {
                    'node_id': f'mercury_node_{node_index}',
                    'completion_time': completion_time,
                    'method': 'vcs_clustering_completion',  # Mercury-specific
                    'latency': completion_time - block_metrics.get('generation_time', 0.0)
                }
                
                # Add Mercury-specific data if available
                vcs_data = block_metrics.get('vcs_convergence_data', {})
                if node_index in vcs_data:
                    completion_data['vcs_convergence'] = vcs_data[node_index]
                    completion_data['clustering_efficiency'] = vcs_data[node_index].get('clustering_rate', 0)
                
                completions.append(completion_data)
            
            return completions
            
        except Exception as e:
            logger.error(f"Error formatting Mercury completions: {e}")
            return []
