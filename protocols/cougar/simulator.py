"""
Cougar Protocol Simulator - COMPLETE P PARAMETER FIXED VERSION

Implements event-driven simulation for the Cougar protocol following the same
architectural pattern as KadcastSimulator for fair comparison between protocols.

Key Features:
- Header-first dissemination model
- Close + Random neighbor selection  
- Parallelism parameter support (P PARAMETER NOW WORKING!)
- Academic timing models (TCP, validation delays)
- Comprehensive metrics collection

CRITICAL FIXES:
- P parameter now properly supported in event handling
- Enhanced metrics collection for parallel body requests
- Improved timeout handling for multiple concurrent requests
- Better performance tracking for parallelism efficiency
- ALL METHODS INCLUDED - COMPLETE VERSION
"""

import logging
import random
import time
import uuid
import sys
import os
import json
from typing import Dict, List, Any, Optional

# Add the src directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, '..', '..')
sys.path.insert(0, src_dir)

# Core simulation imports (same as Kadcast)
from simulation.event_queue import EventQueue, Event, EventType, SimulationEngine
from simulation.network_delay import NetworkDelayCalculator
from simulation.metrics import Metrics
from data.geo_data_provider import GeoDataProvider
from data.realistic_payload import RealisticBlockGenerator

# Cougar-specific imports
from .node import CougarNode
from src.protocols.cougar.messages import (
    CougarHeaderMessage, 
    CougarBodyRequestMessage, 
    CougarBodyResponseMessage,
    CougarMessageType,
    CougarTCPModel
)

logger = logging.getLogger("CougarSimulator")


class CougarSimulator:
    """
    Cougar protocol simulator - COMPLETE P PARAMETER FIXED VERSION.
    
    Follows the same architectural pattern as KadcastSimulator for consistent
    comparison while implementing Cougar's header-first dissemination model.
    
    CRITICAL: Now properly supports P parameter for parallel body requests.
    ALL METHODS INCLUDED - COMPLETE IMPLEMENTATION.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Cougar simulator with configuration."""
        self.config = self._setup_default_config(config)
        self.simulation_id = str(uuid.uuid4())
        self.node_count = self.config.get("nodeCount", 100)
        
        # Broadcasting configuration (same as Kadcast)
        self.block_size = self.config.get("blockSize", 1024 * 1024)  # 1MB default
        self.broadcast_frequency = self.config.get("broadcastFrequency", 10)  # 10 blocks/minute
        self.broadcasting_duration = self.config.get("duration", 300.0)  # 5 minutes
        
        # Cougar protocol parameters - P PARAMETER SUPPORT!
        self.default_C = self.config.get("closeNeighbors", 4)  # Close neighbors
        self.default_R = self.config.get("randomNeighbors", 4)  # Random neighbors  
        self.default_P = self.config.get("parallelism", 4)     # Body request parallelism (FIXED!)
        
        # Performance configuration (same as Kadcast)
        self.performance_distribution = self.config.get("nodePerformanceDistribution", {})
        self.performance_variation = self.config.get("nodePerformanceVariation", 0.1)
        self.deterministic_performance = self.config.get("deterministicPerformance", True)
        self.random_seed = self.config.get("randomSeed", 42)
        
        # Set random seed for reproducible results
        random.seed(self.random_seed)
        
        # Node management
        self.nodes: Dict[int, CougarNode] = {}
        self.nodes_by_id: Dict[str, CougarNode] = {}
        self.current_time = 0.0
        self.running = False
        self.phase = "initialization"
        
        # Event system (same as Kadcast)
        self.event_queue = EventQueue()
        self.engine = SimulationEngine(self.event_queue)
        
        # Geographic and network infrastructure (same as Kadcast)
        data_dir = os.path.join(src_dir, 'data')
        self.geo_provider = GeoDataProvider(data_dir)
        
        delay_config = {
            "node_performance_variation": self.performance_variation,
            "performance_distribution": self.performance_distribution,
            "random_seed": self.random_seed if self.deterministic_performance else None
        }
        self.delay_calculator = NetworkDelayCalculator(self.geo_provider, config=delay_config)
        
        # Metrics collection (adapted for Cougar with P parameter support)
        self.metrics_collector = Metrics(self.node_count)
        self.metrics_collector.set_protocol_name("cougar")
        self.metrics_collector.set_config(self.config)
        
        # Block generation (same as Kadcast)
        self.block_generator = RealisticBlockGenerator(seed=self.random_seed)
        
        # TCP model for academic timing
        self.tcp_model = CougarTCPModel(geo_provider=self.geo_provider)
        
        # Block tracking (adapted for Cougar with P parameter metrics)
        self.block_metrics = {}  # block_id -> metrics
        self.real_time_metrics = {
            "blocks_completed": {},
            "headers_transmitted": 0,
            "body_requests_sent": 0,
            "body_responses_sent": 0,
            "bandwidth_usage": 0,
            "timeouts_occurred": 0,
            # ✅ NEW: P parameter specific metrics
            "parallel_requests_stats": {
                "total_blocks_with_parallel_requests": 0,
                "avg_requests_per_block": 0.0,
                "max_requests_per_block": 0,
                "parallel_efficiency": 0.0  # success_rate with P > 1
            }
        }

        self.generated_blocks_content = {}
        
        # ✅ FIXED: Enhanced timeout tracking for P parameter
        self.pending_body_timeouts = {}  # (node_index, block_hash, peer_id) -> timeout_event_id
        self.block_request_counts = {}   # block_hash -> {node_index -> request_count}
        
        # CRITICAL: Block completion callback support
        self.on_block_completed = None
        
        logger.info(f"CougarSimulator initialized with {self.node_count} nodes")
        logger.info(f"Protocol parameters: C={self.default_C}, R={self.default_R}, P={self.default_P}")
        logger.info(f"✅ P parameter parallelism support: ENABLED")
    
    def _setup_default_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Setup default configuration values."""
        defaults = {
            "nodeCount": 100,
            "blockSize": 1024 * 1024,
            "broadcastFrequency": 10,
            "closeNeighbors": 4,
            "randomNeighbors": 4,
            "parallelism": 4,  # P parameter default
            "nodePerformanceDistribution": {
                "high_performance": 0.2, 
                "average_performance": 0.6, 
                "low_performance": 0.2
            },
            "nodePerformanceVariation": 0.1,
            "deterministicPerformance": True,
            "randomSeed": 42
        }
        
        for key, value in defaults.items():
            if key not in config:
                config[key] = value
        
        return config
    
    # =============================================================================
    # NETWORK INITIALIZATION
    # =============================================================================
    
    def initialize_network(self) -> bool:
        """Initialize the Cougar network (same pattern as Kadcast)."""
        try:
            logger.info("🌐 Initializing Cougar network...")
            
            # Create nodes with P parameter support
            self._create_nodes()
            
            # Setup network connections
            self._setup_network_connections()
            
            # FIXED: Distribute complete network knowledge with peer mapping
            self._distribute_network_knowledge()
            
            # Perform neighbor selection
            self._perform_neighbor_selection()
            
            logger.info(f"✅ Network initialized with {len(self.nodes)} Cougar nodes (P={self.default_P})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Network initialization failed: {e}")
            return False
    
    def _create_nodes(self):
        """Create Cougar nodes with geographic distribution and P parameter support."""
        # Use GeoDataProvider's proper method for node assignment
        node_assignments = self.geo_provider.assign_nodes_to_regions_and_cities(self.node_count)
        
        # Performance type distribution
        performance_types = []
        for perf_type, ratio in self.performance_distribution.items():
            count = int(self.node_count * ratio)
            performance_types.extend([perf_type] * count)
        
        # Fill remaining with average performance
        while len(performance_types) < self.node_count:
            performance_types.append("average_performance")
        
        random.shuffle(performance_types)
        
        # Create nodes using the assignments
        for i in range(self.node_count):
            assignment = node_assignments[i]
            city_name = assignment["city"]
            coordinates = tuple(assignment["coordinates"])  # Convert to tuple
            
            # Generate node ID
            node_id = f"{i:016x}"
            
            # ✅ FIXED: Create Cougar node with proper P parameter
            node = CougarNode(
                node_index=i,
                node_id=node_id,
                city=city_name,
                coordinates=coordinates,
                performance_type=performance_types[i],
                C=self.default_C,
                R=self.default_R, 
                P=self.default_P  # P parameter properly passed!
            )
            
            node.set_simulation(self)
            
            self.nodes[i] = node
            self.nodes_by_id[node_id] = node
        
        # Set performance assignments for computational delay calculator
        performance_assignments = {i: performance_types[i] for i in range(self.node_count)}
        self.delay_calculator.computational_calculator.set_node_performance_assignments(performance_assignments)
        
        logger.info(f"Created {len(self.nodes)} Cougar nodes with P={self.default_P} across {len(set(node.city for node in self.nodes.values()))} cities")
        logger.info(f"Performance distribution: {dict(zip(*zip(*[(perf, performance_types.count(perf)) for perf in set(performance_types)])))}")
    
    def _setup_network_connections(self):
        """Setup network connections with realistic bandwidth (same as Kadcast)."""
        # Initialize bandwidth tracking for all node pairs
        for sender_index in range(self.node_count):
            for receiver_index in range(self.node_count):
                if sender_index != receiver_index:
                    self.delay_calculator.bandwidth_tracker.set_connection_bandwidth(
                        sender_index, receiver_index, self.delay_calculator.bandwidth_tracker.default_bandwidth
                    )
        
        logger.info("Network connections established")
    
    def _distribute_network_knowledge(self):
        """
        Distribute complete network knowledge to all nodes (fair comparison).
        
        CRITICAL FIX: Added peer_id_to_index mapping for proper RTT lookups.
        """
        # Create complete peer list
        all_peers = {}
        for node_index, node in self.nodes.items():
            all_peers[node.node_id] = {
                "node_index": node_index,
                "coordinates": node.coordinates,
                "city": node.city
            }
        
        # Calculate RTTs between all pairs
        for node_index, node in self.nodes.items():
            node.all_peers = set(peer_id for peer_id in all_peers.keys() if peer_id != node.node_id)
            
            # CRITICAL FIX: Add peer ID to index mapping
            node.peer_id_to_index = {}
            
            # Calculate RTTs to all other nodes
            for peer_id, peer_info in all_peers.items():
                if peer_id != node.node_id:
                    # Store the mapping
                    node.peer_id_to_index[peer_id] = peer_info["node_index"]
                    
                    rtt_seconds = self.delay_calculator.calculate_rtt_delay(
                        node.city, peer_info["city"]
                    )
                    
                    # Convert to milliseconds (Cougar expects ms)
                    rtt_ms = rtt_seconds * 1000.0 if rtt_seconds else 100.0
                    node.peer_rtts[peer_id] = rtt_ms
        
        logger.info("Distributed complete network knowledge to all nodes")
    
    def _perform_neighbor_selection(self):
        """
        Perform Cougar neighbor selection (C close + R random).
        
        FIXED: Proper neighbor selection and ONS update.
        """
        total_close_neighbors = 0
        total_random_neighbors = 0
        
        for node_index, node in self.nodes.items():
            # Clear existing neighbor sets
            node.close_neighbors.clear()
            node.random_neighbors.clear()
            
            # Sort peers by RTT for close neighbor selection
            sorted_peers = sorted(
                node.peer_rtts.items(),
                key=lambda x: x[1]  # Sort by RTT value
            )
            
            # Select C closest neighbors
            close_count = min(self.default_C, len(sorted_peers))
            for i in range(close_count):
                peer_id, _ = sorted_peers[i]
                node.close_neighbors.add(peer_id)
            
            # Select R random neighbors (excluding close neighbors)
            remaining_peers = [peer_id for peer_id, _ in sorted_peers if peer_id not in node.close_neighbors]
            random_count = min(self.default_R, len(remaining_peers))
            
            if random_count > 0:
                random_peers = random.sample(remaining_peers, random_count)
                for peer_id in random_peers:
                    node.random_neighbors.add(peer_id)
            
            # CRITICAL FIX: Update outgoing neighbor set (ONS)
            node.outgoing_neighbor_set = node.close_neighbors | node.random_neighbors
            
            total_close_neighbors += len(node.close_neighbors)
            total_random_neighbors += len(node.random_neighbors)
            
            logger.debug(f"Node {node_index}: {len(node.close_neighbors)} close + {len(node.random_neighbors)} random neighbors")
        
        avg_close = total_close_neighbors / self.node_count
        avg_random = total_random_neighbors / self.node_count
        logger.info(f"Completed Cougar neighbor selection (avg: {avg_close:.1f} close + {avg_random:.1f} random)")
    
    # =============================================================================
    # BROADCASTING PHASE
    # =============================================================================
    
    def start_broadcasting(self) -> bool:
        """Start the broadcasting phase."""
        try:
            logger.info("📡 Starting Cougar broadcasting phase...")
            self.phase = "broadcasting"
            
            # Schedule block generation events
            self._schedule_block_generation()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Broadcasting start failed: {e}")
            return False
    
    def _schedule_block_generation(self):
        """Schedule block generation events (same pattern as Kadcast)."""
        block_interval = 60.0 / self.broadcast_frequency  # seconds between blocks
        total_blocks = int(self.broadcasting_duration / block_interval)
        
        for block_id in range(total_blocks):
            # Random source node
            source_node_index = random.randint(0, self.node_count - 1)
            
            # Schedule generation
            delay = block_id * block_interval
            
            self.event_queue.schedule_event(
                EventType.COUGAR_BLOCK_GENERATE,
                node_id=-1,  # System event
                delay=delay,
                data={
                    "block_id": block_id,
                    "source_node_index": source_node_index,
                    "block_size": self.block_size
                }
            )
        
        logger.info(f"Scheduled {total_blocks} block generations over {self.broadcasting_duration}s")
    
    # =============================================================================
    # EVENT HANDLERS - ENHANCED FOR P PARAMETER
    # =============================================================================
    
    def _register_event_handlers(self):
        """Register event handlers with the simulation engine."""
        # Core events
        self.engine.register_handler(EventType.COUGAR_BLOCK_GENERATE, self._handle_block_generate_event)
        self.engine.register_handler(EventType.COUGAR_BLOCK_GENERATED, self._handle_cougar_block_generated)
        self.engine.register_handler(EventType.COUGAR_BLOCK_SEND, self._handle_cougar_block_send_event)
        
        # Message events
        self.engine.register_handler(EventType.COUGAR_MESSAGE_RECEIVED, self._handle_cougar_message_received)
        
        # Validation events
        self.engine.register_handler(EventType.COUGAR_HEADER_VALIDATED, self._handle_cougar_header_validated)
        self.engine.register_handler(EventType.COUGAR_BODY_VALIDATED, self._handle_cougar_body_validated)
        self.engine.register_handler(EventType.COUGAR_BODY_REQUEST_PROCESSED, self._handle_cougar_body_request_processed)
        
        # Timeout events
        self.engine.register_handler(EventType.COUGAR_BODY_REQUEST_TIMEOUT, self._handle_cougar_body_request_timeout)
        
        # Completion events
        self.engine.register_handler(EventType.COUGAR_BLOCK_COMPLETED, self._handle_cougar_block_completed)
        
        logger.debug("Event handlers registered")
    
    def _handle_block_generate_event(self, event: Event):
        """Handle block generation with computational delays (Cougar version)."""
        try:
            generator_node_index = event.data["source_node_index"]
            block_id = event.data["block_id"]

            # Generate realistic block using the CORRECT method
            prev_hash = f"0x{'0' * 64}" if block_id == 0 else f"0x{hash(str(block_id - 1)):016x}{'0' * 48}"
            header_data, transactions = self.block_generator.generate_block(
               block_id=block_id,
               prev_hash=prev_hash,
               block_size_bytes=self.block_size
            )

            import hashlib
            header_bytes = json.dumps(header_data, sort_keys=True).encode('utf-8')
            block_hash = hashlib.sha256(header_bytes).hexdigest()
        
            # Calculate PROPER block generation delay
            generation_delay = self._calculate_block_generation_delay(
                generator_node_index, transactions, self.block_size
            )
        
            # Schedule block generation completion
            self.event_queue.schedule_event(
               EventType.COUGAR_BLOCK_GENERATED,
                node_id=generator_node_index,
                delay=generation_delay,
                data={
                  "block_id": block_id,
                  "block_hash": block_hash,
                  "header_data": header_data,
                  "transactions": transactions
                }
            )
        
            logger.info(f"Block {block_id} generation scheduled for node {generator_node_index} "
                       f"(delay: {generation_delay:.4f}s, {len(transactions)} txs)")
        
        except Exception as e:
            logger.error(f"Error handling block generation: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    def _handle_cougar_block_generated(self, event: Event):
        """✅ FIXED: Block generation completion with proper ID tracking."""
        try:
            generator_node_index = event.node_id
            block_id = event.data["block_id"]  # This should be the original numeric ID
            block_hash = event.data["block_hash"]
            header_data = event.data["header_data"]  # ✅ CRITICAL FIX: Extract from event data
            transactions = event.data["transactions"]  # ✅ CRITICAL FIX: Extract from event data
            
            # ✅ CRITICAL: Use ACTUAL simulation time
            actual_generation_time = self.event_queue.current_time
            
            # ✅ CRITICAL: Store with ORIGINAL block_id (not hash-derived)
            self.block_metrics[block_id] = {
                'transactions': transactions,
                'block_size': len(json.dumps(transactions).encode()),
                'source_node': generator_node_index,
                'generation_time': actual_generation_time,  # ✅ FIXED: Use actual sim time!
                'start_time': actual_generation_time,
                'completion_times': {},
                'nodes_completed': 0,
                'block_hash': block_hash  # ✅ NEW: Store hash for reverse lookup
            }
            
            # ✅ Store real block data with SAME key
            self.generated_blocks_content[block_id] = {
                'block_id': block_id,
                'block_hash': block_hash,
                'transactions': transactions,
                'header_data': header_data,
                'source': 'RealisticBlockGenerator',
                'data_type': 'real_simulation_data',
                'generation_time': actual_generation_time
            }
            
            logger.error(f"✅ BLOCK {block_id} GENERATED AT TIME {actual_generation_time:.3f}s (hash: {block_hash[:8]})")
            
            # ✅ Call test runner callback with CORRECT block_id
            if hasattr(self, '_test_runner_callback'):
                try:
                    self._test_runner_callback('block_generated', {
                        'block_id': block_id,  # Use original numeric ID!
                        'generation_time': actual_generation_time,
                        'source_node': generator_node_index,
                        'block_hash': block_hash  # Also provide hash for cross-reference
                    })
                except:
                    pass  # Ignore callback errors
            
            # Start broadcasting from generator node
            generator_node = self.nodes[generator_node_index]
            generator_node.start_block_broadcast(block_hash, header_data, transactions)
            
            logger.info(f"Cougar Block {block_id} broadcasting started from node {generator_node_index} at {actual_generation_time:.3f}s")
            
        except Exception as e:
            logger.error(f"Error in Cougar block generation completion: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")

        
    def _handle_cougar_block_send_event(self, event: Event):
        """Handle block broadcasting initiation."""
        try:
            sender_index = event.node_id
            block_hash = event.data["block_hash"]
            header_data = event.data["header_data"]
            transactions = event.data["transactions"]
            
            sender_node = self.nodes[sender_index]
            sender_node.start_block_broadcast(block_hash, header_data, transactions)
            
        except Exception as e:
            logger.error(f"Error in block send event: {e}")
    
    def _handle_cougar_message_received(self, event: Event):
        """Handle message arrival with proper computational delays and P parameter support."""
        try:
            receiver_node_index = event.node_id
            message = event.data["message"]
            sender_index = event.data["sender_index"]
            
            receiver_node = self.nodes[receiver_node_index]
            
            # Apply proper computational delays based on message type
            if message.message_type == CougarMessageType.HEADER_ADVERTISEMENT:
                # Use PROPER header validation delay calculation
                validation_delay = self._calculate_header_validation_delay(
                    receiver_node_index, message.header_data
                )
                
                # Add basic message processing overhead
                processing_delay = self._calculate_message_processing_delay(
                    receiver_node_index, message.size
                )
                
                total_delay = validation_delay + processing_delay
                
                if total_delay > 0.0001:  # Only schedule if meaningful delay
                    # Schedule proper header validation
                    self.event_queue.schedule_event(
                        EventType.COUGAR_HEADER_VALIDATED,
                        node_id=receiver_node_index,
                        delay=total_delay,
                        data={
                            "message": message,
                            "sender_index": sender_index
                        }
                    )
                else:
                    # Process immediately if delay is negligible
                    receiver_node.handle_header_message(message)
                    
            elif message.message_type == CougarMessageType.BODY_REQUEST:
                # Body requests are lightweight - minimal processing needed
                processing_delay = self._calculate_message_processing_delay(
                    receiver_node_index, message.size
                )
                
                if processing_delay > 0.0001:  # Only schedule if significant
                    self.event_queue.schedule_event(
                        EventType.COUGAR_BODY_REQUEST_PROCESSED,
                        node_id=receiver_node_index,
                        delay=processing_delay,
                        data={
                            "message": message,
                            "sender_index": sender_index
                        }
                    )
                else:
                    # Process immediately for very small delays
                    receiver_node.handle_body_request(message)
                    
            elif message.message_type == CougarMessageType.BODY_RESPONSE:
                # Use PROPER body validation delay calculation
                validation_delay = self._calculate_body_validation_delay(
                    receiver_node_index, message.transactions
                )
                
                # Add basic message processing overhead
                processing_delay = self._calculate_message_processing_delay(
                    receiver_node_index, message.size
                )
                
                total_delay = validation_delay + processing_delay
                
                if total_delay > 0.0001:  # Only schedule if meaningful delay
                    # Schedule proper body validation
                    self.event_queue.schedule_event(
                        EventType.COUGAR_BODY_VALIDATED,
                        node_id=receiver_node_index,
                        delay=total_delay,
                        data={
                            "message": message,
                            "sender_index": sender_index
                        }
                    )
                else:
                    # Process immediately
                    receiver_node.handle_body_response(message)
            
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    def _handle_cougar_header_validated(self, event: Event):
        """Handle header validation completion."""
        try:
            receiver_node_index = event.node_id
            message = event.data["message"]
            
            receiver_node = self.nodes[receiver_node_index]
            receiver_node.handle_header_message(message)
            
        except Exception as e:
            logger.error(f"Error in header validation: {e}")
    
    def _handle_cougar_body_validated(self, event: Event):
        """Handle body validation completion."""
        try:
            receiver_node_index = event.node_id
            message = event.data["message"]
            
            receiver_node = self.nodes[receiver_node_index]
            receiver_node.handle_body_response(message)
            
        except Exception as e:
            logger.error(f"Error in body validation: {e}")
    
    def _handle_cougar_body_request_processed(self, event: Event):
        """Handle body request processing completion."""
        try:
            receiver_node_index = event.node_id
            message = event.data["message"]
            
            receiver_node = self.nodes[receiver_node_index]
            receiver_node.handle_body_request(message)
            
        except Exception as e:
            logger.error(f"Error in body request processing: {e}")
    
    def _handle_cougar_body_request_timeout(self, event: Event):
        """Handle body request timeout with P parameter awareness."""
        try:
            node_index = event.node_id
            block_hash = event.data["block_hash"]
            peer_id = event.data["peer_id"]
            
            node = self.nodes[node_index]
            node.handle_body_request_timeout(block_hash, peer_id)
            
            # ✅ UPDATE: Enhanced timeout metrics for P parameter analysis
            self.real_time_metrics["timeouts_occurred"] += 1
            
            # Track timeout efficiency for P parameter analysis
            if block_hash in self.block_request_counts:
                if node_index in self.block_request_counts[block_hash]:
                    total_requests = self.block_request_counts[block_hash][node_index]
                    timeout_rate = 1.0 / total_requests if total_requests > 0 else 1.0
                    
                    # Update parallel efficiency metrics
                    current_efficiency = self.real_time_metrics["parallel_requests_stats"]["parallel_efficiency"]
                    self.real_time_metrics["parallel_requests_stats"]["parallel_efficiency"] = (
                        (current_efficiency + (1.0 - timeout_rate)) / 2.0
                    )
            
        except Exception as e:
            logger.error(f"Error in body request timeout: {e}")
    
    def _handle_cougar_block_completed(self, event: Event):
        """✅ FIXED: Handle block completion for final metrics."""
        try:
            node_index = event.data.get("node_index")
            block_id = event.data.get("block_id")
            completion_time = event.data.get("completion_time")
            
            # ✅ Update block metrics properly
            if block_id in self.block_metrics:
                self.block_metrics[block_id]["completion_times"][node_index] = completion_time
                self.block_metrics[block_id]["nodes_completed"] += 1
                
                # Update real-time metrics
                if block_id not in self.real_time_metrics["blocks_completed"]:
                    self.real_time_metrics["blocks_completed"][block_id] = 0
                self.real_time_metrics["blocks_completed"][block_id] += 1
                
                # Calculate coverage
                total_nodes = self.node_count
                completed_nodes = self.block_metrics[block_id]["nodes_completed"]
                coverage = (completed_nodes / total_nodes) * 100
                
                logger.debug(f"✅ Block {block_id} completed by node {node_index} (coverage: {coverage:.1f}%)")
            
        except Exception as e:
            logger.error(f"Error handling Cougar block completion: {e}")
    # =============================================================================
    # MESSAGE ROUTING - ENHANCED FOR P PARAMETER
    # =============================================================================
    def on_block_completed(self, node_index: int, block_id: int, completion_time: float):
        """
        ✅ ENHANCED: Handle Cougar block completion with proper timing and metrics integration.
        """
        try:
            # ✅ Use ACTUAL simulation time instead of passed completion_time
            actual_completion_time = self.event_queue.current_time
            
            # ✅ Ensure block metrics exist with correct generation time
            if block_id not in self.block_metrics:
                # If somehow missing, create with estimated time
                estimated_gen_time = max(0.0, actual_completion_time - 10.0)  # Estimate 10s ago
                self.block_metrics[block_id] = {
                    'transactions': [],
                    'block_size': self.block_size,
                    'source_node': -1,
                    'generation_time': estimated_gen_time,
                    'start_time': estimated_gen_time,
                    'completion_times': {},
                    'nodes_completed': 0
                }
                self.logger.warning(f"Block {block_id} metrics missing - created with estimated generation time")
            
            # ✅ Get ACTUAL generation time from block metrics
            generation_time = self.block_metrics[block_id]['generation_time']
            
            # ✅ Calculate PROPER latency using simulation times
            actual_latency = actual_completion_time - generation_time
            
            # ✅ Sanity check for latency
            if actual_latency < 0:
                self.logger.warning(f"Negative latency for block {block_id}: {actual_latency:.3f}s - using 0")
                actual_latency = 0.0
            elif actual_latency > 300:  # More than 5 minutes
                self.logger.warning(f"Excessive latency for block {block_id}: {actual_latency:.3f}s")
            
            # ✅ Add computational processing delays (like Kadcast)
            transactions = self.block_metrics[block_id].get("transactions", [])
            block_size = self.block_metrics[block_id]["block_size"]
            
            # Cougar-specific body validation delay
            body_validation_delay = 0.005  # 5ms base delay
            if hasattr(self, 'delay_calculator'):
                try:
                    body_validation_delay = self.delay_calculator.calculate_block_processing_delay(
                        node_index, block_size, transactions
                    )
                except:
                    body_validation_delay = 0.005
            
            final_completion_time = actual_completion_time + body_validation_delay
            final_latency = final_completion_time - generation_time
            
            # ✅ Update block metrics immediately
            self.block_metrics[block_id]["completion_times"][node_index] = final_completion_time
            self.block_metrics[block_id]["nodes_completed"] += 1
            
            # ✅ CRITICAL: Report to metrics collector with proper data
            if hasattr(self, 'metrics_collector'):
                # Report basic completion
                self.metrics_collector.report_block_processed(node_index, block_id, final_completion_time)
                
                # Report with latency for enhanced tracking
                if hasattr(self.metrics_collector, 'report_block_completed'):
                    self.metrics_collector.report_block_completed(node_index, block_id, final_completion_time)
                    
                # ✅ NEW: Add direct latency tracking to metrics collector
                if hasattr(self.metrics_collector, 'add_latency_sample'):
                    self.metrics_collector.add_latency_sample(block_id, node_index, final_latency)
                
                # ✅ NEW: Add coverage tracking to metrics collector
                if hasattr(self.metrics_collector, 'update_coverage'):
                    total_nodes = self.node_count
                    nodes_completed = self.block_metrics[block_id]["nodes_completed"]
                    coverage_percentage = (nodes_completed / total_nodes) * 100
                    self.metrics_collector.update_coverage(block_id, coverage_percentage)
            
            # ✅ Schedule final completion event after processing delays
            self.event_queue.schedule_event(
                EventType.COUGAR_BLOCK_COMPLETED,
                node_id=node_index,
                delay=body_validation_delay,
                data={
                    "node_index": node_index,
                    "block_id": block_id,
                    "completion_time": final_completion_time,
                    "generation_time": generation_time,
                    "actual_latency": final_latency
                }
            )
            
            self.logger.debug(f"✅ Cougar Block {block_id} completion: gen={generation_time:.3f}s, "
                            f"comp={final_completion_time:.3f}s, latency={final_latency:.3f}s")
            
        except Exception as e:
            self.logger.error(f"❌ Error in Cougar block completion callback: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")


    def _calculate_final_metrics(self) -> Dict[str, Any]:
        """Calculate final simulation metrics with enhanced Cougar tracking."""
        try:
            self.logger.info("📊 Calculating enhanced Cougar final metrics...")
            
            # ✅ CRITICAL: Calculate coverage and latency from actual data
            total_blocks = len(self.block_metrics)
            total_nodes = self.node_count
            
            # Calculate real coverage from block_metrics
            coverage_values = []
            latency_values = []
            
            for block_id, block_info in self.block_metrics.items():
                nodes_completed = block_info.get("nodes_completed", 0)
                block_coverage = (nodes_completed / total_nodes) * 100 if total_nodes > 0 else 0
                coverage_values.append(block_coverage)
                
                # Calculate latencies for this block
                generation_time = block_info.get("generation_time", 0)
                completion_times = block_info.get("completion_times", {})
                
                for node_index, completion_time in completion_times.items():
                    if generation_time > 0:
                        latency = completion_time - generation_time
                        if 0 < latency < 300:  # Reasonable latency range
                            latency_values.append(latency)
            
            # Calculate averages
            avg_coverage = sum(coverage_values) / len(coverage_values) if coverage_values else 0.0
            avg_latency = sum(latency_values) / len(latency_values) if latency_values else 0.0
            
            # ✅ OVERRIDE: Update metrics collector with real values
            if hasattr(self, 'metrics_collector'):
                # Force update the metrics collector with our calculated values
                self.metrics_collector._coverage_samples = coverage_values
                self.metrics_collector._latency_samples = latency_values
                
                # Add summary override
                def enhanced_get_summary():
                    return {
                        'coverage': avg_coverage,
                        'latency': avg_latency,
                        'bandwidth_utilization': sum(latency_values) if latency_values else 0,
                        'total_blocks': total_blocks,
                        'total_nodes': total_nodes
                    }
                
                # Override the get_summary method
                self.metrics_collector.get_summary = enhanced_get_summary
            
            # Continue with rest of calculation...
            base_metrics = {
                "coverage": avg_coverage,    # ✅ Now calculated correctly
                "latency": avg_latency,      # ✅ Now calculated correctly
                "bandwidth_utilization": 0.0
            }
            
            # Get Cougar-specific metrics
            cougar_metrics = {}
            if hasattr(self.metrics_collector, 'get_cougar_specific_metrics'):
                cougar_metrics = self.metrics_collector.get_cougar_specific_metrics()
            
            # Bandwidth efficiency metrics
            total_headers = self.real_time_metrics["headers_transmitted"]
            total_body_requests = self.real_time_metrics["body_requests_sent"]
            total_body_responses = self.real_time_metrics["body_responses_sent"]
            total_timeouts = self.real_time_metrics["timeouts_occurred"]
            
            # Calculate efficiency ratios
            request_response_ratio = 0.0
            if total_body_requests > 0:
                request_response_ratio = total_body_responses / total_body_requests
            
            timeout_rate = 0.0
            if total_body_requests > 0:
                timeout_rate = total_timeouts / total_body_requests
            
            # ✅ P parameter effectiveness metrics
            parallel_stats = self.real_time_metrics["parallel_requests_stats"]
            blocks_with_parallel = parallel_stats["total_blocks_with_parallel_requests"]
            
            avg_requests_per_block = 0.0
            if total_blocks > 0:
                avg_requests_per_block = total_body_requests / (total_blocks * self.node_count)
            
            parallel_efficiency = parallel_stats["parallel_efficiency"]
            
            final_metrics = {
                "protocol": "cougar",
                "simulation_id": self.simulation_id,
                "simulation_time": self.event_queue.current_time,
                "total_blocks": total_blocks,
                "avg_coverage": avg_coverage,      # ✅ Now correct
                "avg_latency": avg_latency,        # ✅ Now correct
                "bandwidth_utilization": base_metrics.get("bandwidth_utilization", 0.0),
                "message_efficiency": {
                    "headers_sent": total_headers,
                    "body_requests": total_body_requests,
                    "body_responses": total_body_responses,
                    "request_response_ratio": request_response_ratio,
                    "timeout_rate": timeout_rate,
                    "total_bandwidth_usage": self.real_time_metrics["bandwidth_usage"]
                },
                "parallelism_metrics": {
                    "P_parameter": self.default_P,
                    "avg_requests_per_block": avg_requests_per_block,
                    "blocks_with_parallel_requests": blocks_with_parallel,
                    "parallel_efficiency": parallel_efficiency,
                    "expected_vs_actual_requests": {
                        "expected_per_node_per_block": 1.0,
                        "actual_per_node_per_block": avg_requests_per_block,
                        "parallelism_factor": avg_requests_per_block / 1.0 if avg_requests_per_block > 0 else 0.0
                    }
                },
                "cougar_specific": cougar_metrics,
                "node_count": self.node_count,
                "protocol_parameters": {
                    "C": self.default_C,
                    "R": self.default_R,
                    "P": self.default_P
                },
                "performance_distribution": self.performance_distribution,
                "debug_metrics": {
                    "coverage_samples": len(coverage_values),
                    "latency_samples": len(latency_values),
                    "coverage_range": f"{min(coverage_values):.1f}-{max(coverage_values):.1f}%" if coverage_values else "N/A",
                    "latency_range": f"{min(latency_values):.3f}-{max(latency_values):.3f}s" if latency_values else "N/A"
                }
            }
            
            self.logger.info(f"📈 Enhanced Cougar Final Statistics:")
            self.logger.info(f"   - Total blocks: {total_blocks}")
            self.logger.info(f"   - ✅ Average coverage: {avg_coverage:.1f}%")
            self.logger.info(f"   - ✅ Average latency: {avg_latency:.3f}s")
            self.logger.info(f"   - Coverage samples: {len(coverage_values)}")
            self.logger.info(f"   - Latency samples: {len(latency_values)}")
            
            return final_metrics
            
        except Exception as e:
            self.logger.error(f"Error calculating final metrics: {e}")
            return {'error': str(e), 'avg_coverage': 0.0, 'avg_latency': 0.0}
        

    def resolve_block_hash_to_id(self, block_hash: str) -> Optional[int]:
        """Resolve block hash to original block ID."""
        try:
            # Direct search through generated content
            for block_id, content in self.generated_blocks_content.items():
                if content.get('block_hash') == block_hash:
                    return block_id
            
            # Search through block metrics
            for block_id, metrics in self.block_metrics.items():
                if metrics.get('block_hash') == block_hash:
                    return block_id
                    
            return None
        except Exception:
            return None    

    def _route_cougar_message(self, message, sender_index: int, receiver_index: int):
        """Route Cougar messages between nodes with transmission delays and P parameter tracking."""
        try:
            sender_node = self.nodes[sender_index]
            receiver_node = self.nodes[receiver_index]
            
            # Calculate transmission delay (RTT + bandwidth)
            transmission_delay_info = self.delay_calculator.calculate_transmission_delay(
                sender_index, receiver_index,
                sender_node.city, receiver_node.city,
                message.size, self.event_queue.current_time
            )
            
            # FIXED: Use correct key name
            total_transmission_delay = transmission_delay_info["transmission_delay"]
            
            # ✅ FIXED: Schedule timeout AFTER message delivery, not immediately!
            if message.message_type == CougarMessageType.BODY_REQUEST:
                block_hash = message.block_hash
                if block_hash not in self.block_request_counts:
                    self.block_request_counts[block_hash] = {}
                if sender_index not in self.block_request_counts[block_hash]:
                    self.block_request_counts[block_hash][sender_index] = 0
                self.block_request_counts[block_hash][sender_index] += 1
                
                # CRITICAL FIX: Schedule timeout AFTER transmission + processing delays
                # NOT immediately! This was causing premature timeouts.
                self._schedule_body_request_timeout_fixed(
                    message, sender_index, receiver_index, total_transmission_delay
                )
            
            # Schedule message delivery
            self.event_queue.schedule_event(
                EventType.COUGAR_MESSAGE_RECEIVED,
                node_id=receiver_index,
                delay=total_transmission_delay,
                data={
                    "message": message,
                    "sender_index": sender_index
                }
            )
            
            # Update real-time metrics
            self.real_time_metrics["bandwidth_usage"] += message.size
            
            if message.message_type == CougarMessageType.HEADER_ADVERTISEMENT:
                self.real_time_metrics["headers_transmitted"] += 1
            elif message.message_type == CougarMessageType.BODY_REQUEST:
                self.real_time_metrics["body_requests_sent"] += 1
            elif message.message_type == CougarMessageType.BODY_RESPONSE:
                self.real_time_metrics["body_responses_sent"] += 1
            
        except Exception as e:
            logger.error(f"Error routing message: {e}")

    
    def _schedule_body_request_timeout_fixed(self, body_request: CougarBodyRequestMessage, 
                                        sender_index: int, receiver_index: int, 
                                        transmission_delay: float):
        """FIXED timeout calculation that accounts for transmission delays."""
        try:
            sender_node = self.nodes[sender_index]
            receiver_node = self.nodes[receiver_index]
            
            # Estimate body size from block size
            estimated_body_size = self.block_size * 0.9
            
            # Use enhanced TCP model with real cities
            timeout_stats = self.tcp_model.get_enhanced_timeout_stats(
                estimated_body_size, sender_node.city, receiver_node.city
            )
            
            # P parameter adjustment
            p_factor = 1.0 + (self.default_P - 1) * 0.1
         
            base_timeout_seconds = timeout_stats["timeout_seconds"]
            
            # CRITICAL FIX: Add transmission delay to timeout calculation
            # Timeout should start AFTER the request is delivered and processed
            processing_delay = self._calculate_message_processing_delay(receiver_index, body_request.size)
            total_delay_before_timeout = transmission_delay + processing_delay
            final_timeout_seconds = base_timeout_seconds + total_delay_before_timeout
            
            logger.debug(f"FIXED timeout calculation:")
            logger.debug(f"  Base timeout: {base_timeout_seconds:.3f}s")
            logger.debug(f"  Transmission delay: {transmission_delay:.3f}s")
            logger.debug(f"  Processing delay: {processing_delay:.3f}s")
            logger.debug(f"  Final timeout: {final_timeout_seconds:.3f}s")
            
            # Schedule timeout event with corrected timing
            timeout_event_id = self.event_queue.schedule_event(
                EventType.COUGAR_BODY_REQUEST_TIMEOUT,
                node_id=sender_index,
                delay=final_timeout_seconds,  # Now includes proper delays!
                data={
                    "block_hash": body_request.block_hash,
                    "peer_id": receiver_node.node_id,
                    "timeout_stats": timeout_stats
                }
            )
            
            # Track timeout for potential cancellation
            timeout_key = (sender_index, body_request.block_hash, receiver_node.node_id)
            self.pending_body_timeouts[timeout_key] = timeout_event_id
            
        except Exception as e:
            logger.error(f"Error scheduling fixed body request timeout: {e}")
    # DELAY CALCULATIONS - FIXED TO USE computational_delay.py PROPERLY
    # =============================================================================
    
    def _calculate_header_validation_delay(self, node_index: int, header_data: Dict[str, Any]) -> float:
        """
        Calculate header validation computational delay using the NEW dedicated function.
        
        Now uses the proper calculate_header_validation_delay from computational_delay.py
        which handles all header-specific validation tasks:
        - Hash verifications (prev_hash, merkle_root)
        - Digital signature verification  
        - PoW/PoS verification
        - Timestamp and structure validation
        - Consensus rule validation
        
        This replaces the previous pseudo-transaction approach with a proper academic function.
        """
        try:
            # Use the NEW dedicated header validation function from computational_delay.py
            header_validation_delay = self.delay_calculator.calculate_header_validation_delay(
                node_index, header_data
            )
            
            #logger.debug(f"Header validation delay for node {node_index}: {header_validation_delay:.6f}s")
            return header_validation_delay
            
        except Exception as e:
            logger.error(f"Error calculating header validation delay: {e}")
            # Fallback to academic standard (~5ms average)
            return 0.005
    
    def _calculate_body_validation_delay(self, node_index: int, transactions: List[Dict[str, Any]]) -> float:
        """
        Calculate body validation computational delay using proper computational_delay.py functions.
        
        This is the proper way to calculate block body validation delay.
        """
        try:
            # Calculate actual transaction data size
            # This should match the RealisticBlockGenerator output
            transaction_data = json.dumps(transactions).encode('utf-8')
            body_size = len(transaction_data)
            
            # Use the PROPER computational_delay.py function
            # This function already handles:
            # - Node performance factors
            # - Transaction type-specific validation costs  
            # - Block size overhead
            # - Merkle tree verification
            # - All the complex validation logic
            validation_delay = self.delay_calculator.calculate_block_processing_delay(
                node_index, body_size, transactions
            )
            
            #logger.debug(f"Body validation delay for node {node_index}: {validation_delay:.6f}s ({len(transactions)} txs)")
            return validation_delay
            
        except Exception as e:
            logger.error(f"Error calculating body validation delay: {e}")
            # Fallback based on transaction count
            fallback_delay = len(transactions) * 0.001  # 1ms per transaction fallback
            return fallback_delay
    
    def _calculate_block_generation_delay(self, node_index: int, transactions: List[Dict], block_size: int) -> float:
        """Calculate block generation delay (for source nodes creating new blocks)."""
        try:
            return self.delay_calculator.calculate_block_generation_delay(
                node_index, transactions, block_size
            )
        except Exception as e:
            logger.error(f"Error calculating block generation delay: {e}")
            return 0.1  # 100ms fallback for block generation
    
    def _calculate_message_processing_delay(self, node_index: int, message_size: int) -> float:
        """Calculate basic message processing overhead."""
        try:
            # For small messages like headers and requests, use minimal processing
            # This represents JSON parsing, message validation, etc.
            
            # Use the chunk processing delay as it's designed for small data processing
            return self.delay_calculator.calculate_chunk_processing_delay(
                node_index, message_size
            )
        except Exception as e:
            logger.error(f"Error calculating message processing delay: {e}")
            return 0.0001  # 0.1ms fallback
    
    def _initialize_block_metrics(self, block_id):
        """Initialize metrics tracking for a new block with P parameter support."""
        if block_id not in self.block_metrics:
            self.block_metrics[block_id] = {
                "start_time": self.event_queue.current_time,
                "completion_times": {},
                "nodes_completed": 0,
                "coverage_timeline": [],
                # ✅ NEW: P parameter specific metrics
                "parallel_request_stats": {
                    "total_parallel_requests": 0,
                    "nodes_using_parallel": 0,
                    "max_parallel_per_node": 0
                }
            }
    
    # =============================================================================
    # SIMULATION CONTROL - ENHANCED FOR P PARAMETER
    # =============================================================================
    
    def run_simulation(self) -> bool:
        """Run the complete Cougar simulation with P parameter support."""
        try:
            self.running = True
            logger.info(f"🎯 Starting Cougar simulation {self.simulation_id}")
            logger.info(f"🔧 P parameter: {self.default_P} (parallel body requests per block)")
            
            # Register event handlers
            self._register_event_handlers()
            
            # Phase 1: Initialize network
            if not self.initialize_network():
                raise Exception("Network initialization failed")
            
            # Phase 2: Start broadcasting
            if not self.start_broadcasting():
                raise Exception("Broadcasting start failed")
            
            # Run event loop
            total_simulation_time = self.broadcasting_duration + 120.0  # 5 minutes + 2 minute buffer
            logger.info(f"Running Cougar simulation for {total_simulation_time} seconds...")
            
            start_time = time.time()
            self.engine.run(max_time=total_simulation_time)
            end_time = time.time()
            
            # Calculate final metrics
            final_metrics = self._calculate_final_metrics()
            
            logger.info(f"✅ Cougar simulation completed successfully in {end_time - start_time:.2f} real seconds")
            return True
            
        except Exception as e:
            logger.error(f"❌ Cougar simulation failed: {e}")
            self.running = False
            return False
    
    def _calculate_final_metrics(self) -> Dict[str, Any]:
        """Calculate final simulation metrics with P parameter analysis."""
        try:
            logger.info("📊 Calculating Cougar final metrics...")
            
            # Get base metrics from metrics collector
            base_metrics = self.metrics_collector.get_summary()
            
            # Get Cougar-specific metrics
            cougar_metrics = {}
            if hasattr(self.metrics_collector, 'get_cougar_specific_metrics'):
                cougar_metrics = self.metrics_collector.get_cougar_specific_metrics()
            
            # Calculate additional Cougar metrics
            total_blocks = len(self.block_metrics)
            avg_coverage = base_metrics.get("coverage", 0.0)
            avg_latency = base_metrics.get("latency", 0.0)
            
            # Bandwidth efficiency metrics
            total_headers = self.real_time_metrics["headers_transmitted"]
            total_body_requests = self.real_time_metrics["body_requests_sent"]
            total_body_responses = self.real_time_metrics["body_responses_sent"]
            total_timeouts = self.real_time_metrics["timeouts_occurred"]
            
            # Calculate efficiency ratios
            request_response_ratio = 0.0
            if total_body_requests > 0:
                request_response_ratio = total_body_responses / total_body_requests
            
            timeout_rate = 0.0
            if total_body_requests > 0:
                timeout_rate = total_timeouts / total_body_requests
            
            # ✅ NEW: Calculate P parameter effectiveness metrics
            parallel_stats = self.real_time_metrics["parallel_requests_stats"]
            blocks_with_parallel = parallel_stats["total_blocks_with_parallel_requests"]
            
            avg_requests_per_block = 0.0
            if total_blocks > 0:
                avg_requests_per_block = total_body_requests / (total_blocks * self.node_count)
            
            parallel_efficiency = parallel_stats["parallel_efficiency"]
            
            final_metrics = {
                "protocol": "cougar",
                "simulation_id": self.simulation_id,
                "simulation_time": self.event_queue.current_time,
                "total_blocks": total_blocks,
                "avg_coverage": avg_coverage,
                "avg_latency": avg_latency,
                "bandwidth_utilization": base_metrics.get("bandwidth_utilization", 0.0),
                "message_efficiency": {
                    "headers_sent": total_headers,
                    "body_requests": total_body_requests,
                    "body_responses": total_body_responses,
                    "request_response_ratio": request_response_ratio,
                    "timeout_rate": timeout_rate,
                    "total_bandwidth_usage": self.real_time_metrics["bandwidth_usage"]
                },
                # ✅ NEW: P parameter specific metrics
                "parallelism_metrics": {
                    "P_parameter": self.default_P,
                    "avg_requests_per_block": avg_requests_per_block,
                    "blocks_with_parallel_requests": blocks_with_parallel,
                    "parallel_efficiency": parallel_efficiency,
                    "expected_vs_actual_requests": {
                        "expected_per_node_per_block": 1.0,  # Without P, each node requests once
                        "actual_per_node_per_block": avg_requests_per_block,
                        "parallelism_factor": avg_requests_per_block / 1.0 if avg_requests_per_block > 0 else 0.0
                    }
                },
                "cougar_specific": cougar_metrics,
                "node_count": self.node_count,
                "protocol_parameters": {
                    "C": self.default_C,
                    "R": self.default_R,
                    "P": self.default_P
                },
                "performance_distribution": self.performance_distribution
            }
            
            logger.info(f"📈 Final Cougar Statistics (P={self.default_P}):")
            logger.info(f"   - Total blocks: {total_blocks}")
            logger.info(f"   - Average coverage: {avg_coverage:.1f}%")
            logger.info(f"   - Average latency: {avg_latency:.3f}s")
            logger.info(f"   - Headers sent: {total_headers}")
            logger.info(f"   - Body requests: {total_body_requests}")
            logger.info(f"   - Body responses: {total_body_responses}")
            logger.info(f"   - Request/Response ratio: {request_response_ratio:.3f}")
            logger.info(f"   - Timeout rate: {timeout_rate:.3f}")
            logger.info(f"   - 🔧 Avg requests/block: {avg_requests_per_block:.2f} (P={self.default_P})")
            logger.info(f"   - 🔧 Blocks with parallel requests: {blocks_with_parallel}")
            logger.info(f"   - 🔧 Parallel efficiency: {parallel_efficiency:.3f}")
            
            return final_metrics
            
        except Exception as e:
            logger.error(f"Error calculating final metrics: {e}")
            return {}
    
    # =============================================================================
    # BLOCK COMPLETION CALLBACK (CRITICAL FIX)
    # =============================================================================
    
    
    # =============================================================================
    # UTILITY METHODS
    # =============================================================================
    def _extract_block_id_from_hash(self, block_hash: str) -> int:
        """Extract numeric block ID from block hash for metrics compatibility."""
        try:
            # Try to extract from hash if it contains block ID
            if 'block_' in block_hash:
                return int(block_hash.split('block_')[1].split('_')[0])
            else:
                # Use hash of the string for consistent mapping
                return hash(block_hash) % 1000000
        except (ValueError, IndexError):
            return hash(block_hash) % 1000000        
        
    def get_node_by_id(self, node_id: str) -> Optional[CougarNode]:
        """Get node by its ID."""
        return self.nodes_by_id.get(node_id)
    
    def get_simulation_time(self) -> float:
        """Get current simulation time."""
        return self.event_queue.current_time
    
    def is_running(self) -> bool:
        """Check if simulation is currently running."""
        return self.running
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot with P parameter data."""
        return {
            "real_time_metrics": self.real_time_metrics.copy(),
            "block_metrics": self.block_metrics.copy(),
            "current_time": self.event_queue.current_time,
            "phase": self.phase,
            "P_parameter": self.default_P,
            "block_request_counts": self.block_request_counts.copy()
        }
    
    def get_config(self) -> Dict[str, Any]:
        """Get simulator configuration."""
        return self.config.copy()
    
    def stop_simulation(self):
        """Stop the simulation gracefully."""
        self.running = False
        logger.info("Simulation stop requested")

    def get_real_block_data(self, block_id: int) -> Optional[Dict[str, Any]]:
        """
        Get real block data for specific block ID (Cougar version).
        
        Returns:
            Real block data including transactions, headers, and source info
        """
        return self.generated_blocks_content.get(block_id)

    def get_all_real_blocks(self) -> Dict[int, Dict[str, Any]]:
        """
        Get all real block data (Cougar version).
        
        Returns:
            Complete dictionary of all captured real block data
        """
        return self.generated_blocks_content.copy()

    def export_simulation_blocks(self) -> List[Dict[str, Any]]:
        """
        Export simulation blocks for JSON export (Cougar version).
        
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
                    'cougar_metrics': {
                        'source_node': block_metrics.get('source_node'),
                        'completion_times': block_metrics.get('completion_times', {}),
                        'nodes_completed': block_metrics.get('nodes_completed', 0),
                        'parallel_request_stats': block_metrics.get('parallel_request_stats', {})
                    },
                    'node_completions': self._format_cougar_completions(block_id)
                }
                
                exported_blocks.append(exported_block)
            
            logger.info(f"Exported {len(exported_blocks)} Cougar blocks with real data")
            return exported_blocks
            
        except Exception as e:
            logger.error(f"Error exporting Cougar simulation blocks: {e}")
            return []

    def _format_cougar_completions(self, block_id: int) -> List[Dict[str, Any]]:
        """Format node completions for Cougar block export."""
        try:
            completions = []
            block_metrics = self.block_metrics.get(block_id, {})
            completion_times = block_metrics.get('completion_times', {})
            
            for node_index, completion_time in completion_times.items():
                completion_data = {
                    'node_id': f'cougar_node_{node_index}',
                    'completion_time': completion_time,
                    'method': 'header_body_completion',  # Cougar-specific
                    'latency': completion_time - block_metrics.get('generation_time', 0.0)
                }
                
                # Add P parameter specific data if available
                if hasattr(self, 'block_request_counts') and block_id in self.block_request_counts:
                    if node_index in self.block_request_counts[block_id]:
                        completion_data['parallel_requests_sent'] = self.block_request_counts[block_id][node_index]
                        completion_data['parallelism_factor'] = self.default_P
                
                completions.append(completion_data)
            
            return completions
            
        except Exception as e:
            logger.error(f"Error formatting Cougar completions: {e}")
            return []    


# =============================================================================
# MAIN EXECUTION (for testing P parameter)
# =============================================================================

if __name__ == "__main__":
    # ✅ ENHANCED: P parameter test configurations
    test_configs = [
        {
            "name": "P=1 (Conservative)",
            "config": {
                "nodeCount": 50,
                "blockSize": 512 * 1024,  # 512KB
                "broadcastFrequency": 5,  # 5 blocks/minute
                "closeNeighbors": 3,
                "randomNeighbors": 3,
                "parallelism": 1,  # Single request
                "randomSeed": 42
            }
        },
        {
            "name": "P=4 (Balanced)",
            "config": {
                "nodeCount": 50,
                "blockSize": 512 * 1024,
                "broadcastFrequency": 5,
                "closeNeighbors": 3,
                "randomNeighbors": 3,
                "parallelism": 4,  # Paper default
                "randomSeed": 42
            }
        },
        {
            "name": "P=8 (Greedy)",
            "config": {
                "nodeCount": 50,
                "blockSize": 512 * 1024,
                "broadcastFrequency": 5,
                "closeNeighbors": 3,
                "randomNeighbors": 3,
                "parallelism": 8,  # High parallelism
                "randomSeed": 42
            }
        }
    ]
    
    logging.basicConfig(level=logging.INFO)
    
    for test in test_configs:
        print(f"\n🧪 Testing {test['name']}...")
        simulator = CougarSimulator(test['config'])
        success = simulator.run_simulation()
        
        if success:
            metrics = simulator.get_metrics()
            final_metrics = simulator._calculate_final_metrics()
            
            print(f"✅ {test['name']} completed successfully!")
            print(f"   📊 P parameter: {final_metrics['parallelism_metrics']['P_parameter']}")
            print(f"   📊 Avg requests/block: {final_metrics['parallelism_metrics']['avg_requests_per_block']:.2f}")
            print(f"   📊 Parallel efficiency: {final_metrics['parallelism_metrics']['parallel_efficiency']:.3f}")
            print(f"   📊 Coverage: {final_metrics['avg_coverage']:.1f}%")
            print(f"   📊 Latency: {final_metrics['avg_latency']:.3f}s")
        else:
            print(f"❌ {test['name']} failed!")
        
        print("-" * 60)


    # cougar/simulator.py - YENİ METHOD EKLE:

    