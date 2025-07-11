"""
Pure Cougar Protocol Node Implementation - ACADEMICALLY CORRECT VERSION

Implements the CougarNode class following the EXACT original Cougar protocol specification
from the DEBS '22 paper without any simulation-specific enhancements.

Key adherence to original protocol:
- Link placement: C (close) + R (random) neighbors based on RTT
- Header-first dissemination: Push header → Pull body model ONLY
- Parallelism: P parameter for concurrent body requests
- NO header forwarding on body request (not in original protocol)
- Pure academic implementation without simulation optimizations

This is the academically correct version that follows the protocol specification exactly.
"""

import random
import time
import logging
from typing import Dict, List, Set, Tuple, Optional, Any

from src.protocols.cougar.messages import (
    CougarHeaderMessage, CougarBodyRequestMessage, CougarBodyResponseMessage,
    CougarTCPModel, CougarMessageType
)

logger = logging.getLogger("CougarNode")


class CougarNode:
    """
    Pure Cougar protocol node implementation - ACADEMICALLY CORRECT VERSION.
    
    Follows the EXACT original Cougar protocol specification from DEBS '22 paper.
    No simulation enhancements or optimizations that deviate from the original algorithm.
    
    Core principles maintained:
    - Header forwarding ONLY on header reception (not on body request)
    - P parameter for parallel body requests
    - C+R neighbor selection strategy
    - Header-first dissemination model
    """
    
    def __init__(self, node_index: int, node_id: str = None, city: str = None,
                 coordinates: Tuple[float, float] = None, performance_type: str = "average_performance",
                 C: int = 4, R: int = 4, P: int = 4):
        """
        Initialize Pure Cougar node following original protocol specification.
        
        Args:
            node_index: Node index in simulation
            node_id: Unique node identifier
            city: Geographic location
            coordinates: (latitude, longitude)
            performance_type: Node performance category
            C: Close neighbors count (default: 4)
            R: Random neighbors count (default: 4) 
            P: Parallelism parameter for body requests (default: 4)
        """
        # Basic node properties
        self.node_index = node_index
        self.node_id = node_id or f"{node_index:016x}"
        self.city = city or "Unknown"
        self.coordinates = coordinates or (0.0, 0.0)
        self.performance_type = performance_type
        
        # Cougar protocol parameters (original specification)
        self.C = C  # Close neighbors count
        self.R = R  # Random neighbors count  
        self.P = P  # Parallelism parameter for body requests
        
        # Neighbor management (Algorithm 1 from paper)
        self.close_neighbors = set()      # C closest neighbors by RTT
        self.random_neighbors = set()     # R random neighbors
        self.outgoing_neighbor_set = set()  # ONS = close + random
        self.all_peers = set()           # Complete network knowledge
        self.peer_rtts = {}              # peer_id -> RTT value
        
        # Block tracking (header-first dissemination)
        self.received_headers = {}       # block_hash -> header_data
        self.received_blocks = {}        # block_hash -> (header, transactions)
        self.processed_blocks = set()    # Completed block hashes
        
        # P parameter tracking for parallel body requests
        self.received_headers_sources = {}   # block_hash -> [sender_id_list]
        self.headers_received_from = {}      # block_hash -> {sender_id: timestamp}
        
        # Body request management (P parameter support)
        self.pending_body_requests = {}  # block_hash -> [requested_peer_list]
        self.request_timeouts = {}       # (block_hash, peer_id) -> timeout_event_id
        self.body_request_sources = {}   # block_hash -> [primary_sources]
        
        # Header forwarding control (PURE COUGAR - only on header reception)
        self.forwarded_headers = set()   # block_hash set for flood prevention
        self.max_forwards_per_header = 1 # Headers forwarded only once
        
        # TCP model for academic timing calculations
        self.tcp_model = CougarTCPModel()
        
        # Simulation reference
        self.simulation = None
        
        # Performance tracking
        self.blocks_received = 0
        self.headers_received = 0
        self.body_requests_sent = 0
        self.body_responses_sent = 0

    def set_simulation(self, simulation):
        """Set simulation reference."""
        self.simulation = simulation
    
    # =============================================================================
    # NEIGHBOR MANAGEMENT (Algorithm 1 from Cougar paper)
    # =============================================================================
    
    def set_all_network_peers(self, all_peer_ids: List[str]):
        """
        Set complete network knowledge for fair protocol comparison.
        Note: Original protocol uses PSS (Peer Sampling Service) for discovery.
        """
        self.all_peers = set(peer_id for peer_id in all_peer_ids if peer_id != self.node_id)
        logger.debug(f"Node {self.node_index} received knowledge of {len(self.all_peers)} peers")
    
    def perform_initial_neighbor_selection(self):
        """
        Perform initial neighbor selection using C+R strategy (Algorithm 1).
        This implements the exact link placement algorithm from the paper.
        """
        if len(self.all_peers) < self.C + self.R:
            logger.warning(f"Node {self.node_index}: Not enough peers for C={self.C}, R={self.R}")
            # Use all available peers if network is smaller than C+R
            self.outgoing_neighbor_set = self.all_peers.copy()
            return
        
        # Measure RTTs to all peers (proximity calculation)
        self._measure_rtts_to_all_peers()
        
        # Select C closest neighbors by RTT (proximity-aware selection)
        sorted_peers = sorted(self.all_peers, key=lambda p: self.peer_rtts.get(p, float('inf')))
        self.close_neighbors = set(sorted_peers[:self.C])
        
        # Select R random neighbors from remaining peers (eclipse resistance)
        remaining_peers = list(self.all_peers - self.close_neighbors)
        if len(remaining_peers) >= self.R:
            self.random_neighbors = set(random.sample(remaining_peers, self.R))
        else:
            self.random_neighbors = set(remaining_peers)
        
        # Update outgoing neighbor set (ONS)
        self.outgoing_neighbor_set = self.close_neighbors | self.random_neighbors
        
        logger.info(f"Node {self.node_index} selected neighbors: "
                   f"C={len(self.close_neighbors)}, R={len(self.random_neighbors)}")
    
    def _measure_rtts_to_all_peers(self):
        """Measure RTTs to all peers for proximity-aware neighbor selection."""
        if not self.simulation:
            # Fallback: use dummy RTTs for testing without simulation
            for peer_id in self.all_peers:
                self.peer_rtts[peer_id] = random.uniform(10, 500)  # 10-500ms
            return
        
        # Use actual simulation RTT data for realistic neighbor selection
        for peer_id in self.all_peers:
            peer_index = self._get_peer_index_from_id(peer_id)
            if peer_index is not None:
                # Get peer city for RTT calculation
                peer_city = "Unknown"
                if peer_index in self.simulation.nodes:
                    peer_node = self.simulation.nodes[peer_index]
                    peer_city = peer_node.city
                
                # Calculate RTT using geographic distance
                rtt_seconds = self.simulation.delay_calculator.calculate_rtt_delay(
                    self.city, peer_city
                )
                # Convert to milliseconds for Cougar calculations
                rtt_ms = rtt_seconds * 1000.0 if rtt_seconds else 100.0
                self.peer_rtts[peer_id] = rtt_ms
    
    def _get_peer_index_from_id(self, peer_id: str) -> Optional[int]:
        """Convert peer_id to node_index for RTT lookup."""
        # Fast lookup using cached mapping if available
        if hasattr(self, 'peer_id_to_index') and peer_id in self.peer_id_to_index:
            return self.peer_id_to_index[peer_id]
    
        # Fallback: search through simulation nodes
        if self.simulation:
            for node_index, node in self.simulation.nodes.items():
                if node.node_id == peer_id:
                    return node_index
        return None
    
    # =============================================================================
    # BLOCK BROADCASTING (Pure Cougar Header-First Dissemination)
    # =============================================================================
    
    def broadcast_new_block(self, block_hash: str, header_data: Dict[str, Any], 
                       transactions: List[Dict[str, Any]]):
        """FIXED: Start broadcasting with immediate source completion."""
        
        logger.error(f"🟢 BROADCAST_NEW_BLOCK: Node {self.node_index} starting for {block_hash[:8]}")
        
        if block_hash in self.processed_blocks:
            return  # Already processed this block
        
        # Store complete block data
        self.received_blocks[block_hash] = (header_data, transactions)
        self.received_headers[block_hash] = header_data
        self.processed_blocks.add(block_hash)
        self.blocks_received += 1  # ✅ INCREMENT COUNTER
        
        # ✅ CRITICAL: Source node callback immediately
        self._trigger_block_completion_callback(block_hash, header_data, transactions)
        
        # Push header to all neighbors
        self._push_header_to_neighbors(block_hash, header_data)
        
        logger.info(f"Node {self.node_index} started broadcasting block {block_hash}")

    
    def _push_header_to_neighbors(self, block_hash: str, header_data: Dict[str, Any]):
        """
        Push header to all neighbors (Pure Cougar header advertisement).
        This implements the exact header dissemination from the paper.
        """
        if block_hash in self.forwarded_headers:
            return  # Already forwarded this header (flood prevention)
        
        if not self.outgoing_neighbor_set:
            logger.error(f"Node {self.node_index} has empty outgoing_neighbor_set! Cannot send headers.")
            return
        
        # Mark as forwarded to prevent duplicate forwarding
        self.forwarded_headers.add(block_hash)
        
        # Create header message following protocol specification
        header_msg = CougarHeaderMessage(
            sender_id=self.node_id,
            block_hash=block_hash,
            header_data=header_data
        )
        
        # Send header to all neighbors in ONS (broadcast)
        for neighbor_id in self.outgoing_neighbor_set:
            self._send_message_to_peer(header_msg, neighbor_id)
        
        logger.debug(f"Node {self.node_index} pushed header {block_hash[:8]} to {len(self.outgoing_neighbor_set)} neighbors")
    
    def handle_header_message(self, header_msg: CougarHeaderMessage):
        """FIXED: Proper header handling with callback for source nodes."""
        block_hash = header_msg.block_hash
        sender_id = header_msg.sender_id
        
        # ✅ FIX: If this is a source node receiving its own header, mark as completed
        if sender_id == self.node_id:
            logger.error(f"🎯 SOURCE NODE: Node {self.node_index} processing own block {block_hash[:8]}")
            # Source node has the complete block immediately
            if block_hash not in self.processed_blocks:
                self.processed_blocks.add(block_hash)
                self.blocks_received += 1
                # ✅ TRIGGER CALLBACK FOR SOURCE NODE
                self._trigger_block_completion_callback(block_hash, header_msg.header_data, [])
            return
        
        # ✅ Skip if already have complete block
        if block_hash in self.received_blocks:
            return
        
        # Store header and track sources
        if block_hash not in self.received_headers:
            self.received_headers[block_hash] = header_msg.header_data
            self.headers_received += 1  # ✅ INCREMENT COUNTER
        
        # Track header sources for P parameter
        if block_hash not in self.received_headers_sources:
            self.received_headers_sources[block_hash] = []
        
        if sender_id not in self.received_headers_sources[block_hash]:
            self.received_headers_sources[block_hash].append(sender_id)
        
        # ✅ Forward header to neighbors
        self._push_header_to_neighbors(block_hash, header_msg.header_data)
        
        # ✅ Request body from sources
        self._request_body_from_multiple_peers(block_hash)
    
    def _process_validated_header(self, block_hash: str):
        """
        Process validated header following pure Cougar algorithm.
        
        Pure Cougar steps:
        1. Forward header to neighbors (if not already done)
        2. Request body from up to P peers in parallel
        """
        if block_hash in self.processed_blocks:
            return
        
        # Step 1: Forward header to neighbors (flood prevention check)
        if block_hash not in self.forwarded_headers:
            header_data = self.received_headers[block_hash]
            self._push_header_to_neighbors(block_hash, header_data)
        
        # Step 2: Request body from multiple peers (P parameter)
        if block_hash not in self.pending_body_requests:
            self._request_body_from_multiple_peers(block_hash)
    
    def _request_body_from_multiple_peers(self, block_hash: str):
        """Request body following EXACT Cougar paper specification: P=4 -> 2 close + 2 random."""
        if block_hash in self.pending_body_requests:
            return
        
        header_sources = self.received_headers_sources.get(block_hash, [])
        
        # EXACT PAPER IMPLEMENTATION: P split between close/random sets
        close_requests = self.P // 2      # P=4 -> 2
        random_requests = self.P - close_requests  # P=4 -> 2
        
        # Select from close set: "first two peers that delivered header from close set"
        close_header_sources = [peer for peer in header_sources if peer in self.close_neighbors]
        selected_close = close_header_sources[:close_requests]
        
        # Select from random set: "first two peers that delivered header from random set"  
        random_header_sources = [peer for peer in header_sources if peer in self.random_neighbors]
        selected_random = random_header_sources[:random_requests]
        
        # Combined selection (paper's exact strategy)
        selected_peers = selected_close + selected_random
        
        # If insufficient header sources, fill from remaining neighbors
        if len(selected_peers) < self.P:
            # Add remaining close neighbors if needed
            remaining_close = min(close_requests - len(selected_close), 
                                len([n for n in self.close_neighbors if n not in selected_peers]))
            for neighbor_id in self.close_neighbors:
                if neighbor_id not in selected_peers and remaining_close > 0:
                    selected_peers.append(neighbor_id)
                    remaining_close -= 1
            
            # Add remaining random neighbors if still needed
            remaining_random = self.P - len(selected_peers)
            for neighbor_id in self.random_neighbors:
                if neighbor_id not in selected_peers and remaining_random > 0:
                    selected_peers.append(neighbor_id)
                    remaining_random -= 1
        
        # Send requests (rest stays the same)
        self.pending_body_requests[block_hash] = selected_peers
        self.body_request_sources[block_hash] = selected_peers
        
        for peer_id in selected_peers:
            self._send_single_body_request(block_hash, peer_id)
        
        logger.info(f"Node {self.node_index} sent body requests: {len(selected_close)} close + {len(selected_random)} random (P={self.P})")


    def _send_single_body_request(self, block_hash: str, peer_id: str):
        """Send single body request to specific peer."""
        # Create body request message
        body_request = CougarBodyRequestMessage(
            sender_id=self.node_id,
            target_peer_id=peer_id,
            block_hash=block_hash
        )
        
        self.body_requests_sent += 1
        
        # Report metrics for analysis
        if self.simulation:
            block_id = self._extract_block_id_from_hash(block_hash)
            peer_index = self._get_peer_index_from_id(peer_id)
            self.simulation.metrics_collector.report_body_request_sent(
                self.node_index, block_id, peer_index, 
                self.simulation.event_queue.current_time
            )
        
        # Send request via simulation routing
        self._send_message_to_peer(body_request, peer_id)
    
    def handle_body_request(self, body_request: CougarBodyRequestMessage):
        """
        Handle body request from peer (Pure Cougar Algorithm).
        
        Pure Cougar behavior: Send body response if available.
        NOTE: NO header forwarding here - this is not in the original protocol!
        """
        block_hash = body_request.block_hash
        requester_id = body_request.sender_id
        

        logger.error(f"🔵 BODY REQUEST: Node {self.node_index} got request for {block_hash[:8]} from {requester_id[:8]}")
        logger.error(f"🔵 Current received_blocks: {len(self.received_blocks)} blocks")
        logger.error(f"🔵 Block {block_hash[:8]} in received_blocks: {block_hash in self.received_blocks}")
    
        # Check if we have the complete block
        if block_hash not in self.received_blocks:
            logger.debug(f"Node {self.node_index} cannot fulfill body request for {block_hash}")
            return
        
        # Get block data to send
        header_data, transactions = self.received_blocks[block_hash]
        
        # Create body response message
        body_response = CougarBodyResponseMessage(
            sender_id=self.node_id,
            target_peer_id=requester_id,
            block_hash=block_hash,
            transactions=transactions
        )
        
        self.body_responses_sent += 1
        
        # Report metrics for analysis
        if self.simulation:
            block_id = self._extract_block_id_from_hash(block_hash)
            requester_index = self._get_peer_index_from_id(requester_id)
            self.simulation.metrics_collector.report_body_response_sent(
                self.node_index, block_id, requester_index, 
                body_response.size, self.simulation.event_queue.current_time
            )
        
        # Send body response
        self._send_message_to_peer(body_response, requester_id)
        
        # NOTE: Pure Cougar does NOT forward headers on body request
        # Only body response is sent - header forwarding only happens on header reception
        logger.debug(f"Node {self.node_index} sent body response for {block_hash[:8]} to {requester_id[:8]}")
    
    def handle_body_response(self, body_response: CougarBodyResponseMessage):
        """FIXED: Ensure proper completion callback after block reception."""
        block_hash = body_response.block_hash
        
        logger.error(f"🟢 BODY RESPONSE RECEIVED: Node {self.node_index} got response for {block_hash[:8]}")
        
        if block_hash in self.processed_blocks:
            logger.error(f"🔴 ALREADY PROCESSED: {block_hash[:8]}")
            return
        
        # Cancel other pending requests
        self._cancel_all_pending_requests(block_hash)
        
        # Store complete block
        header_data = self.received_headers[block_hash]
        self.received_blocks[block_hash] = (header_data, body_response.transactions)
        self.processed_blocks.add(block_hash)
        self.blocks_received += 1  # ✅ INCREMENT COUNTER
        
        # ✅ CRITICAL FIX: Call completion callback immediately
        self._trigger_block_completion_callback(block_hash, header_data, body_response.transactions)
        
        # Forward header to remaining neighbors
        self._push_header_to_neighbors(block_hash, header_data)


    def handle_body_response(self, body_response: CougarBodyResponseMessage):
        """FIXED: Ensure proper completion callback after block reception."""
        block_hash = body_response.block_hash
        
        logger.error(f"🟢 BODY RESPONSE RECEIVED: Node {self.node_index} got response for {block_hash[:8]}")
        
        if block_hash in self.processed_blocks:
            logger.error(f"🔴 ALREADY PROCESSED: {block_hash[:8]}")
            return
        
        # Cancel other pending requests
        self._cancel_all_pending_requests(block_hash)
        
        # Store complete block
        header_data = self.received_headers[block_hash]
        self.received_blocks[block_hash] = (header_data, body_response.transactions)
        self.processed_blocks.add(block_hash)
        self.blocks_received += 1  # ✅ INCREMENT COUNTER
        
        # ✅ CRITICAL FIX: Call completion callback immediately
        self._trigger_block_completion_callback(block_hash, header_data, body_response.transactions)
        
        # Forward header to remaining neighbors
        self._push_header_to_neighbors(block_hash, header_data)


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


    def _trigger_block_completion_callback(self, block_hash: str, header_data: Dict[str, Any], 
                             transactions: List[Dict[str, Any]]):
        """✅ ENHANCED: Trigger block completion callback with proper simulator integration."""
        try:
            if self.simulation and hasattr(self.simulation, 'on_block_completed'):
                # ✅ IMPROVED: Better block_id extraction with simulator help
                block_id = None
                
                # Method 1: Ask simulator to resolve hash to ID
                if hasattr(self.simulation, 'resolve_block_hash_to_id'):
                    block_id = self.simulation.resolve_block_hash_to_id(block_hash)
                
                # Method 2: Search through simulator's generated content
                if block_id is None and hasattr(self.simulation, 'generated_blocks_content'):
                    for bid, content in self.simulation.generated_blocks_content.items():
                        if content.get('block_hash') == block_hash:
                            block_id = bid
                            break
                
                # Method 3: Fallback to extraction method
                if block_id is None:
                    block_id = self._extract_block_id_from_hash(block_hash)
                
                current_time = self.simulation.event_queue.current_time
                
                logger.error(f"🔥 TRIGGERING CALLBACK: Node {self.node_index}, Block {block_id} (hash: {block_hash[:8]})")
                
                # ✅ CRITICAL: Update simulator's block_metrics immediately for coverage calculation
                if hasattr(self.simulation, 'block_metrics') and block_id in self.simulation.block_metrics:
                    if self.node_index not in self.simulation.block_metrics[block_id]["completion_times"]:
                        self.simulation.block_metrics[block_id]["completion_times"][self.node_index] = current_time
                        self.simulation.block_metrics[block_id]["nodes_completed"] += 1
                        
                        logger.error(f"✅ BLOCK METRICS UPDATED: Block {block_id} now has {self.simulation.block_metrics[block_id]['nodes_completed']} completions")
                
                # Call the callback with resolved block_id
                self.simulation.on_block_completed(self.node_index, block_id, current_time)
                
                logger.error(f"✅ CALLBACK COMPLETED: Node {self.node_index}, Block {block_id}")
                
        except Exception as e:
            logger.error(f"❌ Error in completion callback: {e}")


    def _process_validated_body(self, block_hash: str, header_data: Dict[str, Any], 
                           transactions: List[Dict[str, Any]]):
        """Process body after validation - complete block reconstruction."""
        # Store complete block
        self.received_blocks[block_hash] = (header_data, transactions)
        self.processed_blocks.add(block_hash)
        self.blocks_received += 1
        
        # Report metrics for analysis
        if self.simulation:
            block_id = self._extract_block_id_from_hash(block_hash)
            self.simulation.metrics_collector.report_body_completed(
                self.node_index, block_id, self.simulation.event_queue.current_time
            )
            
            # Also report standard block completion for compatibility
            self.simulation.metrics_collector.report_block_processed(
                self.node_index, block_id, self.simulation.event_queue.current_time
            )
        
        # Continue propagation to other nodes (re-broadcast complete block)
        self.broadcast_new_block(block_hash, header_data, transactions)
        
        # Notify simulation of block completion
        if self.simulation and hasattr(self.simulation, 'on_block_completed'):
            current_time = time.time()
            if hasattr(self.simulation, 'event_queue'):
                current_time = self.simulation.event_queue.current_time
            
            # Convert block_hash to block_id for metrics consistency
            block_id = self._extract_block_id_from_hash(block_hash)
            
            self.simulation.on_block_completed(
                self.node_index, block_id, current_time
            )
        
        logger.info(f"Node {self.node_index} completed block {block_hash}")
    
    # =============================================================================
    # UTILITY METHODS
    # =============================================================================
    
    def _extract_block_id_from_hash(self, block_hash: str) -> int:
        """Extract numeric block ID from block hash for metrics."""
        try:
            # FIXED: First try to get from simulator's block_metrics
            if self.simulation and hasattr(self.simulation, 'block_metrics'):
                for block_id, metrics in self.simulation.block_metrics.items():
                    # Try to match by looking for this hash in generated content
                    if hasattr(self.simulation, 'generated_blocks_content'):
                        block_data = self.simulation.generated_blocks_content.get(block_id, {})
                        if block_data.get('block_hash', '').startswith(block_hash[:8]):
                            return block_id
            
            # FIXED: Fallback with better parsing
            if block_hash.startswith("block_"):
                # Try to extract number after block_
                parts = block_hash.split("_")
                if len(parts) > 1:
                    try:
                        return int(parts[1])
                    except ValueError:
                        pass
            
            # FIXED: Last resort - use a deterministic hash function instead of random
            import hashlib
            hash_obj = hashlib.md5(block_hash.encode())
            return int(hash_obj.hexdigest()[:6], 16) % 1000  # Use first 6 hex chars
            
        except (ValueError, IndexError):
            return hash(block_hash) % 1000000
        
    def get_completion_coverage_for_simulator(self) -> Dict[str, Any]:
        """Get node completion data for coverage calculation in simulator."""
        return {
            'node_index': self.node_index,
            'blocks_received': self.blocks_received,
            'processed_blocks': list(self.processed_blocks),
            'received_blocks_count': len(self.received_blocks),
            'headers_received': self.headers_received,
            'body_requests_sent': self.body_requests_sent,
            'body_responses_sent': self.body_responses_sent
        }    
            
    def _send_message_to_peer(self, message, peer_id: str):
        """Send message to peer via simulation routing."""
        if self.simulation:
            peer_index = self._get_peer_index_from_id(peer_id)
            if peer_index is not None:
                self.simulation._route_cougar_message(
                    message, self.node_index, peer_index
                )
    
    def _cancel_all_pending_requests(self, block_hash: str):
        """
        Cancel all pending body requests for block (P parameter cleanup).
        Called when first body response is received to cancel other P-1 requests.
        """
        if block_hash in self.pending_body_requests:
            requesting_peers = self.pending_body_requests[block_hash]
            logger.debug(f"Node {self.node_index} canceling {len(requesting_peers)} pending requests for {block_hash[:8]} (P={self.P})")
            
            # Clean up tracking structures
            del self.pending_body_requests[block_hash]
            if block_hash in self.body_request_sources:
                del self.body_request_sources[block_hash]
    
    def handle_body_request_timeout(self, block_hash: str, peer_id: str):
        """Handle body request timeout event (called from simulator)."""
        # Report timeout metrics

         # ✅ FIX: Eğer block zaten complete ise timeout'u ignore et
        if block_hash in self.processed_blocks:
            logger.debug(f"Node {self.node_index} ignoring timeout for {block_hash[:8]} - already completed")
            return
    
    # ✅ FIX: Eğer block zaten received_blocks'ta ise ignore et  
        if block_hash in self.received_blocks:
            logger.debug(f"Node {self.node_index} ignoring timeout for {block_hash[:8]} - already have block")
            return
        if self.simulation:
            block_id = self._extract_block_id_from_hash(block_hash)
            peer_index = self._get_peer_index_from_id(peer_id)
            self.simulation.metrics_collector.report_body_request_timeout(
                self.node_index, block_id, peer_index,
                self.simulation.event_queue.current_time
            )
        
        logger.warning(f"Node {self.node_index} timeout for block {block_hash} from peer {peer_id}")
        
        # Note: Pure Cougar implementation - no retry logic specified in paper
        # Timeout is handled by waiting for other P-1 parallel requests
    
    # =============================================================================
    # STATUS AND DEBUGGING
    # =============================================================================
    
    def get_node_status(self) -> Dict[str, Any]:
        """Get node status for debugging and analysis."""
        return {
            "node_index": self.node_index,
            "node_id": self.node_id,
            "city": self.city,
            "protocol_parameters": {
                "C": self.C,
                "R": self.R, 
                "P": self.P
            },
            "neighbors": {
                "close_neighbors": len(self.close_neighbors),
                "random_neighbors": len(self.random_neighbors),
                "total_neighbors": len(self.outgoing_neighbor_set)
            },
            "block_status": {
                "processed_blocks": len(self.processed_blocks),
                "pending_requests": len(self.pending_body_requests),
                "blocks_received": self.blocks_received
            },
            "parallelism_stats": {
                "pending_requests_by_block": {
                    block_hash: len(peers) 
                    for block_hash, peers in self.pending_body_requests.items()
                },
                "header_sources_tracked": len(self.received_headers_sources),
                "avg_requests_per_block": (
                    sum(len(peers) for peers in self.pending_body_requests.values()) 
                    / max(len(self.pending_body_requests), 1)
                )
            },
            "message_stats": {
                "headers_received": self.headers_received,
                "body_requests_sent": self.body_requests_sent,
                "body_responses_sent": self.body_responses_sent
            }
        }
    
    # =============================================================================
    # COMPATIBILITY METHODS (for simulation interface)
    # =============================================================================
    
    def start_block_broadcast(self, block_hash: str, header_data: Dict[str, Any], 
                         transactions: List[Dict[str, Any]]):
        """
        Simulator compatibility method - delegates to broadcast_new_block.
        This method provides compatibility with simulator's expected interface.
        """
        return self.broadcast_new_block(block_hash, header_data, transactions)
    
    def set_outgoing_neighbor_set(self, close_neighbors: set, random_neighbors: set):
        """Set neighbor sets from simulator (compatibility method)."""
        self.close_neighbors = close_neighbors
        self.random_neighbors = random_neighbors  
        self.outgoing_neighbor_set = close_neighbors.union(random_neighbors)
        logger.debug(f"Node {self.node_index} ONS updated: {len(self.outgoing_neighbor_set)} neighbors")