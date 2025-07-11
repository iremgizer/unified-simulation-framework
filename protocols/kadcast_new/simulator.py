import logging
import random
import time
import uuid
import sys
import os
from typing import Dict, List, Any, Optional

# Add the src directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, '..', '..')
sys.path.insert(0, src_dir)

from simulation.event_queue import EventQueue, Event, EventType, SimulationEngine
from simulation.network_delay import NetworkDelayCalculator
from simulation.metrics import Metrics
from .node import KadcastNode
from data.geo_data_provider import GeoDataProvider
from data.realistic_payload import RealisticBlockGenerator, RealisticChunkGenerator
from src.protocols.kadcast.messages import FindNodeMessage, NodesMessage, PingMessage, PongMessage, ChunkMessage, ReqMessage

logger = logging.getLogger("KadcastSimulator")

class KadcastSimulator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.simulation_id = str(uuid.uuid4())
        self.node_count = config.get("nodeCount", 100)
        self.k_bucket_size = config.get("kBucketSize", 20)

        self.nodes: Dict[int, KadcastNode] = {}
        self.nodes_by_id: Dict[str, KadcastNode] = {}
        self.current_time = 0.0
        self.running = False
        self.phase = "initialization"

        self.event_queue = EventQueue()
        self.engine = SimulationEngine(self.event_queue)

        data_dir = os.path.join(src_dir, 'data')
        self.geo_provider = GeoDataProvider(data_dir)
        self.delay_calculator = NetworkDelayCalculator(self.geo_provider)
        self.metrics_collector = Metrics(self.node_count)
        self.metrics_collector.set_protocol_name("kadcast")

        self.block_generator = RealisticBlockGenerator(seed=42)
        self.chunk_generator = RealisticChunkGenerator()

        self.real_time_metrics = {
            "blocks_generated": 0,
            "blocks_completed": {},
            "chunks_transmitted": 0,
            "current_latency": 0.0,
            "current_coverage": 0.0,
            "current_congestion": 0.0,
            "delay_breakdown": {
                "computational": 0.0,
                "rtt": 0.0,
                "bandwidth": 0.0
            }
        }

        # Peer discovery kontrolü için yeni alan
        self.peer_discovery_flags = {}
        self.peer_discovery_timeout = 180.0  # 3 dakika timeout
        
        self._register_event_handlers()
        self.update_callback = None
        logger.info(f"KadcastSimulator initialized with {self.node_count} nodes")

    def set_update_callback(self, callback):
        self.update_callback = callback

    def _send_update(self, update_type: str, data: Dict[str, Any]):
        if self.update_callback:
            self.update_callback(update_type, data)

    def initialize_network(self) -> bool:
       
     try:
        import hashlib
        
        logger.info("Initializing network...")
        self.phase = "network_initialization"
        node_assignments = self.geo_provider.assign_nodes_to_regions_and_cities(self.node_count)
        used_ids = set()

        def generate_hybrid_node_id(node_index: int, city: str, coordinates: tuple) -> str:
            """
            Hybrid Hash-Based Node ID Generation (Kadcast'a uygun)
            
            Coğrafi bilgi + node_index + salt → hash
            - Deterministic ama unpredictable
            - Coğrafi korelasyonu kırıyor
            - Bucket distribution'ı iyileştiriyor
            """
            # Coğrafi bilgiyi normalize et
            lat, lon = coordinates
            geo_string = f"{city}_{lat:.6f}_{lon:.6f}"
            
            # Simulation seed'i ekle (reproducible results için)
            simulation_salt = "kadcast_simulation_2024"
            
            # Node-specific salt (her node için farklı)
            node_salt = (node_index * 7919) % 1000000  # 7919 prime number
            
            # Combine all components
            combined_input = f"{geo_string}_{node_index}_{node_salt}_{simulation_salt}"
            
            # SHA256 hash
            hash_object = hashlib.sha256(combined_input.encode('utf-8'))
            full_hash = hash_object.hexdigest()
            
            # 32-bit node ID (8 hex characters)
            node_id = full_hash[:8]
            
            return node_id

        def ensure_unique_node_id(node_index: int, city: str, coordinates: tuple) -> str:
            """Unique ID garantisi ile hybrid ID generation"""
            base_id = generate_hybrid_node_id(node_index, city, coordinates)
            
            # Eğer collision varsa, salt ekleyerek yeni ID üret
            collision_counter = 0
            final_id = base_id
            
            while final_id in used_ids:
                collision_counter += 1
                # Collision durumunda salt ekle
                collision_input = f"{city}_{coordinates[0]}_{coordinates[1]}_{node_index}_{collision_counter}_collision"
                collision_hash = hashlib.sha256(collision_input.encode('utf-8')).hexdigest()
                final_id = collision_hash[:8]
                
                # Sonsuz döngü koruması
                if collision_counter > 1000:
                    # Fallback: random ID
                    final_id = f"{random.randint(0, 0xFFFFFFFF):08x}"
                    break
            
            used_ids.add(final_id)
            return final_id

        # Node'ları oluştur
        for node_index in range(self.node_count):
            assignment = node_assignments[node_index]
            
            # Hybrid hash-based ID generation
            node_id = ensure_unique_node_id(
                node_index=node_index,
                city=assignment["city"], 
                coordinates=assignment["coordinates"]
            )
            
            is_bootstrap = node_index < 3

            node = KadcastNode(
                node_index=node_index,
                node_id=node_id,
                city=assignment["city"],
                coordinates=assignment["coordinates"],
                is_bootstrap=is_bootstrap
            )
            node.k = self.k_bucket_size
            node.set_simulation(self)
            self.nodes[node_index] = node
            self.nodes_by_id[node.node_id] = node
            
            # Debug log (ilk 10 node için)
            if node_index < 10:
                logger.debug(f"Node {node_index}: ID={node_id}, City={assignment['city']}, Bootstrap={is_bootstrap}")

        # Peer discovery flags'i başlat
        self.peer_discovery_flags = {idx: False for idx in self.nodes}

        # Bootstrap node'ları birbirlerini tanısın
        for bootstrap_index in range(3):
            bootstrap_node = self.nodes[bootstrap_index]
            for idx, other in self.nodes.items():
                if idx != bootstrap_index:
                    bootstrap_node.add_peer(other.node_id)

        # Diğer node'lar bootstrap node'ları tanısın
        for node_index, node in self.nodes.items():
            if not node.is_bootstrap:
                for bootstrap_index in range(3):
                    bootstrap_node = self.nodes[bootstrap_index]
                    node.add_peer(bootstrap_node.node_id)
                    node.bootstrap_nodes.add(bootstrap_node.node_id)

        self._setup_network_connections()

        # ID distribution analizi (debug için)
        self._analyze_id_distribution()

        self._send_update("network_initialized", {
            "nodeCount": self.node_count,
            "bootstrapNodes": [0, 1, 2],
            "geoDistribution": node_assignments,
            "idGenerationMethod": "hybrid_hash_based"
        })

        logger.info(f"Network initialized with {self.node_count} nodes using hybrid hash-based ID generation")
        return True

     except Exception as e:
        logger.error(f"Failed to initialize network: {e}")
        return False

    def _analyze_id_distribution(self):
       """Node ID dağılımını analiz et (debug amaçlı)"""
       try:
        # Bucket distribution analizi
        bucket_counts = [0] * 32
        
        # Bootstrap node'u referans al (node 0)
        if 0 in self.nodes:
            reference_node = self.nodes[0]
            reference_id_int = int(reference_node.node_id, 16)
            
            for node in self.nodes.values():
                if node.node_index != 0:  # Kendisi hariç
                    node_id_int = int(node.node_id, 16)
                    xor_distance = reference_id_int ^ node_id_int
                    bucket_index = max(0, min(xor_distance.bit_length() - 1, 31))
                    bucket_counts[bucket_index] += 1
            
            # Log bucket distribution
            non_empty_buckets = sum(1 for count in bucket_counts if count > 0)
            logger.info(f"ID Distribution Analysis: {non_empty_buckets}/32 buckets have nodes")
            logger.debug(f"Bucket distribution: {bucket_counts}")
            
            # Coğrafi korelasyon kontrolü
            city_id_samples = {}
            for node in list(self.nodes.values())[:20]:  # İlk 20 node
                city = node.city
                if city not in city_id_samples:
                    city_id_samples[city] = []
                city_id_samples[city].append(node.node_id[:4])  # İlk 4 karakter
            
            logger.debug(f"City-ID samples (first 4 chars): {city_id_samples}")
            
       except Exception as e:
        logger.warning(f"ID distribution analysis failed: {e}")



    def _setup_network_connections(self):
        default_bandwidth = 12500000  # 100 Mbps in bytes/sec

        for i in range(self.node_count):
            for j in range(i + 1, self.node_count):
                city1 = self.nodes[i].city
                city2 = self.nodes[j].city
                rtt = self.geo_provider.get_rtt(city1, city2)

                if rtt < 50:
                    bandwidth = default_bandwidth
                elif rtt < 150:
                    bandwidth = int(default_bandwidth * 0.8)
                else:
                    bandwidth = int(default_bandwidth * 0.6)

                self.delay_calculator.set_connection_bandwidth(i, j, bandwidth)

        logger.info("Network connections configured with realistic bandwidth")

    def start_peer_discovery(self) -> bool:
        try:
            logger.info("Starting peer discovery phase...")
            self.phase = "peer_discovery"
            
            # Timeout event'ini zamanla
            logger.info(f"Scheduling peer discovery timeout for {self.peer_discovery_timeout} seconds")
            self.event_queue.schedule_event(
                EventType.TIMEOUT, -1, self.peer_discovery_timeout,
                {"timeout_type": "peer_discovery_complete"}
            )
            
            # Bootstrap node'ları otomatik complete işaretle (Kadcast'a uygun)
            bootstrap_count = 0
            for node in self.nodes.values():
                if node.is_bootstrap:
                    node.discovery_complete = True
                    self.peer_discovery_flags[node.node_index] = True
                    bootstrap_count += 1
                    logger.info(f"Bootstrap Node {node.node_index} auto-marked as discovery complete")
            
            logger.info(f"Auto-completed {bootstrap_count} bootstrap nodes")
            
            self._send_update("peer_discovery_started", {
                "duration": self.peer_discovery_timeout,
                "phase": "peer_discovery"
            })

            # Bootstrap olmayan node'lar için peer discovery başlat (Kadcast makalesine uygun)
            bootstrap_node_ids = [node.node_id for node in self.nodes.values() if node.is_bootstrap]
            discovery_started_count = 0
            
            for node in self.nodes.values():
                if not node.is_bootstrap:
                    if bootstrap_node_ids:
                        # Kadcast makalesine uygun: Self-lookup başlat
                        node.start_self_lookup()
                        discovery_started_count += 1
                        logger.debug(f"Node {node.node_index} started self-lookup for peer discovery")
                    else:
                        logger.warning(f"No bootstrap nodes found for node {node.node_id} to start peer discovery.")

            logger.info(f"Started peer discovery for {discovery_started_count} non-bootstrap nodes")
            
            # Tüm bootstrap'lar complete ise kontrol et
            if all(self.peer_discovery_flags[i] for i in range(3)):
                completed_count = sum(1 for flag in self.peer_discovery_flags.values() if flag)
                logger.info(f"Initial state: {completed_count}/{len(self.peer_discovery_flags)} nodes completed")

            return True
        except Exception as e:
            logger.error(f"Failed to start peer discovery: {e}")
            return False

    def _get_node_by_id(self, node_id: str) -> Optional[KadcastNode]:
        # node_id'yi temizle (0x ön ekini kaldır ve küçük harfe çevir)
        cleaned_node_id = node_id.lower().replace("0x", "")
        return self.nodes_by_id.get(cleaned_node_id)
    
    def start_broadcasting(self) -> bool:
        """Start the block broadcasting phase."""
        try:
            logger.info("Starting block broadcasting phase...")
            self.phase = "broadcasting"
            
            # Schedule first block generation
            self._schedule_block_generation(1.0)  # Start after 1 second
            
            # Schedule broadcasting completion (after 60 seconds for demo)
            self.event_queue.schedule_event(
                EventType.TIMEOUT, -1, 60.0,
                {"timeout_type": "broadcasting_complete"}
            )
            
            # Send update
            self._send_update("broadcasting_started", {
                "duration": 60,  # 60 seconds for demo
                "frequency": self.config.get("broadcastFrequency", 10),
                "phase": "broadcasting"
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start broadcasting: {e}")
            return False
    
    def _schedule_block_generation(self, delay: float):
        """Schedule block generation."""
        self.event_queue.schedule_event(
            EventType.BLOCK_GENERATE, -1, delay,
            {"block_size": self.config.get("blockSize", 1000000)}
        )
    
    def run_simulation(self):
        """Run the complete simulation."""
        try:
            self.running = True
            logger.info(f"Starting Kadcast simulation {self.simulation_id}")
            
            # Register event handlers
            self._register_event_handlers()
            
            # Phase 1: Initialize network
            if not self.initialize_network():
                raise Exception("Network initialization failed")
            
            # Phase 2: Peer discovery
            if not self.start_peer_discovery():
                raise Exception("Peer discovery failed")
            
            # Run event loop
            self.engine.run(max_time=300.0)  # 5 minutes total
            
            logger.info("Simulation completed successfully")
            
        except Exception as e:
            logger.error(f"Simulation failed: {e}")
            self.running = False
            raise
    
    def _register_event_handlers(self):
        """Register event handlers with the simulation engine."""
        self.engine.register_handler(EventType.TIMEOUT, self._handle_timeout_event)
        self.engine.register_handler(EventType.MESSAGE_SEND, self._handle_message_send_event)
        self.engine.register_handler(EventType.FIND_NODE, self._handle_find_node_event)
        self.engine.register_handler(EventType.NODES_RESPONSE, self._handle_nodes_response_event)
        self.engine.register_handler(EventType.BLOCK_GENERATE, self._handle_block_generate_event)
    
    def _handle_block_generate_event(self, event: Event):
        """Handle block generation event with realistic processing."""
        # Select random node to generate block
        source_node_idx = random.choice(list(self.nodes.keys()))
        source_node = self.nodes[source_node_idx]
        
        # Generate realistic block with transactions
        block_size = event.data.get("block_size", 1000000)
        block_data = self.block_generator.generate_realistic_block(block_size)
        block_id = int(time.time() * 1000) + self.real_time_metrics["blocks_generated"]
        
        # Create block object
        block = RealisticBlockGenerator.generate_block(block_id, self.current_time)
        
        # Generate chunks with FEC
        chunk_size = self.config.get("chunkSize", 1024)
        fec_factor = self.config.get("fecFactor", 0.2)
        chunks = self.chunk_generator.chunkify_with_fec(
            block_data["serialized_data"], 
            chunk_size, 
            fec_factor
        )
        
        # Update metrics
        self.real_time_metrics["blocks_generated"] += 1
        self.real_time_metrics["chunks_transmitted"] += len(chunks)
        
        # Record block start time
        self.metrics_collector.record_block_start(block_id, self.current_time)
        
        # Simulate block broadcasting with delays
        self._broadcast_block_with_delays(source_node_idx, block, chunks, block_data["transactions"])
        
        # Schedule next block generation if still broadcasting
        if self.phase == "broadcasting":
            self._schedule_block_generation(self.config.get("broadcastFrequency", 10))
        
        logger.info(f"Generated block {block_id} from node {source_node_idx} with {len(chunks)} chunks")
    
    def _broadcast_block_with_delays(self, source_node_idx: int, block: Dict[str, Any], 
                                   chunks: List[bytes], transactions: List[Dict]):
        """Broadcast block with realistic delays."""
        source_node = self.nodes[source_node_idx]
        source_city = source_node.city
        
        # Track which nodes have received the block
        nodes_received = set()
        total_delays = []
        
        # Broadcast to all other nodes
        for target_node_idx, target_node in self.nodes.items():
            if target_node_idx == source_node_idx:
                continue
            
            target_city = target_node.city
            
            # Calculate total chunk data size
            total_chunk_data_size = sum(len(chunk_data) for chunk_data in chunks)
            
            delay_info = self.delay_calculator.calculate_total_delay(
                source_node_idx, target_node_idx,
                source_city, target_city,
                total_chunk_data_size, block["size"],
                self.event_queue.current_time, len(transactions)
            )
            
            if delay_info:
                avg_delay = delay_info["total_delay"]
                total_delays.append(avg_delay)
                
                # Send first chunk as representative
                if chunks:
                    first_chunk_bytes = chunks[0]
                    chunk_message = ChunkMessage(sender_id=source_node.node_id, chunk_bytes=first_chunk_bytes)
                    
                    self.event_queue.schedule_event(
                        EventType.MESSAGE_SEND,
                        node_id=target_node.node_index,
                        delay=avg_delay,
                        data={
                            "sender_id": source_node.node_id,
                            "receiver_node_id": target_node.node_id,
                            "message_type": "CHUNK",
                            "message": chunk_message
                        }
                    )
                
                # Record metrics
                self.metrics_collector.record_chunk_propagation(
                    block["block_id"], 0, target_node_idx,
                    self.event_queue.current_time + avg_delay
                )
                
                # Simulate block completion
                completion_time = self.event_queue.current_time + avg_delay
                self.metrics_collector.record_block_completion(
                    block["block_id"], target_node_idx, completion_time
                )
                
                nodes_received.add(target_node_idx)
                
                # Update delay breakdown
                self.real_time_metrics["delay_breakdown"] = {
                    "computational": delay_info["computational_delay"],
                    "rtt": delay_info["rtt_delay"],
                    "bandwidth": delay_info["bandwidth_delay"]
                }
            else:
                logger.warning(f"Could not calculate delay for block broadcast from {source_node_idx} to {target_node_idx}")
        
        # Update real-time metrics
        if total_delays:
            self.real_time_metrics["current_latency"] = sum(total_delays) / len(total_delays)
        
        coverage = len(nodes_received) / (self.node_count - 1) * 100 if (self.node_count - 1) > 0 else 0
        self.real_time_metrics["current_coverage"] = coverage
        
        # Get congestion metrics
        congestion_metrics = self.delay_calculator.get_system_congestion_metrics()
        self.real_time_metrics["current_congestion"] = congestion_metrics["average_utilization"]
        
        # Record block completion
        self.real_time_metrics["blocks_completed"][block["block_id"]] = {
            "completion_time": self.event_queue.current_time + max(total_delays) if total_delays else self.event_queue.current_time,
            "nodes_received": len(nodes_received),
            "coverage": coverage
        }
        
        # Send real-time update to frontend
        self._send_update("block_progress", {
            "blockId": block["block_id"],
            "completedNodes": len(nodes_received),
            "totalNodes": self.node_count - 1,
            "coverage": coverage,
            "latency": self.real_time_metrics["current_latency"],
            "congestion": self.real_time_metrics["current_congestion"],
            "delayBreakdown": self.real_time_metrics["delay_breakdown"]
        })
        
    def _handle_timeout_event(self, event: Event):
        """Handle timeout events."""
        timeout_type = event.data.get("timeout_type")
        
        logger.info(f"🕐 Timeout event triggered: {timeout_type}")
        
        if timeout_type == "peer_discovery_complete":
            self._complete_peer_discovery()
        elif timeout_type == "broadcasting_complete":
            self._complete_broadcasting()
    
    def _complete_peer_discovery(self):
        """Complete peer discovery phase."""
        logger.info("Peer discovery phase completed")
        logger.info(f"DEBUG: _complete_peer_discovery called at simulation time {self.event_queue.current_time:.3f}")
        
        # Calculate nodes ready
        nodes_ready = sum(1 for flag in self.peer_discovery_flags.values() if flag)
        coverage = (nodes_ready / len(self.nodes)) * 100 if len(self.nodes) > 0 else 0.0
        
        logger.info(f"Peer discovery completed: {nodes_ready}/{len(self.nodes)} nodes ready ({coverage:.1f}%)")
        
        # Kadcast makalesine uygun: Daha detaylı istatistikler
        total_peers = sum(len(node.peers) for node in self.nodes.values())
        avg_peers = total_peers / len(self.nodes) if len(self.nodes) > 0 else 0
        total_buckets = sum(node.get_filled_bucket_count() for node in self.nodes.values())
        avg_buckets = total_buckets / len(self.nodes) if len(self.nodes) > 0 else 0
        
        logger.info(f"Discovery stats: Avg peers per node: {avg_peers:.1f}, Avg filled buckets: {avg_buckets:.1f}")
        
        self._send_update("peer_discovery_complete", {
            "nodesReady": nodes_ready,
            "totalNodes": len(self.nodes),
            "coverage": coverage,
            "avgPeers": avg_peers,
            "avgBuckets": avg_buckets
        })
        
        # Start broadcasting phase
        self.start_broadcasting()
    
    def _complete_broadcasting(self):
        """Complete broadcasting phase."""
        logger.info("Broadcasting phase completed")
        self.phase = "completed"
        self.running = False
        
        # Calculate final metrics
        final_metrics = self._calculate_final_metrics()
        
        self._send_update("simulation_complete", {
            "metrics": final_metrics,
            "simulationId": self.simulation_id
        })
    
    def _calculate_final_metrics(self) -> Dict[str, Any]:
        """Calculate final simulation metrics."""
        # Calculate average latency from all blocks
        all_latencies = []
        all_coverages = []
        
        for block_id, completion_info in self.real_time_metrics["blocks_completed"].items():
            if "coverage" in completion_info:
                all_coverages.append(completion_info["coverage"])
        
        avg_latency = sum(all_latencies) / len(all_latencies) if all_latencies else 0.0
        avg_coverage = sum(all_coverages) / len(all_coverages) if all_coverages else 0.0
        avg_congestion = self.real_time_metrics["current_congestion"] 
        
        return {
            "totalBlocks": self.real_time_metrics["blocks_generated"],
            "avgLatency": avg_latency,
            "avgCoverage": avg_coverage,
            "avgCongestion": avg_congestion,
            "totalChunks": self.real_time_metrics["chunks_transmitted"],
            "simulationTime": self.current_time,
            "delayBreakdown": self.real_time_metrics["delay_breakdown"],
            "congestionMetrics": self.delay_calculator.get_system_congestion_metrics()
        }
    
    def on_block_completed(self, node_index: int, block_id: int, completion_time: float):
        """Called when a node completes a block."""
        if block_id not in self.real_time_metrics["blocks_completed"]:
            self.real_time_metrics["blocks_completed"][block_id] = {
                "nodes": [],
                "completion_time": completion_time
            }
        
        self.real_time_metrics["blocks_completed"][block_id]["nodes"].append({
            "node_index": node_index,
            "completion_time": completion_time
        })
        
        # Send real-time update
        completed_nodes = len(self.real_time_metrics["blocks_completed"][block_id]["nodes"])
        coverage = completed_nodes / len(self.nodes) * 100
        
        self._send_update("block_progress", {
            "blockId": block_id,
            "completedNodes": completed_nodes,
            "totalNodes": len(self.nodes),
            "coverage": coverage,
            "latency": self.real_time_metrics["current_latency"],
            "congestion": self.real_time_metrics["current_congestion"],
            "delayBreakdown": self.real_time_metrics["delay_breakdown"]
        })

    def mark_node_discovery_complete(self, node_index: int):
        """Node peer discovery işlemini tamamladığında çağrılır."""
        if node_index not in self.peer_discovery_flags:
            logger.warning(f"Node {node_index} not found in peer_discovery_flags")
            return
            
        if not self.peer_discovery_flags[node_index]:  # Sadece bir kez işaretle
            self.peer_discovery_flags[node_index] = True
            completed_count = sum(1 for flag in self.peer_discovery_flags.values() if flag)
            
            # Kadcast makalesine uygun: Daha detaylı log
            node = self.nodes.get(node_index)
            if node:
                peer_count = len(node.peers)
                bucket_count = node.get_filled_bucket_count()
                logger.info(f"Node {node_index} marked discovery complete. ({completed_count}/{len(self.peer_discovery_flags)} completed) - Peers: {peer_count}, Buckets: {bucket_count}")
            else:
                logger.info(f"Node {node_index} marked discovery complete. ({completed_count}/{len(self.peer_discovery_flags)} completed)")
            
            # Tüm node'lar tamamlandıysa peer discovery'yi bitir
            if all(self.peer_discovery_flags.values()):
                logger.info("✔️ All nodes completed peer discovery")
                self._complete_peer_discovery()

    def _handle_find_node_event(self, event: Event):
        sender_id = event.data.get("sender_id")
        receiver_id = event.data.get("receiver_node_id")
        message = event.data.get("message") 

        receiver_node = self._get_node_by_id(receiver_id)

        if receiver_node and isinstance(message, FindNodeMessage):
            receiver_node.handle_find_node(message)
        else:
            logger.warning(f"FIND_NODE event: Missing receiver node or invalid message type for IDs {sender_id} -> {receiver_id}")

    def _handle_nodes_response_event(self, event: Event):
        sender_id = event.data.get("sender_id")
        receiver_id = event.data.get("receiver_node_id")
        message = event.data.get("message")

        receiver_node = self._get_node_by_id(receiver_id)

        if receiver_node and isinstance(message, NodesMessage):
            receiver_node.handle_nodes(message)
        else:
            logger.warning(f"NODES_RESPONSE event: Receiver node not found or invalid message type for ID {receiver_id}")

    def _handle_message_send_event(self, event: Event):
        message = event.data.get("message")
        receiver_node_id_str = event.data.get("receiver_node_id")
        
        logger.debug(f"Handling MESSAGE_SEND event. Message type: {type(message).__name__}, Receiver: {receiver_node_id_str}")

        receiver_node = self._get_node_by_id(receiver_node_id_str)
        if not receiver_node:
            logger.warning(f"MESSAGE_SEND event: Receiver node not found for ID {receiver_node_id_str}")
            return

        # Message type'a göre uygun handler'ı çağır
        if isinstance(message, PingMessage):
            receiver_node.handle_ping(message)
        elif isinstance(message, PongMessage):
            receiver_node.handle_pong(message)
        elif isinstance(message, FindNodeMessage):
            logger.debug(f"Dispatching FindNodeMessage to node {receiver_node.node_index}")
            receiver_node.handle_find_node(message)
        elif isinstance(message, NodesMessage):
            logger.debug(f"Dispatching NodesMessage to node {receiver_node.node_index}")
            receiver_node.handle_nodes(message)
        elif isinstance(message, ChunkMessage):
            receiver_node.receive_chunk(message, sender_id=message.sender_id)
        elif isinstance(message, ReqMessage):
            logger.warning("REQ message received but no handler implemented yet")
        else:
            logger.warning(f"Unknown message type received: {type(message).__name__}")

    def send_message(self, sender_id: str, receiver_id: str, message):
        """Bir düğümden diğerine mesaj gönderme işlemini simüle eder."""
        sender_node = self._get_node_by_id(sender_id)
        receiver_node = self._get_node_by_id(receiver_id)

        if not sender_node or not receiver_node:
            logger.warning(f"[SEND FAIL] Sender or receiver not found: {sender_id} -> {receiver_id}")
            return

        delay = self.delay_calculator.calculate_rtt_delay(sender_node.city, receiver_node.city)
        
        # Gecikmenin pozitif olduğundan emin ol
        if delay is None or delay <= 0:
            logger.error(f"[SEND FAIL] Invalid delay ({delay}) from {sender_node.city} to {receiver_node.city}. Using minimum delay.")
            delay = 0.000001  # Minimum pozitif gecikme

        # Mesaj tipini belirle
        message_type = message.__class__.__name__.replace("Message", "").upper()

        self.event_queue.schedule_event(
            EventType.MESSAGE_SEND,
            node_id=receiver_node.node_index,
            delay=delay,
            data={
                "sender_id": sender_id,
                "receiver_node_id": receiver_id,
                "message_type": message_type,
                "message": message
            }
        )
        logger.debug(f"Scheduled {message_type} from {sender_id} to {receiver_id} with delay {delay:.6f}")

