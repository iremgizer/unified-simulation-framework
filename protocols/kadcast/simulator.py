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
        self.config = self._setup_default_config(config)
        self.simulation_id = str(uuid.uuid4())
        self.node_count = self.config.get("nodeCount", 100)
        self.k_bucket_size = self.config.get("kBucketSize", 20)
        
        # Broadcasting configuration
        self.block_size = self.config.get("blockSize", 1024 * 1024)  # 1MB default
        self.broadcast_frequency = self.config.get("broadcastFrequency", 10)  # 10 blocks/minute default
        self.chunk_size = self.config.get("chunkSize", 1024)  # 1KB chunks default
        self.fec_ratio = self.config.get("fecRatio", 0.2)  # 20% FEC default
        self.broadcasting_duration = config.get('duration', 300.0)  
        self.beta = self.config.get("beta", 3)  # Default beta = 3

        # Performance configuration
        self.performance_distribution = self.config.get("nodePerformanceDistribution", {})
        self.performance_variation = self.config.get("nodePerformanceVariation", 0.1)
        self.deterministic_performance = self.config.get("deterministicPerformance", True)
        self.random_seed = self.config.get("randomSeed", 42)

      
        self.early_termination_enabled = self.config.get("early_termination_enabled", True)
        
        # 🔧 ADAPTIVE THRESHOLDS: Büyük ağlarda daha agresif early termination
        if self.node_count >= 64:
            # 64+ node ağlarda daha agresif settings
            self.coverage_threshold = min(92.0, self.config.get("coverage_threshold", 95.0))
            self.coverage_check_interval = min(2.0, self.config.get("coverage_check_interval", 5.0))
            logger.info(f" Large network detected ({self.node_count} nodes): Aggressive early termination")
            logger.info(f"   Coverage threshold: {self.coverage_threshold}%")
            logger.info(f"   Check interval: {self.coverage_check_interval}s")
        else:
            # Normal settings for smaller networks
            self.coverage_threshold = self.config.get("coverage_threshold", 95.0)
            self.coverage_check_interval = self.config.get("coverage_check_interval", 5.0)
        
        self.early_terminated_blocks = set()  # Erken durdurulan block'lar
        self.last_coverage_check = 0.0        # Son kontrol zamanı
        self._last_cleanup_time = 0.0         # Memory cleanup tracking

        self.nodes: Dict[int, KadcastNode] = {}
        self.nodes_by_id: Dict[str, KadcastNode] = {}
        self.current_time = 0.0
        self.running = False
        self.phase = "initialization"

        self.event_queue = EventQueue()
        self.engine = SimulationEngine(self.event_queue)

        data_dir = os.path.join(src_dir, 'data')
        self.geo_provider = GeoDataProvider(data_dir)
        
        # Initialize network delay calculator with performance config
        delay_config = {
            "node_performance_variation": self.performance_variation,
            "performance_distribution": self.performance_distribution,
            "random_seed": self.random_seed if self.deterministic_performance else None
        }
        self.delay_calculator = NetworkDelayCalculator(self.geo_provider, config=delay_config)
        
        self.metrics_collector = Metrics(self.node_count)
        self.metrics_collector.set_protocol_name("kadcast")

        self.block_generator = RealisticBlockGenerator(seed=self.random_seed)
        self.chunk_generator = RealisticChunkGenerator()

        # Block tracking
        self.blocks_generated = 0
        self.blocks_scheduled = []
        self.block_metrics = {}  # Per-block metrics
        self.active_blocks = set()  # Currently propagating blocks
        self.generated_blocks_content = {} 
        
        # Real-time metrics
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

        # Node performance assignments (will be set during initialization)
        self.node_performance_assignments = {}

        # Peer discovery kontrolü için yeni alan (korundu ama kullanılmayacak)
        self.peer_discovery_flags = {}
        self.peer_discovery_timeout = 180.0  # 3 dakika timeout
        
        self.update_callback = None
        
        # Early termination log
        termination_status = "ENABLED" if self.early_termination_enabled else "DISABLED"
        logger.info(f"KadcastSimulator initialized with {self.node_count} nodes (CENTRALIZED + CHUNK-BASED BROADCASTING + PERFORMANCE MODELING)")
        logger.info(f" Early Termination: {termination_status} (Threshold: {self.coverage_threshold}%)")

    def _setup_default_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Setup default configuration with performance parameters."""
        default_config = {
            "nodeCount": 100,
            "kBucketSize": 20,
            "blockSize": 1024 * 1024,
            "broadcastFrequency": 10,
            "chunkSize": 1024,
            "fecRatio": 0.2,
            "beta": 3,
            "nodePerformanceDistribution": {
                "high_performance": 0.15,    # 15% high-end nodes
                "average_performance": 0.70, # 70% average nodes  
                "low_performance": 0.15      # 15% low-end nodes
            },
            "nodePerformanceVariation": 0.1,  # ±10% random variation
            "deterministicPerformance": True,   # Same seed = same assignments
            "randomSeed": 42,
          
            "early_termination_enabled": True,
            "coverage_threshold": 95.0,
            "coverage_check_interval": 5.0
        }
        
        # Update with provided config
        default_config.update(config)
        return default_config

    def calculate_current_coverage(self, block_id: int) -> float:
        """Şu andaki coverage'ı hesapla"""
        if block_id not in self.block_metrics:
            return 0.0
            
        completed_nodes = self.block_metrics[block_id]["nodes_completed"]
        total_nodes = self.node_count
        coverage = (completed_nodes / total_nodes) * 100.0
        
        return coverage
        
        

    def check_and_terminate_if_needed(self, current_time: float):
        """Basit ve hızlı early termination"""
        if not self.early_termination_enabled:
            return
            
        # Her 1 saniyede kontrol et
        if current_time - self.last_coverage_check < 1.0:
            return
            
        self.last_coverage_check = current_time
        
        for block_id in list(self.active_blocks):
            if block_id in self.early_terminated_blocks:
                continue
                
            coverage = self.calculate_current_coverage(block_id)
            
            # 90% coverage'da terminate et
            if coverage >= 90.0:
                logger.info(f" TERMINATING Block {block_id}: {coverage:.1f}%")
                self.terminate_block_propagation(block_id)
                self.early_terminated_blocks.add(block_id)
                

    def terminate_block_propagation(self, block_id: int):
   
        #  Use the optimized event queue removal method
        def is_block_related(event):
            return self.is_block_related_event(event, block_id)
        
        # Bu metod artık optimize edildi (single pass + single heapify)
        terminated_events = self.event_queue.remove_events_by_condition(is_block_related)
        
        # Active blocks'tan çıkar
        if block_id in self.active_blocks:
            self.active_blocks.remove(block_id)
        
        logger.info(f"Terminated {terminated_events} orphan events for block {block_id}")
        
        return terminated_events


    def is_block_related_event(self, event: Event, block_id: int) -> bool:
        """Event'in belirli bir block ile ilgili olup olmadığını kontrol et"""
        try:
            # MESSAGE_SEND eventleri kontrol et
            if event.event_type == EventType.MESSAGE_SEND:
                message = event.data.get("message")
                if hasattr(message, 'block_id') and message.block_id == block_id:
                    return True
                    
            # CHUNK_FORWARD eventleri kontrol et  
            elif event.event_type == EventType.CHUNK_FORWARD:
                if event.data.get("block_id") == block_id:
                    return True
                    
            # BLOCK_PROCESSING eventleri kontrol et
            elif event.event_type == EventType.BLOCK_PROCESSING:
                if event.data.get("block_id") == block_id:
                    return True
                    
            # CHUNK_SEND eventleri kontrol et
            elif event.event_type == EventType.CHUNK_SEND:
                if event.data.get("block_id") == block_id:
                    return True
                    
            # CHUNK_RECEIVE eventleri kontrol et
            elif event.event_type == EventType.CHUNK_RECEIVE:
                message = event.data.get("message")
                if hasattr(message, 'block_id') and message.block_id == block_id:
                    return True
                    
            return False
        except Exception:
            return False

    def _assign_node_performances(self) -> Dict[int, str]:
        """
        Assign performance types to all nodes based on distribution.
        
        Returns:
            Dictionary mapping node_id -> performance_type_string
        """
        assignments = {}
        
        if self.deterministic_performance:
            # Use deterministic seed for reproducible assignments
            random.seed(self.random_seed)
        
        # Create weighted list based on distribution
        performance_types = []
        for perf_type, probability in self.performance_distribution.items():
            count = int(self.node_count * probability)
            performance_types.extend([perf_type] * count)
        
        # Fill remaining slots with average performance
        while len(performance_types) < self.node_count:
            performance_types.append("average_performance")
        
        # Shuffle for random assignment
        random.shuffle(performance_types)
        
        # Assign to nodes
        for node_index in range(self.node_count):
            assignments[node_index] = performance_types[node_index]
        
        # Log assignment statistics
        type_counts = {}
        for perf_type in assignments.values():
            type_counts[perf_type] = type_counts.get(perf_type, 0) + 1
        
        logger.info(" Node Performance Assignment:")
        for perf_type, count in type_counts.items():
            percentage = (count / self.node_count) * 100
            logger.info(f"   - {perf_type}: {count} nodes ({percentage:.1f}%)")
        
        return assignments

    def set_update_callback(self, callback):
        self.update_callback = callback

    def _send_update(self, update_type: str, data: Dict[str, Any]):
        if self.update_callback:
            self.update_callback(update_type, data)

    def initialize_network(self) -> bool:
        try:
            self.phase = "network_initialization"
            
            # Assign performance types to nodes
            self.node_performance_assignments = self._assign_node_performances()
            
            # Update delay calculator with performance assignments
            delay_config = self.delay_calculator.computational_calculator.config.copy()
            delay_config["node_performance_assignments"] = self.node_performance_assignments
            self.delay_calculator.computational_calculator.set_node_performance_assignments(
                self.node_performance_assignments
            )
            
            node_assignments = self.geo_provider.assign_nodes_to_regions_and_cities(self.node_count)
            used_ids = set()

            def generate_kadcast_node_id() -> str:
                """KADCAST ORIGINAL: 64-bit uniform random ID generation (makaledeki L=64 uyumlu)"""
                # Gerçek Kadcast: RandomIDInInterval(pow2(0), pow2(KAD_ID_LEN))
                # [1, 2^64) aralığında uniform random
                while True:
                    # 64-bit uniform random ID (1'den 2^64-1'e kadar)
                    random_id = random.randint(1, 0xFFFFFFFFFFFFFFFF)  # 64-bit range
                    node_id = f"{random_id:016x}"  # 16 hex digits for 64-bit
                    
                    if node_id not in used_ids:
                        used_ids.add(node_id)
                        return node_id

            # Node'ları oluştur
            all_node_ids = []
            for node_index in range(self.node_count):
                assignment = node_assignments[node_index]
                
                # KADCAST ORIGINAL: 64-bit uniform random ID generation
                node_id = generate_kadcast_node_id()
                
                all_node_ids.append(node_id)
                is_bootstrap = node_index < 3
                
                # Get performance type for this node
                performance_type = self.node_performance_assignments.get(node_index, "average_performance")

                node = KadcastNode(
                    node_index=node_index,
                    node_id=node_id,
                    city=assignment["city"],
                    coordinates=assignment["coordinates"],
                    performance_type=performance_type,  # ← NEW PARAMETER
                    is_bootstrap=is_bootstrap,
                    beta=self.beta
                )
                node.k = self.k_bucket_size
                node.set_simulation(self)
                self.nodes[node_index] = node
                self.nodes_by_id[node.node_id] = node
                
                # Debug log (ilk 10 node için)
                if node_index < 10:
                    logger.debug(f"Node {node_index}: ID={node_id}, City={assignment['city']}, "
                               f"Performance={performance_type}, Bootstrap={is_bootstrap}")

            # MERKEZI YAKLAŞIM: Tüm node'lara ağdaki tüm node'ların bilgisini ver
            logger.info(" CENTRALIZED APPROACH: Giving all nodes complete network knowledge...")
            
            for node_index, node in self.nodes.items():
                # Her node'a diğer tüm node'ların ID'lerini ver
                other_node_ids = [nid for nid in all_node_ids if nid != node.node_id]
                node.add_all_network_peers(other_node_ids)
                
                # Bootstrap bilgilerini de ekle (eski kod uyumluluğu için)
                if not node.is_bootstrap:
                    for bootstrap_index in range(3):
                        if bootstrap_index in self.nodes:
                            bootstrap_node = self.nodes[bootstrap_index]
                            node.bootstrap_nodes.add(bootstrap_node.node_id)

            # Peer discovery flags'i başlat (eski kod uyumluluğu için)
            self.peer_discovery_flags = {idx: False for idx in self.nodes}

            self._setup_network_connections()

            # ID distribution analizi (debug için)
            self._analyze_id_distribution()

            # Merkezi yaklaşımda discovery completion kontrolü
            self._check_all_nodes_completion()

            self._send_update("network_initialized", {
                "nodeCount": self.node_count,
                "bootstrapNodes": [0, 1, 2],
                "geoDistribution": node_assignments,
                "performanceDistribution": {
                    "assignments": self.node_performance_assignments,
                    "distribution": self.performance_distribution
                },
                "broadcastConfig": {
                    "blockSize": self.block_size,
                    "frequency": self.broadcast_frequency,
                    "chunkSize": self.chunk_size,
                    "fecRatio": self.fec_ratio,
                    "duration": self.broadcasting_duration
                }
            })

            logger.info(f"Network initialized with {self.node_count} nodes")
            return True
        
        except Exception as e:
            logger.error(f"Failed to initialize network: {e}")
            return False
        
    def _check_all_nodes_completion(self):
        """Tüm node'ların discovery completion durumunu kontrol et."""
        completed_count = 0
        for node in self.nodes.values():
            if node.discovery_complete:
                completed_count += 1
                self.peer_discovery_flags[node.node_index] = True
        
        logger.info(f" Initial completion status: {completed_count}/{len(self.nodes)} nodes completed discovery")
        
        # İstatistikleri logla
        total_peers = sum(len(node.peers) for node in self.nodes.values())
        avg_peers = total_peers / len(self.nodes) if self.nodes else 0
        total_buckets = sum(node.get_filled_bucket_count() for node in self.nodes.values())
        avg_buckets = total_buckets / len(self.nodes) if self.nodes else 0
        
        logger.info(f"Network stats: Avg peers per node: {avg_peers:.1f}, Avg filled buckets: {avg_buckets:.1f}")

    def _analyze_id_distribution(self):
        """KADCAST ORIGINAL: ID distribution analizini yap (bucket dağılımı kontrolü)"""
        try:
            logger.info(" Analyzing ID distribution (Kadcast original uniform random, 64-bit)...")
            
            # Her node için bucket distribution'ı kontrol et
            sample_nodes = list(self.nodes.values())[:5]  # İlk 5 node'u sample al
            
            for node in sample_nodes:
                bucket_counts = [0] * 64  # 64-bit ID için 64 bucket
                
                for peer_id in node.peers:
                    try:
                        peer_id_int = int(peer_id, 16)
                        node_id_int = int(node.node_id, 16)
                        distance = peer_id_int ^ node_id_int
                        
                        if distance > 0:
                            bucket_index = min(63, distance.bit_length() - 1)  # 64-bit için max 63
                            bucket_counts[bucket_index] += 1
                    except ValueError:
                        continue
                
                filled_buckets = sum(1 for count in bucket_counts if count > 0)
                total_peers = sum(bucket_counts)
                
                logger.info(f"Node {node.node_index} ({node.node_id}): {total_peers} peers, {filled_buckets}/64 buckets filled")
                logger.debug(f"  Bucket distribution: {bucket_counts}")
                
        except Exception as e:
            logger.error(f"Error analyzing ID distribution: {e}")

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
        """Peer discovery phase (korundu ama kullanılmayacak)"""
        try:
            logger.info("Starting peer discovery phase...")
            self.phase = "peer_discovery"
            
            # Timeout event'ini zamanla
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
                        
                    else:
                        logger.warning(f"No bootstrap nodes found for node {node.node_id} to start peer discovery.")

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
        """Chunk-based broadcasting phase'ini başlat"""
        try:
            logger.info(" Starting chunk-based broadcasting phase...")
            self.phase = "broadcasting"

            duration = self.config.get('duration', 300.0)
            
            # Calculate block generation schedule
            total_blocks = int((duration / 60.0) * self.broadcast_frequency)
            block_interval = duration / total_blocks if total_blocks > 0 else 60.0
            
            logger.info(f"Broadcasting configuration:")
            logger.info(f"   - Duration: {duration}s ")
            logger.info(f"   - Frequency: {self.broadcast_frequency} blocks/minute")
            logger.info(f"   - Total blocks: {total_blocks}")
            logger.info(f"🔧 DEBUG: Block interval = {block_interval}")
            
            # Schedule block generation events
            current_time = 1.0  # Start after 1 second
            for block_id in range(total_blocks):
                # Select random source node
                source_node_index = random.randint(0, self.node_count - 1)
                
                self.event_queue.schedule_event(
                    EventType.BLOCK_GENERATE,
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
            

            extra_time = 60.0 if self.block_size > 50000 else 30.0  # Extra time for large blocks
            timeout_delay = duration + extra_time
        
            # Schedule broadcasting completion timeout
            self.event_queue.schedule_event(
                EventType.TIMEOUT,
                node_id=-1,
                delay= timeout_delay,  # 30 second buffer
                data={"timeout_type": "broadcasting_complete"}
            )
            
            # Send update
            self._send_update("broadcasting_started", {
                "duration": self.broadcasting_duration,
                "frequency": self.broadcast_frequency,
                "totalBlocks": total_blocks,
                "phase": "broadcasting"
            })
            
            logger.info(f" Scheduled {total_blocks} blocks for generation")
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
        """Run the complete simulation (centralized approach) with enhanced optimizations."""
        try:
            self.running = True
            logger.info(f" Starting OPTIMIZED Kadcast simulation {self.simulation_id}")
            
            if self.early_termination_enabled:
                logger.info(f" Early termination enabled: {self.coverage_threshold}% threshold, {self.coverage_check_interval}s intervals")
            
            # Register event handlers
            self._register_event_handlers()
            
            # Phase 1: Initialize network (centralized)
            if not self.initialize_network():
                raise Exception("Network initialization failed")
            
            # Phase 2: Skip peer discovery, go directly to broadcasting
            if not self.start_broadcasting():
                raise Exception("Broadcasting start failed")
            
            # Enhanced event loop with optimizations
            total_simulation_time = self.broadcasting_duration + 20.0
            logger.info(f"Running simulation for {total_simulation_time} seconds...")
            
            event_count = 0
            last_progress_log = 0.0
            
            while (self.running and 
                self.event_queue.current_time < total_simulation_time and
                not self.event_queue.is_empty()):
                
                # Process event
                event = self.event_queue.pop()
                if event:
                    self.engine._process_event(event)
                    event_count += 1
                    
                    #  Early termination checks
                    self.check_and_terminate_if_needed(self.event_queue.current_time)
                    
                    #  Periodic memory cleanup
                    self._periodic_memory_cleanup(self.event_queue.current_time)
                    
                    # Progress logging (every 10000 events or 30 seconds)
                    current_time = self.event_queue.current_time
                    if (event_count % 10000 == 0 or 
                        current_time - last_progress_log > 30.0):
                        
                        queue_size = len(self.event_queue.events)
                        active_blocks = len(self.active_blocks)
                        terminated_blocks = len(self.early_terminated_blocks)
                        
                        logger.info(f" Progress: t={current_time:.1f}s, events={event_count}, "
                                f"queue={queue_size}, active_blocks={active_blocks}, "
                                f"terminated={terminated_blocks}")
                        
                        last_progress_log = current_time
                    
                    # Update callback
                    if self.update_callback:
                        self.update_callback(self._get_real_time_status())
            
            logger.info(" Simulation completed successfully")
            
            # Final statistics
            if self.early_termination_enabled:
                termination_stats = self.metrics_collector.get_termination_statistics()
                if termination_stats.get("enabled", False):
                    logger.info(f" Early termination saved {termination_stats['total_events_saved']} events")
                    logger.info(f"{termination_stats['blocks_terminated']} blocks terminated early")
            
            return True
            
        except Exception as e:
            logger.error(f" Simulation failed: {e}")
            self.running = False
            return False

    def _get_real_time_status(self) -> Dict[str, Any]:
        """Get real-time simulation status."""
        return {
            "simulation_time": self.event_queue.current_time,
            "phase": self.phase,
            "blocks_generated": self.blocks_generated,
            "active_blocks": len(self.active_blocks),
            "early_terminated_blocks": len(self.early_terminated_blocks),
            "chunks_transmitted": self.real_time_metrics["chunks_transmitted"],
            "queue_size": len(self.event_queue.events)
        }
  
    def _handle_block_generate_event(self, event: Event):
        """Block generation event'ini işle - WITH COMPUTATIONAL DELAYS AND REAL DATA STORAGE"""
        try:
            block_id = event.data.get("block_id")
            source_node_index = event.data.get("source_node_index")
            block_size = event.data.get("block_size", self.block_size)
            
            logger.info(f" Generating block {block_id} from node {source_node_index} with FEC ratio {self.fec_ratio}")
            
            # Generate realistic block
            prev_hash = f"0x{'0' * 64}" if block_id == 0 else f"0x{hash(str(block_id - 1)):016x}{'0' * 48}"
            header, transactions = self.block_generator.generate_block(
                block_id=block_id,
                prev_hash=prev_hash,
                block_size_bytes=block_size
            )
            
            # 🔧 NEW: Store real block content for JSON export
            self.generated_blocks_content[block_id] = {
                'block_id': block_id,
                'block_size': len(self.block_generator.serialize_block(header, transactions)),
                'block_hash': header.get('block_hash', f"0x{hash(str(block_id)):064x}"),
                'timestamp': header.get('timestamp', self.event_queue.current_time),
                'transactions': transactions,  # REAL TRANSACTIONS!
                'transaction_count': len(transactions),
                'merkle_root': header.get('merkle_root', f"merkle_{block_id:032x}"),
                'prev_hash': header.get('prev_hash', prev_hash),
                'nonce': header.get('nonce', block_id * 12345),
                'difficulty': header.get('difficulty', 1.0),
                'source': 'RealisticBlockGenerator'  # Mark as real
            }
            
            # Serialize block
            block_data = self.block_generator.serialize_block(header, transactions)
            actual_size = len(block_data)
            
            #  ADD BLOCK GENERATION DELAY
            generation_delay = self.delay_calculator.calculate_block_generation_delay(
                source_node_index, transactions, actual_size
            )
            
            # ADD CHUNKIFY DELAY
            chunkify_delay = self.delay_calculator.calculate_chunkify_delay(
                source_node_index, actual_size, self.chunk_size, self.fec_ratio
            )
            
            total_generation_delay = generation_delay + chunkify_delay
            
            logger.debug(f"Block {block_id} generation delays: "
                        f"generation={generation_delay:.4f}s, chunkify={chunkify_delay:.4f}s, "
                        f"total={total_generation_delay:.4f}s")
            
            # Schedule the actual chunk broadcasting after generation delay
            self.event_queue.schedule_event(
                EventType.CHUNK_SEND,
                node_id=source_node_index,
                delay=total_generation_delay,
                data={
                    "block_id": block_id,
                    "source_node_index": source_node_index,
                    "block_data": block_data,
                    "actual_size": actual_size,
                    "transactions": transactions
                }
            )
            
        except Exception as e:
            logger.error(f"Error in block generation: {e}")

    def _handle_chunk_send_event(self, event: Event):
        """Handle chunk sending after generation delay."""
        try:
            block_id = event.data.get("block_id")
            source_node_index = event.data.get("source_node_index")
            block_data = event.data.get("block_data")
            actual_size = event.data.get("actual_size")
            transactions = event.data.get("transactions")
            
            # Generate chunks with FEC
            chunks = self.chunk_generator.chunkify_block(
                data=block_data,
                chunk_size=self.chunk_size,
                block_id=block_id,
                fec_ratio=self.fec_ratio
            )
            
            logger.info(f" Block {block_id}: {actual_size} bytes → {len(chunks)} chunks")
            
            # Initialize block metrics
            self.block_metrics[block_id] = {
                "block_id": block_id,
                "source_node": source_node_index,
                "start_time": self.event_queue.current_time,
                "block_size": actual_size,
                "total_chunks": len(chunks),
                "chunks_sent": 0,
                "nodes_completed": 0,
                "completion_times": {},
                "chunk_propagation": {},
                "fec_ratio": self.fec_ratio,
                "transactions": transactions
            }
            
            self.active_blocks.add(block_id)
            self.blocks_generated += 1
            self.real_time_metrics["blocks_generated"] = self.blocks_generated
            
            # Start chunk broadcasting from source node
            source_node = self.nodes[source_node_index]
            self._start_chunk_broadcasting(source_node, chunks, block_id)
            
            # Update metrics
            self.metrics_collector.report_block_start(block_id, source_node_index, self.event_queue.current_time )
            
        except Exception as e:
            logger.error(f"Error in chunk sending: {e}")

    def _start_chunk_broadcasting(self, source_node: KadcastNode, chunks: List[bytes], block_id: int):
        """Chunk broadcasting'i başlat - WITH TRANSMISSION DELAYS"""
        try:
            logger.info(f" Starting chunk broadcasting for block {block_id} from node {source_node.node_index}")
            
            # Get broadcast peers using Kadcast routing
            initial_height = 64  # 64-bit için height 64
            broadcast_peers = source_node.get_broadcast_peers(block_id, initial_height)

            logger.info(f"Broadcasting to {len(broadcast_peers)} peers with height {initial_height}")
            
            # Send each chunk to broadcast peers
            for chunk_bytes in chunks:
                chunk_message = ChunkMessage(sender_id=source_node.node_id, chunk_bytes=chunk_bytes)
                
                for peer_id in broadcast_peers:
                    #  ADD TRANSMISSION DELAY (RTT + Bandwidth)
                    peer_node = self._get_node_by_id(peer_id)
                    if peer_node:
                        transmission_delays = self.delay_calculator.calculate_transmission_delay(
                            source_node.node_index,
                            peer_node.node_index,
                            source_node.city,
                            peer_node.city,
                            len(chunk_bytes),
                            self.event_queue.current_time
                        )
                        
                        transmission_delay = transmission_delays["transmission_delay"]
                        
                        # Schedule chunk transmission with realistic delay
                        self.event_queue.schedule_event(
                            EventType.MESSAGE_SEND,
                            node_id=peer_node.node_index,
                            delay=transmission_delay,
                            data={
                                "sender_id": source_node.node_id,
                                "receiver_node_id": peer_id,
                                "message_type": "CHUNK",
                                "message": chunk_message,
                                "height": initial_height - 1
                            }
                        )
                
                # Update metrics
                self.block_metrics[block_id]["chunks_sent"] += len(broadcast_peers)
                self.real_time_metrics["chunks_transmitted"] += len(broadcast_peers)
            
        except Exception as e:
            logger.error(f"Error starting chunk broadcasting: {e}")

    def _handle_timeout_event(self, event: Event):
        """Handle timeout events."""
        timeout_type = event.data.get("timeout_type")
        
        logger.info(f" Timeout event triggered: {timeout_type}")
        
        if timeout_type == "peer_discovery_complete":
            self._complete_peer_discovery()
        elif timeout_type == "broadcasting_complete":
            self._complete_broadcasting()
    
    def _complete_peer_discovery(self):
        """Complete peer discovery phase (korundu ama kullanılmayacak)."""
        logger.info("Peer discovery phase completed")
        
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
            "phase": "completed",
            "finalMetrics": final_metrics,
            "simulationTime": self.event_queue.current_time
        })
    
    def _calculate_final_metrics(self) -> Dict[str, Any]:
        """Calculate final simulation metrics."""
        try:
            logger.info("Calculating final metrics...")
            
            # Calculate final statistics
            total_blocks = len(self.block_metrics)
            total_chunks = sum(metrics["chunks_sent"] for metrics in self.block_metrics.values())
            
            # Calculate average latency and coverage
            avg_latency = 0.0
            avg_coverage = 0.0
            latencies = []
            coverages = []
            
            for block_id, metrics in self.block_metrics.items():
                if metrics["completion_times"]:
                    start_time = metrics["start_time"]
                    completion_times = list(metrics["completion_times"].values())
                    avg_completion_time = sum(completion_times) / len(completion_times)
                    latency = avg_completion_time - start_time
                    latencies.append(latency)
                    
                    coverage = (len(completion_times) / self.node_count) * 100
                    coverages.append(coverage)
            
            if latencies:
                avg_latency = sum(latencies) / len(latencies)
            if coverages:
                avg_coverage = sum(coverages) / len(coverages)
            
            # Bandwidth utilization
            bandwidth_metrics = self.delay_calculator.get_system_congestion_metrics()
            
            # Performance distribution summary
            perf_summary = self.delay_calculator.computational_calculator.get_performance_distribution_summary()
            
            # Early termination statistics
            termination_stats = self.metrics_collector.get_termination_statistics()
            
            final_metrics = {
                "total_blocks": total_blocks,
                "total_chunks": total_chunks,
                "avg_latency": avg_latency,
                "avg_coverage": avg_coverage,
                "simulation_time": self.event_queue.current_time,
                "bandwidth_utilization": bandwidth_metrics.get("average_utilization", 0.0),
                "peak_utilization": bandwidth_metrics.get("peak_utilization", 0.0),
                "blocks_completed_count": len([m for m in self.block_metrics.values() if m["completion_times"]]),
                "chunk_efficiency": (total_chunks / (total_blocks * 1000)) if total_blocks > 0 else 0.0,
                "performance_distribution": perf_summary,
                "early_termination": termination_stats  
            }
            
            logger.info(f" Final Statistics:")
            logger.info(f"   - Total blocks: {total_blocks}")
            logger.info(f"   - Total chunks: {total_chunks}")
            logger.info(f"   - Average latency: {avg_latency:.3f}s")
            logger.info(f"   - Average coverage: {avg_coverage:.1f}%")
            logger.info(f"   - Bandwidth utilization: {final_metrics['bandwidth_utilization']:.1f}%")
            logger.info(f"   - Performance distribution: {perf_summary['distribution_percentages']}")
            
            if termination_stats.get("enabled", False):
                logger.info(f"   - Early termination: {termination_stats['total_events_saved']} events saved")
            
            return final_metrics
        
        except Exception as e:
            logger.error(f"Error calculating final metrics: {e}")
            return {}
    
    def _handle_find_node_event(self, event: Event):
        """Handle find node events (korundu ama kullanılmayacak)."""
        sender_id = event.data.get("sender_id")
        receiver_id = event.data.get("receiver_node_id")
        message = event.data.get("message") 

        receiver_node = self._get_node_by_id(receiver_id)

        if receiver_node and isinstance(message, FindNodeMessage):
            receiver_node.handle_find_node(message)
        else:
            logger.warning(f"FIND_NODE event: Missing receiver node or invalid message type for IDs {sender_id} -> {receiver_id}")

    def _handle_nodes_response_event(self, event: Event):
        """Handle nodes response events (korundu ama kullanılmayacak)."""
        sender_id = event.data.get("sender_id")
        receiver_id = event.data.get("receiver_node_id")
        message = event.data.get("message")

        receiver_node = self._get_node_by_id(receiver_id)

        if receiver_node and isinstance(message, NodesMessage):
            receiver_node.handle_nodes(message)
        else:
            logger.warning(f"NODES_RESPONSE event: Receiver node not found or invalid message type for ID {receiver_id}")
    
    def _handle_message_send_event(self, event: Event):
        """Handle message send events - WITH CHUNK PROCESSING DELAYS"""
        message_type = event.data.get("message_type")
        receiver_node_id = event.data.get("receiver_node_id")
        message = event.data.get("message")
        sender_id = event.data.get("sender_id")
        height = event.data.get("height", None) 
        
        # Get receiver node
        receiver_node = self._get_node_by_id(receiver_node_id)
        if not receiver_node:
            logger.warning(f"Receiver node not found: {receiver_node_id}")
            return
        
        # Message type'a göre uygun handler'ı çağır
        try:
            if message_type == "CHUNK":
               
                chunk_size = len(message.full_chunk_bytes) if hasattr(message, 'full_chunk_bytes') else 1024
                processing_delay = self.delay_calculator.calculate_chunk_processing_delay(
                    receiver_node.node_index, chunk_size
                )
                
                # Schedule chunk processing after computational delay
                self.event_queue.schedule_event(
                    EventType.CHUNK_RECEIVE,
                    node_id=receiver_node.node_index,
                    delay=processing_delay,
                    data={
                        "sender_id": sender_id,
                        "receiver_node_id": receiver_node_id,
                        "message": message,
                        "height": height
                    }
                )
                
            elif isinstance(message, PingMessage):
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
                
        except Exception as e:
            logger.error(f"Error handling {message_type} message: {e}")

    def _handle_chunk_receive_event(self, event: Event):
        """Handle chunk receive after processing delay."""
        try:
            sender_id = event.data.get("sender_id")
            receiver_id = event.data.get("receiver_node_id")
            message = event.data.get("message")
            height = event.data.get("height", 32)
            
            receiver_node = self._get_node_by_id(receiver_id)
            
            if receiver_node and isinstance(message, ChunkMessage):
                # Node chunk'ı alır
                receiver_node.receive_chunk(message, sender_id)
                
                # Chunk'ı forward et (Kadcast routing)
                self._forward_chunk(receiver_node, message, sender_id, height)
                
                # Metrics güncelle
                block_id = message.block_id
                if block_id in self.block_metrics:
                    chunk_key = f"{block_id}_{message.chunk_id}"
                    if chunk_key not in self.block_metrics[block_id]["chunk_propagation"]:
                        self.block_metrics[block_id]["chunk_propagation"][chunk_key] = []
                    
                    self.block_metrics[block_id]["chunk_propagation"][chunk_key].append({
                        "node": receiver_node.node_index,
                        "time": self.event_queue.current_time
                    })
            
        except Exception as e:
            logger.error(f"Error handling chunk receive: {e}")

    def send_message(self, sender_id: str, receiver_id: str, message):
        """Bir düğümden diğerine mesaj gönderme işlemini simüle eder - WITH TRANSMISSION DELAYS"""
        sender_node = self._get_node_by_id(sender_id)
        receiver_node = self._get_node_by_id(receiver_id)

        if not sender_node or not receiver_node:
            logger.warning(f"[SEND FAIL] Sender or receiver not found: {sender_id} -> {receiver_id}")
            return

       
        message_size = getattr(message, 'size', 64)  # Default size for simple messages
        
        transmission_delays = self.delay_calculator.calculate_transmission_delay(
            sender_node.node_index,
            receiver_node.node_index,
            sender_node.city,
            receiver_node.city,
            message_size,
            self.event_queue.current_time
        )
        
        delay = transmission_delays["transmission_delay"]
        
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

    def mark_node_discovery_complete(self, node_index: int):
        """Mark a node as having completed discovery (korundu)."""
        if node_index in self.peer_discovery_flags:
            self.peer_discovery_flags[node_index] = True
            completed_count = sum(1 for flag in self.peer_discovery_flags.values() if flag)
    
    def get_simulation_stats(self) -> Dict[str, Any]:
        """Get simulation statistics."""
        completed_nodes = sum(1 for flag in self.peer_discovery_flags.values() if flag)
        total_peers = sum(len(node.peers) for node in self.nodes.values())
        avg_peers = total_peers / len(self.nodes) if self.nodes else 0
        
        return {
            "simulation_id": self.simulation_id,
            "node_count": self.node_count,
            "completed_nodes": completed_nodes,
            "completion_rate": completed_nodes / self.node_count if self.node_count > 0 else 0,
            "average_peers_per_node": avg_peers,
            "phase": self.phase,
            "approach": "centralized",
            "real_time_metrics": self.real_time_metrics,
            "block_metrics": self.block_metrics,
            "blocks_generated": self.blocks_generated,
            "active_blocks": len(self.active_blocks),
            "performance_distribution": self.node_performance_assignments,
            "early_termination_enabled": self.early_termination_enabled,  
            "early_terminated_blocks": len(self.early_terminated_blocks),
        }
    
    def _get_node_index_by_id(self, node_id: str) -> int:
        """Node ID'ye göre node index'ini bul"""
        node = self._get_node_by_id(node_id)
        return node.node_index if node else -1
    
    def on_block_completed(self, node_index: int, block_id: int, completion_time: float):
        """Block completion callback - WITH BLOCK PROCESSING DELAYS"""
        try:
            # ✅ ADD BLOCK PROCESSING AND RECONSTRUCTION DELAYS
            if block_id in self.block_metrics:
                transactions = self.block_metrics[block_id].get("transactions", [])
                block_size = self.block_metrics[block_id]["block_size"]
                total_chunks = self.block_metrics[block_id]["total_chunks"]
                
                # Calculate reconstruction delay
                reconstruction_delay = self.delay_calculator.calculate_block_reconstruction_delay(
                    node_index, total_chunks, block_size, fec_used=True
                )
                
                # Calculate block processing delay
                processing_delay = self.delay_calculator.calculate_block_processing_delay(
                    node_index, block_size, transactions
                )
                
                total_processing_delay = reconstruction_delay + processing_delay
                final_completion_time = completion_time + total_processing_delay
                
                logger.debug(f"Block {block_id} processing delays for node {node_index}: "
                           f"reconstruction={reconstruction_delay:.4f}s, "
                           f"processing={processing_delay:.4f}s, "
                           f"total={total_processing_delay:.4f}s")
                
                # Schedule final completion after processing delays
                self.event_queue.schedule_event(
                    EventType.BLOCK_PROCESSED,
                    node_id=node_index,
                    delay=total_processing_delay,
                    data={
                        "node_index": node_index,
                        "block_id": block_id,
                        "completion_time": final_completion_time
                    }
                )
            
        except Exception as e:
            logger.error(f"Error handling block completion: {e}")

    def _handle_block_processed_event(self, event: Event):
        """Handle final block processing completion."""
        try:
            node_index = event.data.get("node_index")
            block_id = event.data.get("block_id")
            completion_time = event.data.get("completion_time")
            
            if block_id in self.block_metrics:
                self.block_metrics[block_id]["completion_times"][node_index] = completion_time
                self.block_metrics[block_id]["nodes_completed"] += 1
                
             
                completed_nodes = self.block_metrics[block_id]["nodes_completed"]
                total_nodes = self.node_count
                coverage = (completed_nodes / total_nodes) * 100.0
                
              
                if coverage >= 100.0 and block_id in self.active_blocks:
                    logger.info(f" Block {block_id} reached 100% - IMMEDIATE TERMINATION!")
                    self.terminate_block_propagation(block_id)
                    self.early_terminated_blocks.add(block_id)
                
                # Update real-time metrics
                if block_id not in self.real_time_metrics["blocks_completed"]:
                    self.real_time_metrics["blocks_completed"][block_id] = 0
                self.real_time_metrics["blocks_completed"][block_id] += 1
                
                # Update metrics collector
                self.metrics_collector.report_block_processed(
                node_index, block_id, completion_time
                )

                # This helps track the final bandwidth state
                self.metrics_collector.report_bandwidth_usage(
                node_index, node_index, 0, completion_time  # 0 bytes = completion marker
            )
                
          
                if coverage >= 95.0:
                 logger.info(f" Block {block_id} reached {coverage:.1f}% completion "
                        f"({completed_nodes}/{total_nodes} nodes)")
        
        except Exception as e:
            logger.error(f"Error handling final block processing: {e}")


    def _forward_chunk(self, node: KadcastNode, chunk_message: ChunkMessage, sender_id: str, height: int = None):
        """Chunk'ı Kadcast routing ile forward et - WITH TRANSMISSION DELAYS"""
        try:
            if height is None:
                height = 64  # Default height
    
            # Height 0'a ulaştıysa forwarding'i durdur
            if height <= 0:
                return
    
            # Get forward peers (exclude sender) with height-based selection
            forward_peers = node.get_broadcast_peers(chunk_message.block_id, height)
            forward_peers = [peer for peer in forward_peers if peer != sender_id]
            
            # Limit forwarding to prevent flooding
            max_forwards = min(3, len(forward_peers))
            if max_forwards > 0:
                selected_peers = random.sample(forward_peers, max_forwards)
                
                for peer_id in selected_peers:
                    # ADD TRANSMISSION DELAY FOR FORWARDING
                    peer_node = self._get_node_by_id(peer_id)
                    if peer_node:
                        chunk_size = len(chunk_message.full_chunk_bytes) if hasattr(chunk_message, 'full_chunk_bytes') else 1024
                        
                        transmission_delays = self.delay_calculator.calculate_transmission_delay(
                            node.node_index,
                            peer_node.node_index,
                            node.city,
                            peer_node.city,
                            chunk_size,
                            self.event_queue.current_time
                        )
                        
                        self.event_queue.schedule_event(
                            EventType.MESSAGE_SEND,
                            node_id=peer_node.node_index,
                            delay=transmission_delays["transmission_delay"],
                            data={
                                "sender_id": node.node_id,
                                "receiver_node_id": peer_id,
                                "message_type": "CHUNK",
                                "message": chunk_message,
                                "height": height - 1
                            }
                        )
                
                # Update metrics
                self.real_time_metrics["chunks_transmitted"] += len(selected_peers)
            
        except Exception as e:
            logger.error(f"Error forwarding chunk: {e}")

    # Register additional event handlers
    def _register_event_handlers(self):
        """Register event handlers with the simulation engine."""
        self.engine.register_handler(EventType.TIMEOUT, self._handle_timeout_event)
        self.engine.register_handler(EventType.MESSAGE_SEND, self._handle_message_send_event)
        self.engine.register_handler(EventType.FIND_NODE, self._handle_find_node_event)
        self.engine.register_handler(EventType.NODES_RESPONSE, self._handle_nodes_response_event)
        self.engine.register_handler(EventType.BLOCK_GENERATE, self._handle_block_generate_event)
        self.engine.register_handler(EventType.CHUNK_SEND, self._handle_chunk_send_event)  # ← NEW
        self.engine.register_handler(EventType.CHUNK_RECEIVE, self._handle_chunk_receive_event)  # ← NEW
        self.engine.register_handler(EventType.BLOCK_PROCESSED, self._handle_block_processed_event)  # ← NEW

    def _periodic_memory_cleanup(self, current_time: float):
        """
        Periodic memory cleanup for long-running simulations
        """
        # Her 30 saniyede bir cleanup yap
        if current_time - self._last_cleanup_time > 30.0:
            # Completed blocks için eski data temizle
            blocks_cleaned = 0
            for block_id, metrics in self.block_metrics.items():
                if block_id in self.early_terminated_blocks:
                    # Early terminated block'ların detaylı verilerini temizle
                    if 'chunk_propagation' in metrics:
                        metrics['chunk_propagation'] = {}  # Clear detailed chunk tracking
                        blocks_cleaned += 1
            
            if blocks_cleaned > 0:
                logger.info(f" Cleaned up detailed data for {blocks_cleaned} terminated blocks")
            
            # Python garbage collection
            import gc
            collected = gc.collect()
            if collected > 0:
                logger.debug(f" Garbage collected {collected} objects")
            
            self._last_cleanup_time = current_time    


    def get_real_block_data(self, block_id: int) -> Optional[Dict]:
        """Get real block data for specific block_id (for JSON export)."""
        return self.generated_blocks_content.get(block_id)

    def get_all_real_blocks(self) -> Dict[int, Dict]:
        """Get all real block data for JSON export."""
        return self.generated_blocks_content.copy()

    def export_simulation_blocks(self) -> List[Dict]:
        """Export all blocks with real content for JSON export."""
        exported_blocks = []
        
        for block_id, block_content in self.generated_blocks_content.items():
            # Get completion metrics if available
            block_metrics = self.block_metrics.get(block_id, {})
            
            exported_block = {
                'block_id': block_id,
                'broadcast_time': block_metrics.get('start_time', block_id * 6.0),
                'content': block_content,  # REAL CONTENT from RealisticBlockGenerator!
                'metrics': {
                    'total_chunks': block_metrics.get('total_chunks', 0),
                    'fec_ratio': block_metrics.get('fec_ratio', self.fec_ratio),
                    'nodes_completed': block_metrics.get('nodes_completed', 0),
                    'completion_rate': (block_metrics.get('nodes_completed', 0) / self.node_count) * 100 if self.node_count > 0 else 0.0,
                    'source_node': block_metrics.get('source_node', -1)
                }
            }
            
            exported_blocks.append(exported_block)
        
        # Sort by block_id
        exported_blocks.sort(key=lambda x: x['block_id'])
        
        logger.info(f"📊 Exported {len(exported_blocks)} blocks with REAL RealisticBlockGenerator content")
        return exported_blocks        
