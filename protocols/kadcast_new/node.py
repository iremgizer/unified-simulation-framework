import logging
import random
import time
from typing import Dict, List, Set, Tuple, Optional, Any

from src.protocols.kadcast.messages import PingMessage, PongMessage, FindNodeMessage, NodesMessage, ChunkMessage
from src.data.realistic_payload import RealisticChunkGenerator

logger = logging.getLogger("KadcastNode")

class KadcastNode:
    def __init__(self, node_index: int, node_id: str = None, city: str = None,
                 coordinates: Tuple[float, float] = None, is_bootstrap: bool = False):
        self.node_index = node_index
        self.node_id = node_id.lower().replace("0x", "") if node_id else f"{node_index:08x}"
        self.city = city or "Unknown"
        self.coordinates = coordinates or (0.0, 0.0)
        self.is_bootstrap = is_bootstrap

        self.node_id_int = int(self.node_id, 16)

        self.k_buckets = [set() for _ in range(32)]
        self.k = 20
        self.peers = set()
        self.last_seen = {}

        # Peer discovery kontrolü için yeni alanlar (Kadcast makalesine uygun)
        self.discovery_complete = False
        self.bootstrap_nodes = set()
        self.recent_findnode_failures = 0  # Art arda başarısız FIND_NODE sayısı
        self.max_failures = 10  # Kadcast'a uygun: daha fazla deneme hakkı (3→10)
        self.queried_targets = set()  # Daha önce sorgulanmış hedefler
        self.self_lookup_done = False  # Kendi ID'ini lookup yaptı mı?

        self.received_chunks = {}
        self.block_chunks = {}
        self.block_total_chunks = {}
        self.block_original_chunks = {}
        self.fec_chunks = {}
        self.processed_blocks = set()

        self.simulation = None

    def set_simulation(self, simulation):
        self.simulation = simulation

    def _get_bucket_index(self, peer_id: str) -> int:
        try:
            peer_id_int = int(peer_id.lower().replace("0x", ""), 16)
            xor_distance = self.node_id_int ^ peer_id_int
            return max(0, min(xor_distance.bit_length() - 1, 31))
        except Exception:
            return 0

    def add_peer(self, peer_id: str) -> bool:
        if peer_id is None or peer_id == self.node_id:
            return False
        peer_id = peer_id.lower().replace("0x", "")
        bucket_index = self._get_bucket_index(peer_id)
        bucket = self.k_buckets[bucket_index]

        if peer_id in bucket:
            self.last_seen[peer_id] = time.time()
            return False

        if len(bucket) < self.k:
            bucket.add(peer_id)
        else:
            oldest_peer = min(bucket, key=lambda x: self.last_seen.get(x, float('inf')))
            bucket.remove(oldest_peer)
            bucket.add(peer_id)

        self.peers.add(peer_id)
        self.last_seen[peer_id] = time.time()
        return True

    def are_all_buckets_full(self) -> bool:
        """Tüm K-bucket'ların dolu olup olmadığını kontrol et."""
        return all(len(bucket) >= self.k for bucket in self.k_buckets)

    def get_filled_bucket_count(self) -> int:
        """Dolu bucket sayısını döndür (Kadcast makalesine uygun)."""
        return sum(1 for bucket in self.k_buckets if len(bucket) > 0)

    def find_closest_nodes(self, target_id: str, count: int = 20) -> List[str]:
        target_id_int = int(target_id.lower().replace("0x", ""), 16)
        all_peers = list(self.peers)
        distances = [(peer_id, target_id_int ^ int(peer_id, 16)) for peer_id in all_peers]
        distances.sort(key=lambda x: x[1])
        return [peer for peer, _ in distances[:count]]

    def get_broadcast_peers(self, block_id: int) -> List[str]:
        broadcast_peers = []
        for bucket in self.k_buckets:
            if bucket:
                broadcast_peers.append(random.choice(list(bucket)))
        if len(broadcast_peers) < 10:
            additional_peers = [p for p in self.peers if p not in broadcast_peers]
            needed = min(10 - len(broadcast_peers), len(additional_peers))
            if additional_peers:
                broadcast_peers.extend(random.sample(additional_peers, needed))
        return broadcast_peers

    def receive_chunk(self, chunk: ChunkMessage, sender_id: str):
        block_id = chunk.block_id
        chunk_id = chunk.chunk_id
        chunk_key = (block_id, chunk_id)
        self.received_chunks[chunk_key] = chunk

        self.block_chunks.setdefault(block_id, set()).add(chunk_id)
        if chunk.is_fec:
            self.fec_chunks.setdefault(block_id, set()).add(chunk_id)

        self.block_total_chunks[block_id] = chunk.total_chunks
        self.add_peer(sender_id)

        if self._is_block_complete(block_id):
            self._process_complete_block(block_id)

    def _is_block_complete(self, block_id: int) -> bool:
        received_chunks = self.block_chunks.get(block_id, set())
        total_chunks = self.block_total_chunks.get(block_id, 0)
        estimated_original = int(total_chunks / 1.2) if total_chunks > 0 else 0
        return len(received_chunks) >= estimated_original

    def _process_complete_block(self, block_id: int):
        if block_id in self.processed_blocks:
            return
        self.processed_blocks.add(block_id)
        if self.simulation and hasattr(self.simulation, 'on_block_completed'):
            self.simulation.on_block_completed(self.node_index, block_id, time.time())

    def send_message(self, target_id: str, message):
        if self.simulation and hasattr(self.simulation, "send_message"):
            self.simulation.send_message(self.node_id, target_id, message)

    def send_ping(self, target_id: str):
        self.send_message(target_id, PingMessage(sender_id=self.node_id))

    def send_find_node(self, target_id: str, search_id: str):
        """FIND_NODE mesajı gönder."""
        if self.discovery_complete:
            logger.debug(f"Node {self.node_index}: Skipping FIND_NODE - discovery already complete")
            return
        self.send_message(target_id, FindNodeMessage(sender_id=self.node_id, target_id=search_id))

    def start_self_lookup(self):
        """Kadcast makalesine uygun: Kendi ID'ini lookup yap."""
        if not self.self_lookup_done and self.bootstrap_nodes:
            random_bootstrap = random.choice(list(self.bootstrap_nodes))
            self.send_find_node(random_bootstrap, self.node_id)
            self.self_lookup_done = True
            logger.debug(f"Node {self.node_index}: Started self-lookup to bootstrap {random_bootstrap}")

    def handle_ping(self, message: PingMessage):
        self.add_peer(message.sender_id)
        self.send_message(message.sender_id, PongMessage(sender_id=self.node_id, ping_sender_id=message.sender_id))

    def handle_pong(self, message: PongMessage):
        self.add_peer(message.sender_id)

    def handle_find_node(self, message: FindNodeMessage):
        """FIND_NODE mesajını işle ve NODES cevabı gönder."""
        self.add_peer(message.sender_id)
        closest_nodes = self.find_closest_nodes(message.target_id, count=self.k)
        nodes_info = [{"node_id": n} for n in closest_nodes]
        self.send_message(message.sender_id, NodesMessage(sender_id=self.node_id, nodes=nodes_info))

    def handle_nodes(self, message: NodesMessage):
        """NODES mesajını işle ve peer discovery mantığını uygula."""
        self.add_peer(message.sender_id)
        
        if self.discovery_complete:
            logger.debug(f"Node {self.node_index}: Ignoring NODES - discovery already complete")
            return  # Zaten tamamlanmışsa işlem yapma
        
        # Yeni node'ları öğren
        new_nodes_learned = False
        new_nodes = []
        
        for node_info in message.nodes:
            node_id = node_info.get("node_id")
            if node_id and node_id != self.node_id:
                was_new = self.add_peer(node_id)
                if was_new:
                    new_nodes_learned = True
                    new_nodes.append(node_id)
        
        # Eğer yeni node öğrenilmediyse başarısızlık sayısını artır
        if not new_nodes_learned:
            self.recent_findnode_failures += 1
            logger.debug(f"Node {self.node_index}: No new nodes learned, failures: {self.recent_findnode_failures}")
        else:
            # Yeni node öğrenildiyse başarısızlık sayısını sıfırla
            self.recent_findnode_failures = 0
            logger.debug(f"Node {self.node_index}: Learned {len(new_nodes)} new nodes, resetting failures")
            
            # Yeni öğrenilen node'lardan en yakın 3'üne FIND_NODE gönder (Kadcast α=3 parametresi)
            closest_new_nodes = self.find_closest_nodes(self.node_id, count=len(new_nodes))
            targets_to_query = [node for node in closest_new_nodes if node in new_nodes][:3]
            
            for target_node in targets_to_query:
                if target_node not in self.queried_targets:
                    self.queried_targets.add(target_node)
                    self.send_find_node(target_node, self.node_id)
                    logger.debug(f"Node {self.node_index}: Sent FIND_NODE to new peer {target_node}")
        
        # Discovery tamamlanma kontrolü
        self.check_discovery_completion()

    def check_discovery_completion(self):
        """Peer discovery'nin tamamlanıp tamamlanmadığını kontrol et (Kadcast makalesine uygun)."""
        if self.discovery_complete:
            return
        
        # Bootstrap node'lar için özel durum (Kadcast'ta bootstrap'lar zaten network'ü biliyor)
        if self.is_bootstrap:
            if len(self.peers) >= self.k:  # k kadar peer yeterli
                self.discovery_complete = True
                logger.info(f"Bootstrap Node {self.node_index}: Discovery completed with {len(self.peers)} peers")
                if self.simulation and hasattr(self.simulation, 'mark_node_discovery_complete'):
                    self.simulation.mark_node_discovery_complete(self.node_index)
                return
        
        # Kadcast makalesine uygun kriterler
        
        # Kriter 1: Tüm bucket'lar dolu mu? (orijinal kriter)
        all_buckets_full = self.are_all_buckets_full()
        
        # Kriter 2: Art arda çok fazla FIND_NODE başarısızlığı var mı? (10'a çıkarıldı)
        too_many_failures = self.recent_findnode_failures >= self.max_failures
        
        # Kriter 3: Yeterli peer'a sahip mi? (Kadcast'a uygun: 40 peer)
        has_enough_peers = len(self.peers) >= min(self.k * 2, 40)
        
        # Kriter 4: Yeterli bucket dolu mu? (Kadcast'a uygun: en az 10 bucket)
        filled_buckets = self.get_filled_bucket_count()
        has_enough_buckets = filled_buckets >= 10
        
        if all_buckets_full or too_many_failures or has_enough_peers or has_enough_buckets:
            self.discovery_complete = True
            
            # Hangi kriterin sağlandığını belirle
            if all_buckets_full:
                reason = "all_buckets_full"
            elif too_many_failures:
                reason = "too_many_failures"
            elif has_enough_peers:
                reason = "enough_peers"
            else:
                reason = "enough_buckets"
            
            logger.info(f"Node {self.node_index}: Peer discovery completed. Reason: {reason}, Peers: {len(self.peers)}, Buckets: {filled_buckets}")
            
            # Simulator'a bildir
            if self.simulation and hasattr(self.simulation, 'mark_node_discovery_complete'):
                self.simulation.mark_node_discovery_complete(self.node_index)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_index": self.node_index,
            "city": self.city,
            "coordinates": self.coordinates,
            "is_bootstrap": self.is_bootstrap,
            "total_peers": sum(len(bucket) for bucket in self.k_buckets),
            "peers_in_buckets": [len(bucket) for bucket in self.k_buckets],
            "discovery_complete": self.discovery_complete,
            "recent_failures": self.recent_findnode_failures,
            "queried_targets_count": len(self.queried_targets),
            "filled_buckets": self.get_filled_bucket_count(),
            "self_lookup_done": self.self_lookup_done
        }

