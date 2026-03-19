import random
import time
import logging
from typing import Dict, List, Set, Tuple, Optional, Any

from src.protocols.kadcast.messages import PingMessage, PongMessage, FindNodeMessage, NodesMessage, ChunkMessage
from src.data.realistic_payload import RealisticChunkGenerator

logger = logging.getLogger("KadcastNode")

class KadcastNode:
    def __init__(self, node_index: int, node_id: str = None, city: str = None,
                 coordinates: Tuple[float, float] = None, performance_type: str = "average_performance", is_bootstrap: bool = False, beta: int = 3):
        self.node_index = node_index
        self.node_id = node_id.lower().replace("0x", "") if node_id else f"{node_index:016x}"  # 64-bit hex format
        self.city = city or "Unknown"
        self.coordinates = coordinates or (0.0, 0.0)
        self.performance_type = performance_type
        self.is_bootstrap = is_bootstrap

        # Node ID'yi integer'a çevir (XOR distance hesaplaması için)
        self.node_id_int = int(self.node_id, 16)

        # K-bucket yapısı (64-bit ID space için 64 bucket)
        self.k_buckets = [set() for _ in range(64)]  # 64 bucket (64-bit ID space için)
        self.k = 20  # Bucket size (Kadcast makalesine uygun)
        self.peers = set()  # Tüm peer'ların ID'leri
        self.last_seen = {}  # Peer'ların son görülme zamanları

        # Peer discovery kontrolü için alanlar (kullanılmayacak ama korunacak)
        self.discovery_complete = False
        self.bootstrap_nodes = set()
        self.recent_findnode_failures = 0
        self.max_failures = 10  # Kadcast'a uygun artırıldı
        self.queried_targets = set()
        self.self_lookup_done = False

        # Chunk-based broadcasting için yeni alanlar
        self.received_chunks = {}  # (block_id, chunk_id) -> ChunkMessage
        self.block_chunks = {}  # block_id -> set of chunk_ids
        self.block_total_chunks = {}  # block_id -> total chunk count
        self.block_original_chunks = {}  # block_id -> original chunk count (FEC öncesi)
        self.fec_chunks = {}  # block_id -> set of FEC chunk_ids
        self.processed_blocks = set()  # Tamamlanan block'lar
        self.block_reconstruction_data = {}  # Block reconstruction için geçici data
        
        # Chunk generator for reconstruction
        self.chunk_generator = RealisticChunkGenerator()
        
        # Forwarding control (flooding prevention)
        self.forwarded_chunks = set()  # (block_id, chunk_id) tuples
        self.max_forwards_per_chunk = 3  # Kadcast routing için limit

        self.beta = beta

        self.simulation = None

    def set_simulation(self, simulation):
        """Simulation referansını ayarla."""
        self.simulation = simulation

    def _get_bucket_index(self, peer_id: str) -> int:
        """Peer ID'ye göre bucket index'ini hesapla (XOR distance)."""
        try:
            peer_id_int = int(peer_id.lower().replace("0x", ""), 16)
            xor_distance = self.node_id_int ^ peer_id_int
            
            if xor_distance == 0:
                return 0
            
            # 64-bit için bucket index: Bit length - 1 = bucket index
            bucket_index = max(0, min(xor_distance.bit_length() - 1, 63))  # 63 max bucket index
            return bucket_index
        except Exception as e:
            logger.warning(f"Error calculating bucket index for {peer_id}: {e}")
            return 0

    def add_peer(self, peer_id: str) -> bool:
        """Peer'ı uygun K-bucket'a ekle."""
        if peer_id is None or peer_id == self.node_id:
            return False
        
        # ID'yi normalize et
        peer_id = peer_id.lower().replace("0x", "")
        
        # Bucket index'ini hesapla
        bucket_index = self._get_bucket_index(peer_id)
        bucket = self.k_buckets[bucket_index]
        
        # Zaten varsa sadece last_seen'i güncelle
        if peer_id in bucket:
            self.last_seen[peer_id] = time.time()
            return False
        
        # Bucket'ta yer varsa ekle
        if len(bucket) < self.k:
            bucket.add(peer_id)
            self.peers.add(peer_id)  # ← SADECE GERÇEK EKLEME DURUMUNDA
            self.last_seen[peer_id] = time.time()
            return True
        else:
            # Bucket dolu, en eski peer'ı çıkar
            if hasattr(self, '_centralized_mode') and self._centralized_mode:
            # Random eviction for centralized mode
              oldest_peer = random.choice(list(bucket))
            else:
            # Normal LRU eviction

              oldest_peer = min(bucket, key=lambda x: self.last_seen.get(x, float('inf')))
           
            bucket.remove(oldest_peer)
            self.peers.discard(oldest_peer) 

            bucket.add(peer_id)
        
            # Global peer set'e ekle
            self.peers.add(peer_id)
            self.last_seen[peer_id] = time.time()
            return True

    def add_all_network_peers(self, all_node_ids: List[str]):
        """Merkezi yaklaşım: Tüm ağdaki node'ları peer olarak ekle."""
        self._centralized_mode = True  
        
        added_count = 0
        for node_id in all_node_ids:
            if node_id != self.node_id:  # Kendisi hariç
                if self.add_peer(node_id):
                    added_count += 1
        
          #logger.info(f"Node {self.node_index}: Added {added_count} new peers, total {len(self.peers)} peers, filled {self.get_filled_bucket_count()}/64 buckets")
        
        # Merkezi yaklaşımda discovery completion kontrolü yap
        self.check_discovery_completion()

    def are_all_buckets_full(self) -> bool:
        """Tüm K-bucket'ların dolu olup olmadığını kontrol et."""
        return all(len(bucket) >= self.k for bucket in self.k_buckets)

    def get_filled_bucket_count(self) -> int:
        """Dolu bucket sayısını döndür."""
        return sum(1 for bucket in self.k_buckets if len(bucket) > 0)

    def find_closest_nodes(self, target_id: str, count: int = 20) -> List[str]:
        """Target ID'ye en yakın node'ları bul (XOR distance)."""
        try:
            target_id_int = int(target_id.lower().replace("0x", ""), 16)
            all_peers = list(self.peers)
            
            # XOR distance hesapla ve sırala
            distances = []
            for peer_id in all_peers:
                try:
                    peer_id_int = int(peer_id, 16)
                    distance = target_id_int ^ peer_id_int
                    distances.append((peer_id, distance))
                except ValueError:
                    continue
            
            distances.sort(key=lambda x: x[1])
            return [peer for peer, _ in distances[:count]]
        except Exception as e:
            logger.warning(f"Error finding closest nodes: {e}")
            return list(self.peers)[:count]

    def get_broadcast_peers(self, block_id: int, height: int = None) -> List[str]:
        broadcast_peers = []
        
        if height is None:
            height = 64  # 64-bit ID space için 64 height
        
        # KADCAST ORIGINAL: height-1'den 0'a kadar (TERS YÖNDE döngü)
        for i in range(height - 1, -1, -1):
            if i >= len(self.k_buckets):
                continue # Geçersiz bucket index'ini atla
                
            bucket = self.k_buckets[i]
            if len(bucket) == 0:
                continue  # BOŞ BUCKET SKIP (Gerçek Kadcast davranışı)
            
            selected_count = min(self.beta, len(bucket))
            selected_peers = random.sample(list(bucket), selected_count)
            broadcast_peers.extend(selected_peers)
        
        return broadcast_peers
    

    def receive_chunk(self, chunk: ChunkMessage, sender_id: str):
        """Chunk mesajını al ve işle."""
        try:
            block_id = chunk.block_id
            chunk_id = chunk.chunk_id
            chunk_key = (block_id, chunk_id)
            
            # Duplicate chunk kontrolü
            if chunk_key in self.received_chunks:
                #logger.debug(f"Node {self.node_index}: Duplicate chunk {chunk_id} for block {block_id}")
                return
            
            # Chunk'ı kaydet
            self.received_chunks[chunk_key] = chunk
            
            # Block tracking güncelle
            if block_id not in self.block_chunks:
                self.block_chunks[block_id] = set()
            self.block_chunks[block_id].add(chunk_id)
            
            # FEC chunk tracking
            if chunk.is_fec:
                if block_id not in self.fec_chunks:
                    self.fec_chunks[block_id] = set()
                self.fec_chunks[block_id].add(chunk_id)
            
            # Total chunk count güncelle
            self.block_total_chunks[block_id] = chunk.total_chunks

            if self.simulation and hasattr(self.simulation, 'metrics_collector'):
               current_time = self.simulation.event_queue.current_time
               self.simulation.metrics_collector.report_chunk_received(
               self.node_index, block_id, chunk_id, current_time
           )
            
            # Sender'ı peer olarak ekle
            self.add_peer(sender_id)
            
            #logger.debug(f"Node {self.node_index}: Received chunk {chunk_id}/{chunk.total_chunks} for block {block_id} from {sender_id}")
            
            # Block tamamlanma kontrolü
            if self._is_block_complete(block_id):
                self._process_complete_block(block_id)


            
            # Chunk'ı forward et (Kadcast routing)
            self._forward_chunk(chunk, sender_id)
            

        except Exception as e:
            logger.error(f"Node {self.node_index}: Error receiving chunk: {e}")

    def _is_block_complete(self, block_id: int) -> bool:
       """Block'ın tamamlanıp tamamlanmadığını kontrol et."""
       try:
        # DÜZELTME: Zaten işlenmiş block'ları tekrar kontrol etme
           if block_id in self.processed_blocks:
               return False  # Zaten işlenmiş, tekrar "complete" deme

           received_chunks = self.block_chunks.get(block_id, set())
           total_chunks = self.block_total_chunks.get(block_id, 0)
     
           if total_chunks == 0:
               return False
           
           # FEC ile birlikte gelen chunk'ların %80'i yeterli - DYNAMIC FEC ratio
           fec_ratio = 0.2  # Default fallback
           if self.simulation and hasattr(self.simulation, 'fec_ratio'):
               fec_ratio = self.simulation.fec_ratio
           
     
        # FEC ile birlikte gelen chunk'ların %80'i yeterli (FEC ratio 0.2 varsayımı)
           estimated_original = int(total_chunks / (1 + fec_ratio)) if total_chunks > 0 else 0
           required_chunks = max(estimated_original, int(total_chunks * (1-fec_ratio)))
     
           is_complete = len(received_chunks) >= required_chunks
     
           if is_complete:
               #logger.info(f" Node {self.node_index}: Block {block_id} is complete ({len(received_chunks)}/{total_chunks} chunks, required: {required_chunks})")
               return is_complete
           else:
            # Debug için current progress göster
               if len(received_chunks) > 0:
                   # logger.debug(f" Node {self.node_index}: Block {block_id} progress: {len(received_chunks)}/{required_chunks} required chunks")
                   pass
     
           return is_complete
     
       except Exception as e:
           logger.error(f"Node {self.node_index}: Error checking block completion: {e}")
           return False
   
    def _forward_chunk(self, chunk: ChunkMessage, sender_id: str):
        """Chunk'ı Kadcast routing ile forward et."""
        try:
            block_id = chunk.block_id
            chunk_id = chunk.chunk_id
            forward_key = (block_id, chunk_id)
            
            # Duplicate forwarding kontrolü
            if forward_key in self.forwarded_chunks:
                return
            
            # Forward peer'ları seç (sender hariç)
            forward_peers = self.get_broadcast_peers(block_id)
            forward_peers = [peer for peer in forward_peers if peer != sender_id]
            
            # Flooding prevention: maksimum forward sayısı
            max_forwards = min(self.max_forwards_per_chunk, len(forward_peers))
            if max_forwards > 0:
                selected_peers = random.sample(forward_peers, max_forwards)
                
                for peer_id in selected_peers:
                    # Chunk'ı forward et
                    self.send_message(peer_id, chunk)
                
                # Forward edildi olarak işaretle
                self.forwarded_chunks.add(forward_key)
                
                # logger.debug(f"Node {self.node_index}: Forwarded chunk {chunk_id} for block {block_id} to {len(selected_peers)} peers")
            
        except Exception as e:
            logger.error(f"Node {self.node_index}: Error forwarding chunk: {e}")

    def send_message(self, target_id: str, message):
        """Mesaj gönder (simulation üzerinden)."""
        if self.simulation and hasattr(self.simulation, "send_message"):
            self.simulation.send_message(self.node_id, target_id, message)

    def send_ping(self, target_id: str):
        """PING mesajı gönder (korundu ama kullanılmayacak)."""
        ping_message = PingMessage(sender_id=self.node_id)
        self.send_message(target_id, ping_message)

    def send_find_node(self, target_id: str, search_id: str):
        """FIND_NODE mesajı gönder (korundu ama kullanılmayacak)."""
        if self.discovery_complete:
            logger.debug(f"Node {self.node_index}: Skipping FIND_NODE - discovery already complete")
            return
        
        find_node_message = FindNodeMessage(sender_id=self.node_id, target_id=search_id)
        self.send_message(target_id, find_node_message)

    def start_self_lookup(self):
        """Kadcast makalesine uygun: Kendi ID'ini lookup yap (korundu ama kullanılmayacak)."""
        if not self.self_lookup_done and self.bootstrap_nodes:
            random_bootstrap = random.choice(list(self.bootstrap_nodes))
            self.send_find_node(random_bootstrap, self.node_id)
            self.self_lookup_done = True
            logger.debug(f"Node {self.node_index}: Started self-lookup to bootstrap {random_bootstrap}")

    def handle_ping(self, message: PingMessage):
        """PING mesajını işle (korundu ama kullanılmayacak)."""
        # Sender'ı peer olarak ekle
        self.add_peer(message.sender_id)
        
    
        pong_message = PongMessage(sender_id=self.node_id, ping_sender_id=message.sender_id)
        self.send_message(message.sender_id, pong_message)

    def handle_pong(self, message: PongMessage):
      
        # Sender'ı peer olarak ekle
        self.add_peer(message.sender_id)

    def handle_find_node(self, message: FindNodeMessage):
       
   
        self.add_peer(message.sender_id)
        
        closest_nodes = self.find_closest_nodes(message.target_id, count=self.k)
     
        nodes_info = [{"node_id": node_id} for node_id in closest_nodes]
        nodes_message = NodesMessage(sender_id=self.node_id, nodes=nodes_info)
        self.send_message(message.sender_id, nodes_message)

    def handle_nodes(self, message: NodesMessage):
        """NODES mesajını işle (korundu ama kullanılmayacak)."""

        self.add_peer(message.sender_id)
        
        if self.discovery_complete:
            logger.debug(f"Node {self.node_index}: Ignoring NODES - discovery already complete")
            return
 
        new_nodes_learned = False
        new_nodes = []
        
        for node_info in message.nodes:
            node_id = node_info.get("node_id")
            if node_id and node_id != self.node_id:
                was_new = self.add_peer(node_id)
                if was_new:
                    new_nodes_learned = True
                    new_nodes.append(node_id)
        
        # Failure tracking
        if not new_nodes_learned:
            self.recent_findnode_failures += 1
            logger.debug(f"Node {self.node_index}: No new nodes learned, failures: {self.recent_findnode_failures}")
        else:
            self.recent_findnode_failures = 0
            logger.debug(f"Node {self.node_index}: Learned {len(new_nodes)} new nodes, resetting failures")
            
            # Yeni öğrenilen node'lara FIND_NODE gönder (Kadcast makalesine uygun α=3)
            closest_new_nodes = self.find_closest_nodes(self.node_id, count=len(new_nodes))
            targets_to_query = [node for node in closest_new_nodes if node in new_nodes][:3]  # α=3
            
            for target_node in targets_to_query:
                if target_node not in self.queried_targets:
                    self.queried_targets.add(target_node)
                    self.send_find_node(target_node, self.node_id)
                    logger.debug(f"Node {self.node_index}: Sent FIND_NODE to new peer {target_node}")
        
        # Discovery completion kontrolü
        self.check_discovery_completion()

    def check_discovery_completion(self):
        """Peer discovery'nin tamamlanıp tamamlanmadığını kontrol et (aynı kriterler korundu)."""
        if self.discovery_complete:
            return
        
        # Bootstrap node'lar için özel durum (Kadcast'a uygun)
        if self.is_bootstrap:
            if len(self.peers) >= self.k:  # k=20 peer yeterli
                self.discovery_complete = True
                
                if self.simulation and hasattr(self.simulation, 'mark_node_discovery_complete'):
                    self.simulation.mark_node_discovery_complete(self.node_index)
                return
        
        # Normal node'lar için kriterler (Kadcast makalesine uygun)
        all_buckets_full = self.are_all_buckets_full()
        too_many_failures = self.recent_findnode_failures >= self.max_failures
        has_enough_peers = len(self.peers) >= min(self.k * 2, 40)  # 40 peer yeterli
        filled_buckets = self.get_filled_bucket_count()
        has_enough_buckets = filled_buckets >= 10  # 10 bucket yeterli
        
        if all_buckets_full or too_many_failures or has_enough_peers or has_enough_buckets:
            self.discovery_complete = True
            
            # Completion reason
            if all_buckets_full:
                reason = "all_buckets_full"
            elif too_many_failures:
                reason = "too_many_failures"
            elif has_enough_peers:
                reason = "enough_peers"
            else:
                reason = "enough_buckets"
            
            
            
            # Simulation'a bildir
            if self.simulation and hasattr(self.simulation, 'mark_node_discovery_complete'):
                self.simulation.mark_node_discovery_complete(self.node_index)

    def get_stats(self) -> Dict[str, Any]:
        """Node istatistiklerini döndür."""
        return {
            "node_id": self.node_id,
            "node_index": self.node_index,
            "city": self.city,
            "coordinates": self.coordinates,
            "is_bootstrap": self.is_bootstrap,
            "total_peers": len(self.peers),
            "peers_in_buckets": [len(bucket) for bucket in self.k_buckets],
            "discovery_complete": self.discovery_complete,
            "recent_failures": self.recent_findnode_failures,
            "queried_targets_count": len(self.queried_targets),
            "filled_buckets": self.get_filled_bucket_count(),
            "self_lookup_done": self.self_lookup_done,
            # Chunk-based broadcasting stats
            "received_chunks_count": len(self.received_chunks),
            "active_blocks": len(self.block_chunks),
            "processed_blocks_count": len(self.processed_blocks),
            "forwarded_chunks_count": len(self.forwarded_chunks)
        }

    def get_block_stats(self, block_id: int) -> Dict[str, Any]:
        """Belirli bir block için istatistikleri döndür."""
        return {
            "block_id": block_id,
            "received_chunks": len(self.block_chunks.get(block_id, set())),
            "total_chunks": self.block_total_chunks.get(block_id, 0),
            "fec_chunks": len(self.fec_chunks.get(block_id, set())),
            "is_complete": self._is_block_complete(block_id),
            "is_processed": block_id in self.processed_blocks
        }
    
    def _process_complete_block(self, block_id: int):
       """Tamamlanan block'ı işle ve reconstruct et."""
       try:
           if block_id in self.processed_blocks:
                # logger.debug(f"Node {self.node_index}: Block {block_id} already processed, skipping")
               return False
        
            # logger.info(f" Node {self.node_index}: Starting block {block_id} reconstruction...")
        

           chunk_data = []
           received_chunks = self.block_chunks.get(block_id, set())
        
            # logger.debug(f"Node {self.node_index}: Processing {len(received_chunks)} chunks for block {block_id}")
        
           for chunk_id in sorted(received_chunks):
               chunk_key = (block_id, chunk_id)
               if chunk_key in self.received_chunks:
                   chunk_message = self.received_chunks[chunk_key]
                
                # DÜZELTME: full_chunk_bytes kullan (header + payload)
                   chunk_data.append(chunk_message.full_chunk_bytes)
        
           if not chunk_data:
               logger.error(f" Node {self.node_index}: No chunk data found for block {block_id}")
               return False
        
           #  logger.debug(f"Node {self.node_index}: Collected {len(chunk_data)} chunks, attempting reconstruction...")
        
        # Block reconstruction (dechunkify) with improved error handling
           try:
        
               reconstructed_block, actual_block_id = self.chunk_generator.dechunkify_block(
                   chunks=chunk_data
               )
            
            # 🔧 FIX: Explicit success check
               if reconstructed_block is not None and len(reconstructed_block) > 0:
                   #  logger.info(f"Node {self.node_index}: Successfully reconstructed block {block_id} ({len(reconstructed_block)} bytes)")
                   
                # Block'ı processed olarak işaretle
                   self.processed_blocks.add(block_id)
                
                # 🔧 FIX: GUARANTEED CALLBACK EXECUTION
                   callback_success = False
                   if self.simulation and hasattr(self.simulation, 'on_block_completed'):
                       try:
                           current_time = self.simulation.event_queue.current_time
                        
                        # 🔧 FIX: Pre-callback log for debugging
                           #  logger.info(f"Node {self.node_index}: Calling block completion callback for block {block_id} at time {current_time:.3f}")
                        
                           self.simulation.on_block_completed(
                               self.node_index, 
                               block_id, 
                               current_time
                           )
                        
                           callback_success = True
                           #  logger.info(f" Node {self.node_index}: Block completion callback SUCCESS for block {block_id}")
                        
                       except AttributeError as e:
                           logger.error(f" Callback AttributeError for node {self.node_index}, block {block_id}: {e}")
                       except Exception as e:
                           logger.error(f" Callback Exception for node {self.node_index}, block {block_id}: {e}")
                           logger.error(f"   Exception type: {type(e).__name__}")
                           logger.error(f"   Exception args: {e.args}")
                   else:
                       logger.error(f"Node {self.node_index}: No simulation reference or on_block_completed method found")
                       logger.error(f"   self.simulation: {self.simulation}")
                       if self.simulation:
                            logger.error(f"   hasattr on_block_completed: {hasattr(self.simulation, 'on_block_completed')}")
                
                # 🔧 FIX: Log final status
                   if callback_success:
                      #   logger.info(f" Node {self.node_index}: Block {block_id} FULLY COMPLETED with callback")
                      pass
                   else:
                       logger.error(f" Node {self.node_index}: Block {block_id} reconstructed but callback FAILED")
                
                   return True
                
               else:
                   logger.error(f" Node {self.node_index}: RaptorQ decoder returned empty result for block {block_id}")
                   logger.error(f"   reconstructed_block: {reconstructed_block}")
                   logger.error(f"   actual_block_id: {actual_block_id}")
                   return False
                
           except ValueError as e:
               logger.error(f" Node {self.node_index}: RaptorQ ValueError for block {block_id}: {e}")
               return False
           except Exception as e:
               logger.error(f"self.node_index}: Unexpected reconstruction error for block {block_id}: {e}")
               logger.error(f"   Exception type: {type(e).__name__}")
               return False
        
       except Exception as e:
           logger.error(f" Node {self.node_index}: Fatal error in _process_complete_block for block {block_id}: {e}")
           logger.error(f"   Exception type: {type(e).__name__}")
           return False
