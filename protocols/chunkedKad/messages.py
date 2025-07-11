import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from src.data.realistic_payload import RealisticChunkGenerator

logger = logging.getLogger("ChunkedKadMessages")

class ChunkedKadMessage(ABC):
    """Base class for all ChunkedKad protocol messages."""

    def __init__(self, message_type: str, sender_id: str):
        self.message_type = message_type
        self.sender_id = sender_id
        self.timestamp = time.time()
        self.size = 0  # Will be calculated by derived classes

    @abstractmethod
    def _calculate_size(self) -> int:
        """Calculate message size in bytes."""
        pass

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        return {
            "message_type": self.message_type,
            "sender_id": self.sender_id,
            "timestamp": self.timestamp,
            "size": self.size
        }


# ==================================================================================
# 1. KADCAST BASIC CONNECTIVITY MESSAGES (Required for DHT maintenance)
# ==================================================================================

class PingMessage(ChunkedKadMessage):
    """Ping message for peer discovery and connectivity test."""

    def __init__(self, sender_id: str):
        super().__init__("PING", sender_id)
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        return 8  # Small fixed size for ping


class PongMessage(ChunkedKadMessage):
    """Pong message in response to ping."""

    def __init__(self, sender_id: str, ping_sender_id: str):
        super().__init__("PONG", sender_id)
        self.ping_sender_id = ping_sender_id
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        return 16  # Small fixed size for pong


class FindNodeMessage(ChunkedKadMessage):
    """Find node message for peer discovery in Kademlia DHT."""

    def __init__(self, sender_id: str, target_id: str):
        super().__init__("FIND_NODE", sender_id)
        self.target_id = target_id
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        return 32  # Fixed size for find node


class NodesMessage(ChunkedKadMessage):
    """Nodes message containing list of known nodes."""

    def __init__(self, sender_id: str, nodes: List[Dict[str, Any]]):
        super().__init__("NODES", sender_id)
        self.nodes = nodes if nodes is not None else []
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        return 32 + len(self.nodes) * 24  # Base size + node info


# ==================================================================================
# 2. CHUNKEDKAD CLUSTER ASSIGNMENT MESSAGES
# ==================================================================================

@dataclass
class ClusterInfo:
    """Information about a cluster assignment."""
    cluster_id: int
    node_ids: List[str]
    assigned_chunks: List[int]


class ClusterAssignmentMessage(ChunkedKadMessage):
    """
    Message containing cluster assignments and complete chunk-to-node mapping.
    This is the core message that enables proactive exchange coordination.
    """

    def __init__(self, sender_id: str, block_id: int, bucket_id: int):
        super().__init__("CLUSTER_ASSIGNMENT", sender_id)
        self.block_id = block_id
        self.bucket_id = bucket_id
        self.total_chunks = 0
        self.beta = 3  # Default redundancy factor
        
        # Cluster assignments for this specific bucket
        self.cluster_assignments = {}  # {cluster_id: ClusterInfo}
        
        # CRITICAL: Complete network-wide chunk-to-node mapping
        # This enables nodes to know exactly which peers have which chunks
        self.chunk_to_node_mapping = {}  # {chunk_id: [node_ids]}
        
        # Metadata for chunk reconstruction
        self.block_size = 0
        self.chunk_size = 64 * 1024  # 64KB default
        self.fec_ratio = 0.2  # 20% FEC overhead
        
        self.size = self._calculate_size()

    def add_cluster_assignment(self, cluster_id: int, node_ids: List[str], assigned_chunks: List[int]):
        """Add cluster assignment for a specific cluster."""
        self.cluster_assignments[cluster_id] = ClusterInfo(
            cluster_id=cluster_id,
            node_ids=node_ids,
            assigned_chunks=assigned_chunks
        )
        self.size = self._calculate_size()  # Recalculate size

    def set_chunk_to_node_mapping(self, mapping: Dict[int, List[str]]):
        """Set the complete network-wide chunk-to-node mapping."""
        self.chunk_to_node_mapping = mapping
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        # Base message overhead
        base_size = 64
        
        # Cluster assignment data
        cluster_size = sum(
            16 + len(info.node_ids) * 16 + len(info.assigned_chunks) * 4
            for info in self.cluster_assignments.values()
        )
        
        # Chunk-to-node mapping (this can be large but is essential)
        mapping_size = sum(
            4 + len(node_list) * 16  # chunk_id + node_id list
            for node_list in self.chunk_to_node_mapping.values()
        )
        
        return base_size + cluster_size + mapping_size

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "block_id": self.block_id,
            "bucket_id": self.bucket_id,
            "total_chunks": self.total_chunks,
            "cluster_count": len(self.cluster_assignments),
            "mapping_size": len(self.chunk_to_node_mapping),
            "block_size": self.block_size,
            "chunk_size": self.chunk_size,
            "fec_ratio": self.fec_ratio
        })
        return base


# ==================================================================================
# 3. CHUNKEDKAD PROACTIVE EXCHANGE MESSAGES
# ==================================================================================

class ProactiveExchangeMessage(ChunkedKadMessage):
    """
    Message for coordinating proactive bilateral chunk exchanges.
    This eliminates the need for timeout-based pull mechanisms.
    """

    def __init__(self, sender_id: str, target_id: str, block_id: int):
        super().__init__("PROACTIVE_EXCHANGE", sender_id)
        self.target_id = target_id
        self.block_id = block_id
        
        # Chunks I'm offering to send to the target
        self.chunks_offering = []  # List[int] - chunk IDs
        
        # Chunks I'm requesting from the target
        self.chunks_requesting = []  # List[int] - chunk IDs
        
        # Exchange priority (higher = more urgent)
        self.exchange_priority = 0.0
        
        # Exchange type: 'bilateral', 'unilateral'
        self.exchange_type = "bilateral"
        
        # Timeout for this exchange
        self.exchange_timeout = 5.0  # seconds
        
        self.size = self._calculate_size()

    def set_exchange_details(self, offering: List[int], requesting: List[int], 
                           priority: float = 1.0, exchange_type: str = "bilateral"):
        """Set the details of what chunks to exchange."""
        self.chunks_offering = offering
        self.chunks_requesting = requesting
        self.exchange_priority = priority
        self.exchange_type = exchange_type
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        # Base message + chunk ID lists
        return 48 + len(self.chunks_offering) * 4 + len(self.chunks_requesting) * 4

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "target_id": self.target_id,
            "block_id": self.block_id,
            "chunks_offering_count": len(self.chunks_offering),
            "chunks_requesting_count": len(self.chunks_requesting),
            "exchange_priority": self.exchange_priority,
            "exchange_type": self.exchange_type
        })
        return base


class ProactiveExchangeResponse(ChunkedKadMessage):
    """Response to a proactive exchange request."""

    def __init__(self, sender_id: str, requester_id: str, block_id: int, 
                 accepted: bool, reason: str = ""):
        super().__init__("PROACTIVE_EXCHANGE_RESPONSE", sender_id)
        self.requester_id = requester_id
        self.block_id = block_id
        self.accepted = accepted
        self.reason = reason
        
        # If accepted, which chunks will be sent
        self.chunks_will_send = []  # List[int]
        self.chunks_expect_to_receive = []  # List[int]
        
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        return 32 + len(self.reason) + len(self.chunks_will_send) * 4 + len(self.chunks_expect_to_receive) * 4


# ==================================================================================
# 4. ENHANCED CHUNK MESSAGES FOR CHUNKEDKAD
# ==================================================================================

class ChunkMessage(ChunkedKadMessage):
    """
    Enhanced chunk message supporting ChunkedKad features.
    Compatible with existing Kadcast infrastructure but with additional metadata.
    """

    def __init__(self, sender_id: str, chunk_bytes: bytes, exchange_context: Optional[Dict] = None):
        # Extract metadata from chunk_bytes (using existing RealisticChunkGenerator)
        (
            self.block_id,
            self.chunk_id,
            self.total_chunks,
            self.is_fec,
            self.original_data_size,
            self.chunk_data
        ) = RealisticChunkGenerator.extract_chunk_metadata(chunk_bytes)
        
        super().__init__("CHUNK", sender_id)
        
        # Store the full chunk bytes for forwarding
        self.full_chunk_bytes = chunk_bytes
        
        # ChunkedKad specific fields
        self.is_exchange_chunk = False  # True if from proactive exchange
        self.exchange_id = None  # ID of the exchange this chunk belongs to
        self.cluster_id = None  # Which cluster this chunk was assigned to
        self.height = 64  # Kademlia routing height (starts at 64 for 64-bit IDs)
        
        # Exchange context for tracking
        self.exchange_context = exchange_context or {}

        self.is_secondary_cascade = False
        self.secondary_source_node = None
        
        self.size = self._calculate_size()

    def mark_as_exchange_chunk(self, exchange_id: str, cluster_id: int):
        """Mark this chunk as part of a proactive exchange."""
        self.is_exchange_chunk = True
        self.exchange_id = exchange_id
        self.cluster_id = cluster_id

    def _calculate_size(self) -> int:
        return 17 + len(self.chunk_data)  # header + payload (same as Kadcast)

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "block_id": self.block_id,
            "chunk_id": self.chunk_id,
            "is_fec": self.is_fec,
            "original_data_size": self.original_data_size,
            "total_chunks": self.total_chunks,
            "data_size": len(self.chunk_data),
            "is_exchange_chunk": self.is_exchange_chunk,
            "exchange_id": self.exchange_id,
            "cluster_id": self.cluster_id,
            "height": self.height
        })
        return base


# ==================================================================================
# 5. PARALLEL HEADER BROADCASTING
# ==================================================================================

class BlockHeaderMessage(ChunkedKadMessage):
    """
    Block header message for parallel broadcasting while chunks propagate.
    Enables early validation without waiting for complete chunk assembly.
    """

    def __init__(self, sender_id: str, block_id: int, block_header: Dict[str, Any]):
        super().__init__("BLOCK_HEADER", sender_id)
        self.block_id = block_id
        self.block_header = block_header
        
        # ChunkedKad specific metadata
        self.total_chunks = 0
        self.chunk_size = 64 * 1024  # 64KB
        self.fec_ratio = 0.2
        self.block_size = 0
        
        # Cluster assignment summary (lightweight version)
        self.cluster_summary = {}  # {bucket_id: cluster_count}
        
        # Quick validation data
        self.block_hash = block_header.get("block_hash", "")
        self.merkle_root = block_header.get("merkle_root", "")
        self.prev_hash = block_header.get("prev_hash", "")
        
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        # Header data + metadata
        header_size = sum(len(str(v)) for v in self.block_header.values())
        return 64 + header_size + len(self.cluster_summary) * 8

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "block_id": self.block_id,
            "total_chunks": self.total_chunks,
            "block_size": self.block_size,
            "cluster_count": len(self.cluster_summary),
            "block_hash": self.block_hash,
            "merkle_root": self.merkle_root
        })
        return base


# ==================================================================================
# 6. FALLBACK BATCH PULL MECHANISMS
# ==================================================================================

class BatchPullRequest(ChunkedKadMessage):
    """
    Batch request for missing chunks when proactive exchange doesn't achieve 100% coverage.
    This is the fallback mechanism for edge cases.
    """

    def __init__(self, sender_id: str, block_id: int, missing_chunks: List[int]):
        super().__init__("BATCH_PULL_REQUEST", sender_id)
        self.block_id = block_id
        self.missing_chunks = missing_chunks
        
        # Preferred sources for each missing chunk (from chunk-to-node mapping)
        self.preferred_sources = {}  # {chunk_id: [preferred_node_ids]}
        
        # Request priority (higher = more urgent)
        self.request_priority = 1.0
        
        # Timeout for this batch request
        self.request_timeout = 10.0  # seconds
        
        self.size = self._calculate_size()

    def set_preferred_sources(self, sources: Dict[int, List[str]]):
        """Set preferred source nodes for each missing chunk."""
        self.preferred_sources = sources
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        base_size = 32 + len(self.missing_chunks) * 4
        sources_size = sum(
            4 + len(node_list) * 16 
            for node_list in self.preferred_sources.values()
        )
        return base_size + sources_size

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "block_id": self.block_id,
            "missing_chunks_count": len(self.missing_chunks),
            "has_preferred_sources": len(self.preferred_sources) > 0,
            "request_priority": self.request_priority
        })
        return base


class BatchPullResponse(ChunkedKadMessage):
    """Response to batch pull request containing available chunks."""

    def __init__(self, sender_id: str, requester_id: str, block_id: int):
        super().__init__("BATCH_PULL_RESPONSE", sender_id)
        self.requester_id = requester_id
        self.block_id = block_id
        
        # Which chunks can be provided
        self.available_chunks = []  # List[int] - chunk IDs
        self.unavailable_chunks = []  # List[int] - chunk IDs
        
        # Estimated send time
        self.estimated_send_time = 0.0
        
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        return 48 + len(self.available_chunks) * 4 + len(self.unavailable_chunks) * 4


# ==================================================================================
# 7. REQUEST MESSAGE (From Kadcast - for compatibility)
# ==================================================================================

class ReqMessage(ChunkedKadMessage):
    """Request message for missing chunks (Kadcast compatibility)."""

    def __init__(self, sender_id: str, block_id: int, missing_chunks: List[int]):
        super().__init__("REQ", sender_id)
        self.block_id = block_id
        self.missing_chunks = missing_chunks
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        return 16 + len(self.missing_chunks) * 4  # Header + chunk IDs


# ==================================================================================
# 8. FUTURE EXTENSION POINT - MONITORING (NOT IMPLEMENTED YET)
# ==================================================================================
# Note: Performance monitoring will be handled by the simulation framework
# through the metrics collector, not via network messages to avoid bandwidth overhead.


# ==================================================================================
# 9. MESSAGE TYPE ENUM FOR EASY REFERENCE
# ==================================================================================

class ChunkedKadMessageType:
    """Message type constants for ChunkedKad protocol."""
    
    # Kadcast compatibility messages
    PING = "PING"
    PONG = "PONG"
    FIND_NODE = "FIND_NODE"
    NODES = "NODES"
    CHUNK = "CHUNK"
    REQ = "REQ"
    
    # ChunkedKad specific messages
    CLUSTER_ASSIGNMENT = "CLUSTER_ASSIGNMENT"
    PROACTIVE_EXCHANGE = "PROACTIVE_EXCHANGE"
    PROACTIVE_EXCHANGE_RESPONSE = "PROACTIVE_EXCHANGE_RESPONSE"
    BLOCK_HEADER = "BLOCK_HEADER"
    BATCH_PULL_REQUEST = "BATCH_PULL_REQUEST"
    BATCH_PULL_RESPONSE = "BATCH_PULL_RESPONSE"


# ==================================================================================
# 10. UTILITY FUNCTIONS
# ==================================================================================

def create_exchange_id(sender_id: str, target_id: str, block_id: int, timestamp: float) -> str:
    """Create a unique exchange ID for tracking proactive exchanges."""
    return f"exchange_{sender_id[:8]}_{target_id[:8]}_{block_id}_{int(timestamp * 1000)}"


def calculate_complementary_chunks(my_chunks: List[int], total_chunks: int, beta: int = 3) -> List[int]:
    """
    Calculate which chunks would be complementary for proactive exchange.
    In ChunkedKad's round-robin assignment, complementary chunks are:
    - If I have cluster 0 (chunks 0,3,6,9...), I need cluster 1 (1,4,7,10...) and cluster 2 (2,5,8,11...)
    """
    if not my_chunks:
        return []
    
    # Determine my cluster based on the pattern of my chunks
    my_cluster = my_chunks[0] % beta
    
    # Calculate complementary chunks from other clusters
    complementary = []
    for chunk_id in range(total_chunks):
        if chunk_id % beta != my_cluster:
            complementary.append(chunk_id)
    
    return complementary


def analyze_chunk_distribution(chunk_to_node_mapping: Dict[int, List[str]], 
                             target_node: str) -> Dict[str, List[int]]:
    """
    Analyze chunk distribution to find the best exchange partners for a target node.
    Returns a mapping of {partner_node_id: [chunks_they_have]}
    """
    partner_chunks = {}
    
    for chunk_id, node_list in chunk_to_node_mapping.items():
        for node_id in node_list:
            if node_id != target_node:
                if node_id not in partner_chunks:
                    partner_chunks[node_id] = []
                partner_chunks[node_id].append(chunk_id)
    
    return partner_chunks


# ==================================================================================
# 11. LOGGING AND DEBUGGING UTILITIES
# ==================================================================================

def log_message_stats(message: ChunkedKadMessage, extra_info: str = ""):
    """Log message statistics for debugging."""
    logger.debug(f"Message {message.message_type}: {message.size} bytes, "
                f"sender={message.sender_id[:8]}, {extra_info}")


def get_message_size_breakdown() -> Dict[str, int]:
    """Get typical message sizes for different message types."""
    return {
        "PING": 8,
        "PONG": 16,
        "FIND_NODE": 32,
        "NODES": 128,  # Variable, example for 4 nodes
        "CHUNK": 65553,  # 64KB chunk + 17 byte header
        "CLUSTER_ASSIGNMENT": 1024,  # Variable, depends on mapping size
        "PROACTIVE_EXCHANGE": 64,  # Small coordination message
        "BLOCK_HEADER": 256,  # Header + metadata
        "BATCH_PULL_REQUEST": 128,  # Variable, depends on missing chunks
        "REQ": 32  # Small request message
    }