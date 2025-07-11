"""
Mercury Protocol Messages Implementation

Implements message types for the Mercury protocol following the same pattern
as Kadcast and Cougar protocols for consistent architecture.

Mercury uses block broadcasting with chunking (no FEC, just like original code)
adapted from transaction-style messaging pattern.
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

logger = logging.getLogger("MercuryMessages")


class MercuryMessage(ABC):
    """Base class for all Mercury protocol messages."""

    def __init__(self, message_type: str, sender_id: str):
        self.message_type = message_type
        self.sender_id = sender_id
        self.timestamp = time.time()
        self.size = 0

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


class MercuryVivaldiUpdateMessage(MercuryMessage):
    """
    Vivaldi coordinate update message.
    
    Used during the VCS convergence phase for RTT measurements
    and coordinate updates (Mercury Algorithm 1).
    """

    def __init__(self, sender_id: str, coordinate: List[float], error_indicator: float, 
                 query_peers: List[str]):
        super().__init__("VIVALDI_UPDATE", sender_id)
        self.coordinate = coordinate  # 3D virtual coordinate [x, y, z]
        self.error_indicator = error_indicator  # ei from paper
        self.query_peers = query_peers  # 16 peers for RTT measurement
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        # 3 coordinates (8 bytes each) + error (8 bytes) + peer list
        coordinate_size = 3 * 8  # 3D coordinates
        error_size = 8
        peer_list_size = len(self.query_peers) * 16  # Node ID size
        return coordinate_size + error_size + peer_list_size + 32  # Base overhead


class MercuryVivaldiResponseMessage(MercuryMessage):
    """
    Response to Vivaldi update containing coordinate and RTT measurement.
    """

    def __init__(self, sender_id: str, coordinate: List[float], error_indicator: float,
                 measured_rtt: float):
        super().__init__("VIVALDI_RESPONSE", sender_id)
        self.coordinate = coordinate
        self.error_indicator = error_indicator
        self.measured_rtt = measured_rtt
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        return 3 * 8 + 8 + 8 + 16  # Coordinates + error + RTT + overhead


class MercuryBlockDigestMessage(MercuryMessage):
    """
    Block digest announcement message.
    
    Mercury adaptation: Similar to transaction digest in original paper,
    but adapted for block broadcasting with chunking.
    """

    def __init__(self, sender_id: str, block_id: int, block_hash: str, 
                 total_chunks: int, block_size: int):
        super().__init__("BLOCK_DIGEST", sender_id)
        self.block_id = block_id
        self.block_hash = block_hash  # Block hash for verification
        self.total_chunks = total_chunks  # Number of chunks
        self.block_size = block_size  # Original block size
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        # Block ID (4) + hash (32) + chunk count (4) + size (4) + overhead
        return 4 + 32 + 4 + 4 + 16


class MercuryChunkRequestMessage(MercuryMessage):
    """
    Chunk request message for pull-based chunk transfer.
    
    Mercury adaptation: Similar to transaction body request,
    adapted for chunked block transfer.
    """

    def __init__(self, sender_id: str, block_id: int, chunk_ids: List[int]):
        super().__init__("CHUNK_REQUEST", sender_id)
        self.block_id = block_id
        self.chunk_ids = chunk_ids  # List of chunk IDs needed
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        # Block ID (4) + chunk count (4) + chunk IDs (4 each) + overhead
        return 4 + 4 + (len(self.chunk_ids) * 4) + 16


class MercuryChunkResponseMessage(MercuryMessage):
    """
    Chunk response message containing actual chunk data.
    
    Uses RealisticChunkGenerator format for compatibility with existing system.
    """

    def __init__(self, sender_id: str, block_id: int, chunk_id: int, 
                 chunk_data: bytes, total_chunks: int):
        super().__init__("CHUNK_RESPONSE", sender_id)
        self.block_id = block_id
        self.chunk_id = chunk_id
        self.chunk_data = chunk_data  # Actual chunk bytes (with metadata)
        self.total_chunks = total_chunks
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        # Block ID (4) + Chunk ID (4) + Total (4) + data size + overhead
        return 4 + 4 + 4 + len(self.chunk_data) + 16


class MercuryBlockCompleteMessage(MercuryMessage):
    """
    Block completion notification message.
    
    Sent when a node successfully reconstructs a complete block.
    """

    def __init__(self, sender_id: str, block_id: int, completion_time: float):
        super().__init__("BLOCK_COMPLETE", sender_id)
        self.block_id = block_id
        self.completion_time = completion_time
        self.size = self._calculate_size()

    def _calculate_size(self) -> int:
        return 4 + 8 + 16  # Block ID + timestamp + overhead


@dataclass
class MercuryConfig:
    """
    Mercury protocol configuration - fully configurable as requested.
    """
    # Clustering Configuration
    K: int = 8                      # Number of clusters
    dcluster: int = 4               # Close neighbors per cluster
    dmax: int = 8                   # Total relay peers
    drandom: int = 4                # Random neighbors (dmax - dcluster)
    
    # VCS Configuration
    vcs_dimensions: int = 3         # Coordinate space dimensions
    coordinate_rounds: int = 100    # Vivaldi convergence rounds (COORDINATE_UPDATE_ROUND)
    measurements_per_round: int = 16  # RTT measurements per round
    
    # Newton Rules (Security) Configuration
    enable_newton_rules: bool = True
    error_limit: float = 0.25       # ce constant
    adaptive_timestep: float = 0.25 # cc constant
    force_limit: float = 100.0      # Fmax
    gravity_rho: float = 500.0      # Gravity constant
    centroid_drift_limit: float = 50.0  # Drift threshold
    stability_threshold: float = 0.4    # Stable node threshold (ei < 0.4)
    
    # Early Outburst Configuration
    enable_early_outburst: bool = True
    source_fanout: int = 128        # Early outburst fanout (ROOT_FANOUT)
    
    # Block Broadcasting Configuration
    enable_chunking: bool = True
    chunk_size: int = 1024          # 1KB chunks
    fec_ratio: float = 0.0          # No FEC (0.0) like original Mercury
    
    def get_drandom(self) -> int:
        """Calculate random neighbors (derived parameter)."""
        return self.dmax - self.dcluster
    
    def get_adaptive_K(self, node_count: int) -> int:
        """Calculate adaptive K based on network size."""
        if node_count <= 1000:
            return self.K
        elif node_count <= 5000:
            return min(16, self.K + 4)
        else:
            return min(32, self.K + 8)
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'MercuryConfig':
        """Create config from dictionary for easy customization."""
        config = cls()
        for key, value in config_dict.items():
            if hasattr(config, key):
                setattr(config, key, value)
        return config


class MercuryMessageType:
    """Mercury message type constants."""
    VIVALDI_UPDATE = "VIVALDI_UPDATE"
    VIVALDI_RESPONSE = "VIVALDI_RESPONSE"
    BLOCK_DIGEST = "BLOCK_DIGEST"
    CHUNK_REQUEST = "CHUNK_REQUEST"
    CHUNK_RESPONSE = "CHUNK_RESPONSE"
    BLOCK_COMPLETE = "BLOCK_COMPLETE"


class MercuryNetworkPhase:
    """Mercury simulation phases."""
    INITIALIZATION = "initialization"
    VCS_CONVERGENCE = "vcs_convergence"
    CLUSTERING = "clustering"
    BLOCK_BROADCASTING = "block_broadcasting"
    METRICS_COLLECTION = "metrics_collection"