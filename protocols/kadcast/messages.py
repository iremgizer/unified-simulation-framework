import logging # Ekledim, hata ayıklama için faydalı olabilir
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import time

from src.data.realistic_payload import RealisticChunkGenerator


class KadcastMessage(ABC):
    """Base class for all Kadcast protocol messages."""

    def __init__(self, message_type: str, sender_id: str):
        self.message_type = message_type
        self.sender_id = sender_id
        self.timestamp = time.time()
        self.size = 0 # Boyutu burada başlat, türetilmiş sınıflar ayarlayacak

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


class PingMessage(KadcastMessage):
    """Ping message for peer discovery."""

    def __init__(self, sender_id: str):
        super().__init__("PING", sender_id)
        self.size = self._calculate_size() # Nitelikler ayarlandıktan sonra boyutu hesapla


    def _calculate_size(self) -> int:
        return 8  # Small fixed size for ping


class PongMessage(KadcastMessage):
    """Pong message in response to ping."""

    def __init__(self, sender_id: str, ping_sender_id: str):
        super().__init__("PONG", sender_id)
        self.ping_sender_id = ping_sender_id
        self.size = self._calculate_size() # Nitelikler ayarlandıktan sonra boyutu hesapla


    def _calculate_size(self) -> int:
        return 16  # Small fixed size for pong


class FindNodeMessage(KadcastMessage):
    """Find node message for peer discovery."""

    def __init__(self, sender_id: str, target_id: str):
        super().__init__("FIND_NODE", sender_id)
        self.target_id = target_id
        self.size = self._calculate_size() # Nitelikler ayarlandıktan sonra boyutu hesapla


    def _calculate_size(self) -> int:
        return 32  # Fixed size for find node


class NodesMessage(KadcastMessage):
    """Nodes message containing list of known nodes."""

    def __init__(self, sender_id: str, nodes: List[Dict[str, Any]]):
        super().__init__("NODES", sender_id)
        # Eğer 'nodes' None gelirse boş liste olarak ayarla
        self.nodes = nodes if nodes is not None else [] 
        self.size = self._calculate_size() # Nitelikler ayarlandıktan sonra boyutu hesapla


    def _calculate_size(self) -> int:
        return 32 + len(self.nodes) * 24  # Base size + node info


class ChunkMessage(KadcastMessage):
    """
    Message containing a chunk of a block.
    """

    def __init__(self, sender_id: str, chunk_bytes: bytes):
        # DÜZELTME: Orijinal chunk_bytes'ı sakla (header + payload)
        self.full_chunk_bytes = chunk_bytes
        
        # Extract metadata first
        (
            self.block_id,
            self.chunk_id,
            self.total_chunks,
            self.is_fec,
            self.original_data_size,
            self.chunk_data
        ) = RealisticChunkGenerator.extract_chunk_metadata(chunk_bytes)

        super().__init__("CHUNK", sender_id)
        self.size = self._calculate_size() # Nitelikler ayarlandıktan sonra boyutu hesapla


    def _calculate_size(self) -> int:
        return 17 + len(self.chunk_data)  # header + payload

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "block_id": self.block_id,
            "chunk_id": self.chunk_id,
            "is_fec": self.is_fec,
            "original_data_size": self.original_data_size,
            "total_chunks": self.total_chunks,
            "data_size": len(self.chunk_data)
        })
        return base


class ReqMessage(KadcastMessage):
    """Request message for missing chunks."""

    def __init__(self, sender_id: str, block_id: int, missing_chunks: List[int]):
        super().__init__("REQ", sender_id)
        self.block_id = block_id
        self.missing_chunks = missing_chunks
        self.size = self._calculate_size() # Nitelikler ayarlandıktan sonra boyutu hesapla


    def _calculate_size(self) -> int:
        return 16 + len(self.missing_chunks) * 4  # Header + chunk IDs
