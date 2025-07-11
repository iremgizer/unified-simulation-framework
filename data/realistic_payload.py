"""
Realistic message payload implementation for the blockchain simulation.

This module provides classes for generating and handling realistic message payloads
to improve simulation fidelity.
"""


import math
import hashlib
import random
import json
import struct
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Tuple
from models.transaction_type import TxType  
from pyraptorq import Encoder, Decoder

logger = logging.getLogger(__name__)

class TransactionGenerator:
    """
    Generates realistic blockchain transactions for message payloads.
    """

    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the transaction generator.

        Args:
            seed: Random seed for reproducibility
        """
        self.random = random.Random(seed)
        self.address_cache = {}  # Cache of generated addresses

    def generate_address(self) -> str:
        """
        Generate a realistic blockchain address.

        Returns:
            A hexadecimal string representing a blockchain address
        """
        addr_bytes = bytes([self.random.randint(0, 255) for _ in range(20)])
        return "0x" + addr_bytes.hex()

    def get_or_create_address(self, index: int) -> str:
        """
        Get a cached address or create a new one.

        Args:
            index: Address index

        Returns:
            Blockchain address
        """
        if index not in self.address_cache:
            self.address_cache[index] = self.generate_address()
        return self.address_cache[index]

    def generate_transaction(self, tx_id: int, tx_type: TxType = TxType.TOKEN_TRANSFER) -> Dict[str, Any]:
        """
        Generate a realistic transaction.

        Args:
            tx_id: Unique transaction identifier
            tx_type: Type of transaction

        Returns:
            Dictionary representing the transaction
        """
        from_index = self.random.randint(0, 1000)
        to_index = self.random.randint(0, 1000)
        tx = {
            "id": tx_id,
            "from": self.get_or_create_address(from_index),
            "to": self.get_or_create_address(to_index),
            "amount": round(self.random.uniform(0.01, 100.0), 8),
            "timestamp": self.random.randint(1600000000, 1700000000),
            "type": tx_type.name
        }

        if tx_type == TxType.SMART_CONTRACT:
            tx["contract_call"] = {
                "method": "setData",
                "params": {
                    "key": f"data_{tx_id}",
                    "value": self.random.randint(0, 100000)
                }
            }

        return tx

    def generate_block_transactions(self, block_id: int, size_limit_bytes: int, token_ratio: int = 100) -> List[Dict[str, Any]]:
        """
        Generate a list of transactions for a block with size constraint and type ratio.

        Args:
            block_id: ID of the block
            size_limit_bytes: Target total size of transactions
            token_ratio: Percent of token transfers vs. smart contracts

        Returns:
            List of transaction dictionaries
        """
        avg_sizes = {
            TxType.TOKEN_TRANSFER: 250,
            TxType.SMART_CONTRACT: 800
        }

        txs = []
        total = 0
        tx_counter = 0

        while total < size_limit_bytes:
            tx_type = (
                TxType.TOKEN_TRANSFER
                if self.random.randint(1, 100) <= token_ratio
                else TxType.SMART_CONTRACT
            )
            tx = self.generate_transaction(block_id * 10000 + tx_counter, tx_type)
            tx_size = avg_sizes[tx_type]
            if total + tx_size > size_limit_bytes:
                break
            tx["size"] = tx_size
            txs.append(tx)
            total += tx_size
            tx_counter += 1

        return txs
    
    def serialize_transaction(self, tx: Dict[str, Any]) -> bytes:
        """
        Serialize a transaction to bytes.
        
        Args:
            tx: Transaction dictionary
            
        Returns:
            Serialized transaction bytes
        """
        # Simple serialization for simulation purposes
        return json.dumps(tx).encode('utf-8')
    
    def serialize_transactions(self, txs: List[Dict[str, Any]]) -> bytes:
        """
        Serialize a list of transactions to bytes.
        
        Args:
            txs: List of transaction dictionaries
            
        Returns:
            Serialized transactions bytes
        """
        return json.dumps(txs).encode('utf-8')


class RealisticBlockGenerator:
    """
    Generates realistic blockchain blocks with transactions.
    """
    
    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the block generator.
        
        Args:
            seed: Random seed for reproducibility
        """
        self.tx_generator = TransactionGenerator(seed)
        self.random = random.Random(seed)
        
    def generate_block_header(self, block_id: int, prev_hash: str, tx_root: str) -> Dict[str, Any]:
        """
        Generate a realistic block header.
        
        Args:
            block_id: Block ID
            prev_hash: Previous block hash
            tx_root: Merkle root of transactions
            
        Returns:
            Block header as a dictionary
        """
        # Generate timestamp
        timestamp = int(self.random.random() * 1000000)
        
        # Generate difficulty
        difficulty = self.random.randint(1000000, 10000000)
        
        # Generate nonce
        nonce = self.random.randint(0, 2**64 - 1)
        
        # Create header
        header = {
            "block_id": block_id,
            "prev_hash": prev_hash,
            "tx_root": tx_root,
            "timestamp": timestamp,
            "difficulty": difficulty,
            "nonce": nonce
        }
        
        return header
    
    def calculate_merkle_root(self, tx_hashes: List[str]) -> str:
        """
        Calculate Merkle root from transaction hashes.
        
        Args:
            tx_hashes: List of transaction hashes
            
        Returns:
            Merkle root hash
        """
        if not tx_hashes:
            return "0x" + "0" * 64
            
        # If odd number of hashes, duplicate the last one
        if len(tx_hashes) % 2 == 1:
            tx_hashes.append(tx_hashes[-1])
            
        # Pair hashes and hash them together
        next_level = []
        for i in range(0, len(tx_hashes), 2):
            combined = tx_hashes[i] + tx_hashes[i+1].replace("0x", "")
            next_hash = "0x" + hashlib.sha256(combined.encode()).hexdigest()
            next_level.append(next_hash)
            
        # Recurse until we have a single hash
        if len(next_level) > 1:
            return self.calculate_merkle_root(next_level)
        else:
            return next_level[0]
    
    
    def generate_block(self, block_id: int, prev_hash: str, block_size_bytes: int, token_ratio: int = 100) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Generate a realistic block with transactions, given a total data size and transaction type ratio.

        
        Args:
            block_id: Block ID
            prev_hash: Previous block hash
            block_size_bytes: Desired total block data size in bytes (only transaction data)
            token_ratio: Percentage of token transactions vs smart contracts

        Returns:
            Tuple of (block header, transactions)
        """
        # Generate transactions based on size constraint and type ratio
        transactions = self.tx_generator.generate_block_transactions(
            block_id=block_id,
            size_limit_bytes=block_size_bytes,
            token_ratio=token_ratio
        )

        # Calculate transaction hashes
        tx_hashes = [
            "0x" + hashlib.sha256(self.tx_generator.serialize_transaction(tx)).hexdigest()
            for tx in transactions
        ]

        # Calculate Merkle root
        tx_root = self.calculate_merkle_root(tx_hashes)

        # Generate header
        header = self.generate_block_header(block_id, prev_hash, tx_root)

        return header, transactions


    def serialize_block(self, header: Dict[str, Any], transactions: List[Dict[str, Any]]) -> bytes:
        """
        Serialize a block to bytes.
        
        Args:
            header: Block header
            transactions: List of transactions
            
        Returns:
            Serialized block bytes
        """
        block = {
            "header": header,
            "transactions": transactions
        }
        return json.dumps({"header": header, "transactions": transactions}).encode('utf-8')

    
    def calculate_block_size(self, header: Dict[str, Any], transactions: List[Dict[str, Any]]) -> int:
        """
        Calculate the size of a block in bytes.
        
        Args:
            header: Block header
            transactions: List of transactions
            
        Returns:
            Block size in bytes
        """
        serialized = self.serialize_block(header, transactions)
        return len(serialized)


class RealisticChunkGenerator:
    """
    MINIMAL DEĞİŞİKLİK: Sadece FEC ratio'yu dinamik hale getir
    """
    
    @staticmethod
    def chunkify_block(data: bytes, chunk_size: int, block_id: int, fec_ratio: float = 0.2) -> List[bytes]:
        """
        SADECE FEC_RATIO PARAMETRESİ DİNAMİK HALE GETİRİLDİ
        """
        logger.info(f"[CHUNKIFY] Block {block_id}: Using FEC ratio {fec_ratio}")
        
        if fec_ratio < 1.0:
            return RealisticChunkGenerator.chunkify_block_with_fec(data, chunk_size, block_id, fec_ratio)
        else:
            return RealisticChunkGenerator.chunkify_block_without_fec(data, chunk_size, block_id)

    @staticmethod
    def chunkify_block_without_fec(data: bytes, chunk_size: int, block_id: int) -> List[bytes]:
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        total = len(chunks)
        logger.info(f"[CHUNKIFY_NO_FEC] Block {block_id}: {len(data)} bytes -> {total} chunks")
        
        return [
            RealisticChunkGenerator.add_chunk_metadata(chunk, block_id, i, total, is_parity=False, original_data_size=len(data))
            for i, chunk in enumerate(chunks)
        ]

    @staticmethod
    def chunkify_block_with_fec(data: bytes, chunk_size: int, block_id: int, fec_ratio: float) -> List[bytes]:
        encoder = Encoder(data, symbol_size=chunk_size)

        source_symbols = math.ceil(len(data) / chunk_size)
        total_symbols = math.ceil(source_symbols * (1 + fec_ratio))

        logger.info(f"[CHUNKIFY_FEC] Block {block_id}: {len(data)} bytes, chunk_size={chunk_size}, fec_ratio={fec_ratio}")
        logger.info(f"[CHUNKIFY_FEC] Source symbols: {source_symbols}, Total symbols (with FEC): {total_symbols}")

        symbols = []
        for i in range(total_symbols):
           try:
            symbol_data = encoder.gen_symbol(i)
            is_parity = i >= source_symbols
           
            chunk_with_meta = RealisticChunkGenerator.add_chunk_metadata(
              symbol_data, block_id, i, total_symbols, is_parity, len(data)
            )
         
            symbols.append(chunk_with_meta)

           except Exception as e: 
            logger.error(f"[ERROR] Failed to generate symbol {i}: {e}")
            continue

        logger.info(f"[CHUNKIFY_FEC] Generated {len(symbols)} total symbols")
        return symbols

    @staticmethod
    def dechunkify_block(chunks: List[bytes]) -> Tuple[bytes, int]:
        if not chunks:
           return b"", 0

        symbol_dict = {}
        block_id = None
        total_chunks = None
        chunk_size = None
        original_data_size = None

        #logger.debug(f"[DECHUNKIFY] Starting dechunkify process with {len(chunks)} chunks...")

        for chunk in chunks:
            block_id_, chunk_id, total, is_parity, data_size, data = RealisticChunkGenerator.extract_chunk_metadata(chunk)

            symbol_dict[chunk_id] = data
            block_id = block_id_ if block_id is None else block_id
            total_chunks = total if total_chunks is None else total_chunks
            chunk_size = chunk_size or len(data)
            original_data_size = data_size if original_data_size is None else original_data_size

        
        if total_chunks is None or chunk_size is None or original_data_size is None:
          raise ValueError("[ERROR] Missing required metadata from chunks")
        
        #logger.debug(f"[DECHUNKIFY] Block {block_id}: total_chunks={total_chunks}, received={len(symbol_dict)}, chunk_size={chunk_size}, original_size={original_data_size}")

        required_symbols = math.ceil(original_data_size / chunk_size)
        #logger.debug(f"[DECHUNKIFY] Required minimum symbols to decode: {required_symbols}")

        decoder = Decoder(
          symbol_count=required_symbols,
          symbol_size=chunk_size,
          data_size=original_data_size
        )

        symbols_added = 0
        for sid, data in symbol_dict.items():
            decoder.add_symbol(sid, data)
            symbols_added += 1

        #logger.debug(f"[DECHUNKIFY] Added {symbols_added} symbols to decoder")

        if decoder.may_try_decode():
            #logger.debug("[DECHUNKIFY] Decoder reports it may try to decode.")
            output = decoder.try_decode()
            if output is not None:
              #logger.info(f"[DECHUNKIFY_SUCCESS] Block {block_id} decoder successfully reconstructed {len(output)} bytes")
              return output, block_id
            else:
                raise ValueError("[ERROR] Decoder failed to reconstruct data.")
        else:
            raise ValueError(f"[ERROR] Insufficient symbols to decode block. Need {required_symbols}, have {symbols_added}")

    @staticmethod
    def add_chunk_metadata(data: bytes, block_id: int, chunk_id: int, total_chunks: int, is_parity: bool = False, original_data_size: int = 0) -> bytes:
       header = struct.pack(">III?I", block_id, chunk_id, total_chunks, is_parity, original_data_size)
       return header + data
    
    @staticmethod
    def extract_chunk_metadata(chunk: bytes) -> Tuple[int, int, int, bool, int, bytes]: 
       if len(chunk) < 17:
           raise ValueError(f"Chunk too short: {len(chunk)} bytes, expected at least 17")
        
       header = chunk[:17]
    
       try:
           block_id, chunk_id, total, is_parity, original_data_size = struct.unpack(">III?I", header)
           return block_id, chunk_id, total, is_parity, original_data_size, chunk[17:]
        
       except struct.error as e:
           logger.error(f"[CHUNK_ERROR] Struct unpack failed: {e}")
           logger.error(f"  Header hex: {header.hex()}")
           raise
       

    @staticmethod  
    def chunkify_block_mercury(data: bytes, chunk_size: int, block_id: int) -> List[bytes]:
        """
        Mercury-specific chunking without FEC.
    
        Args:
        data: Block data to chunk
        chunk_size: Size of each chunk  
        block_id: Block identifier
        
        Returns:
        List of chunks with Mercury-compatible metadata
        """
        import struct
    
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        total_chunks = len(chunks)
    
        logger.info(f"[MERCURY_CHUNKIFY] Block {block_id}: {len(data)} bytes -> {total_chunks} chunks (NO FEC)")
    
    # Add Mercury-compatible metadata to each chunk
        mercury_chunks = []
        for i, chunk in enumerate(chunks):
        # Mercury metadata: block_id (4) + chunk_id (4) + total (4) = 12 bytes
            metadata = struct.pack(">III", block_id, i, total_chunks)
            mercury_chunk = metadata + chunk
            mercury_chunks.append(mercury_chunk)
    
        return mercury_chunks

    @staticmethod
    def dechunkify_block_mercury(chunks: List[bytes]) -> Tuple[bytes, int]:
        """
        Mercury-specific block reconstruction from chunks (no FEC).
    
       Args:
            chunks: List of Mercury chunks with metadata
        
        Returns:
           Tuple of (reconstructed_data, block_id)
        """
        import struct
    
        if not chunks:
            return b"", 0
    
        chunk_dict = {}
        block_id = None
        total_chunks = None
    
    # Extract metadata and data from each chunk
        for chunk in chunks:
            if len(chunk) < 12:  # Minimum metadata size (3 * 4 bytes)
                logger.warning(f"[MERCURY_DECHUNKIFY] Chunk too short: {len(chunk)} bytes")
                continue
             
        # Extract Mercury metadata: block_id, chunk_id, total_chunks
            metadata = chunk[:12]
            data = chunk[12:]
        
            try:
                bid, cid, total = struct.unpack(">III", metadata)
                chunk_dict[cid] = data
                block_id = bid if block_id is None else block_id
                total_chunks = total if total_chunks is None else total_chunks
            except struct.error as e:
                logger.error(f"[MERCURY_DECHUNKIFY] Invalid chunk metadata: {e}")
                continue
    
        if total_chunks is None or len(chunk_dict) < total_chunks:
            raise ValueError(f"[MERCURY_DECHUNKIFY] Insufficient chunks. Need {total_chunks}, have {len(chunk_dict)}")
    
        # Reconstruct data in order
        reconstructed_data = b""
        for i in range(total_chunks):
            if i in chunk_dict:
                reconstructed_data += chunk_dict[i]
            else:
                raise ValueError(f"[MERCURY_DECHUNKIFY] Missing chunk {i}")
    
        logger.info(f"[MERCURY_DECHUNKIFY] Block {block_id} reconstructed: {len(reconstructed_data)} bytes")
        return reconstructed_data, block_id
