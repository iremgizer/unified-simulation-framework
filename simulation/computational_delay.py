"""
Computational delay modeling for the blockchain simulation.

This module provides classes for calculating realistic computational delays
for block processing and validation in the blockchain network.

UPDATED: Added header validation function and enhanced configurations.
"""

import math
import random
import json
from typing import Dict, List, Optional, Any
from enum import Enum

# Import transaction types
try:
    from src.models.transaction_types import TxType
except ImportError:
    # Fallback if import fails
    class TxType(Enum):
        TOKEN_TRANSFER = "TOKEN_TRANSFER"
        SMART_CONTRACT = "SMART_CONTRACT"
        HEADER_VALIDATION = "HEADER_VALIDATION"  # Added for header validation

class NodePerformanceType(Enum):
    """Node hardware performance categories."""
    HIGH_PERFORMANCE = "high_performance"
    AVERAGE_PERFORMANCE = "average_performance" 
    LOW_PERFORMANCE = "low_performance"

class ComputationalDelayCalculator:
    """
    Calculates computational delays for block processing and validation.
    
    Models the CPU time required to process and validate blocks based on
    their size, transaction count, complexity, and node hardware performance.
    
    NEW: Includes dedicated header validation modeling for Cougar protocol.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the computational delay calculator.
        
        Args:
            config: Configuration parameters which may include:
                   - node_performance_assignments: Dict[int, str] mapping node_id -> performance_type
                   - node_performance_variation: float for random variation
                   - performance_distribution: Dict for default distribution (if no assignments)
        """
        self.config = config or {}
        
        # Node Performance Configurations - ENHANCED with header validation
        self.performance_configs = {
            NodePerformanceType.HIGH_PERFORMANCE: {
                "cpu_factor": 1.5,                    # 50% faster
                "base_validation": 0.025,              # 25ms base (realistic)
                "per_tx_validation": 0.0001,          # 0.1ms per tx
                "fec_processing": 0.00008,             # 0.08ms per KB
                "signature_verification": 0.002,       # 2ms for signature verification
                "pow_verification": 0.0008,           # 0.8ms for PoW verification
                "pos_verification": 0.0003,           # 0.3ms for PoS verification
                "hash_computation": 0.0003,           # 0.3ms per hash operation
                "description": "High-end server (32 cores, NVMe SSD)"
            },
            NodePerformanceType.AVERAGE_PERFORMANCE: {
                "cpu_factor": 1.0,                    # Baseline
                "base_validation": 0.040,              # 40ms base (realistic)
                "per_tx_validation": 0.0002,          # 0.2ms per tx
                "fec_processing": 0.0001,              # 0.1ms per KB
                "signature_verification": 0.003,       # 3ms for signature verification  
                "pow_verification": 0.001,            # 1ms for PoW verification
                "pos_verification": 0.0005,           # 0.5ms for PoS verification
                "hash_computation": 0.0005,           # 0.5ms per hash operation
                "description": "Average consumer hardware (8 cores, SSD)"
            },
            NodePerformanceType.LOW_PERFORMANCE: {
                "cpu_factor": 0.8,                    # 20% slower
                "base_validation": 0.055,              # 55ms base (realistic)
                "per_tx_validation": 0.0005,          # 0.5ms per tx  
                "fec_processing": 0.00015,             # 0.15ms per KB
                "signature_verification": 0.004,       # 4ms for signature verification
                "pow_verification": 0.0015,           # 1.5ms for PoW verification  
                "pos_verification": 0.0008,           # 0.8ms for PoS verification
                "hash_computation": 0.0008,           # 0.8ms per hash operation
                "description": "Low-end hardware (4 cores, HDD)"
            }
        }
        
        # Transaction Type Specific Processing Times
        self.tx_processing_times = {
            TxType.TOKEN_TRANSFER: {
                "signature_verify": 0.0002,    # 0.2ms - ECDSA verification
                "state_check": 0.00005,        # 0.05ms - Balance validation
                "state_update": 0.00003,       # 0.03ms - Update balances
                "total_generation": 0.0001,    # 0.1ms - For generation
                "total_validation": 0.00028    # 0.28ms - For validation
            },
            TxType.SMART_CONTRACT: {
                "signature_verify": 0.0003,    # 0.3ms - ECDSA + contract verification
                "bytecode_validation": 0.0002, # 0.2ms - Bytecode format check
                "gas_calculation": 0.00005,    # 0.05ms - Gas limit validation
                "contract_execution": 0.0005,  # 0.5ms - Contract state execution
                "state_operations": 0.0002,    # 0.2ms - State read/write
                "total_generation": 0.0008,    # 0.8ms - For generation
                "total_validation": 0.00125    # 1.25ms - For validation
            },
            # NEW: Header validation pseudo-transaction
            TxType.HEADER_VALIDATION: {
                "signature_verify": 0.0001,    # 0.1ms - Lightweight header signature
                "hash_verify": 0.00005,        # 0.05ms - Block hash verification
                "total_generation": 0.00005,   # 0.05ms - For generation
                "total_validation": 0.00015    # 0.15ms - For validation
            }
        }
        
        # Processing overhead times - ENHANCED
        self.processing_overheads = {
            "merkle_tree_per_tx": 0.00002,          # 0.02ms per tx for merkle operations
            "block_header_generation": 0.0002,      # 0.2ms for header creation
            "json_parsing_per_kb": 0.00002,         # 0.02ms per KB for JSON operations
            "chunk_header_parsing": 0.00001,        # 0.01ms per chunk header
            "chunk_assembly_per_chunk": 0.00001,    # 0.01ms per chunk assembly
            "integrity_check": 0.0001,              # 0.1ms for basic integrity checks
            "hash_computation": 0.0005,             # 0.5ms per hash operation (SHA-256)
            "signature_verification_base": 0.003,   # 3ms base signature verification
            "timestamp_validation": 0.00001,        # 0.01ms for timestamp checks
            "structure_validation": 0.00005,        # 0.05ms for structure validation
            "consensus_validation": 0.0001,         # 0.1ms for consensus validation
        }
        
        # Node performance variation
        self.node_performance_variation = self.config.get("node_performance_variation", 0.1)  # ±10% variation
        
        # Cache for node performance factors and types
        self.node_performance_factors = {}
        self.node_performance_types = {}
        
        # PRE-ASSIGNED PERFORMANCE TYPES (from simulator)
        self.node_performance_assignments = self.config.get("node_performance_assignments", {})
        
        # Default performance type distribution (fallback for random assignment)
        self.default_performance_distribution = self.config.get("performance_distribution", {
            NodePerformanceType.HIGH_PERFORMANCE: 0.15,    # 15% high-end nodes
            NodePerformanceType.AVERAGE_PERFORMANCE: 0.70, # 70% average nodes  
            NodePerformanceType.LOW_PERFORMANCE: 0.15      # 15% low-end nodes
        })
        
        # Random seed for deterministic behavior (if provided)
        self.random_seed = self.config.get("random_seed", None)
        if self.random_seed is not None:
            random.seed(self.random_seed)
        
    def get_node_performance_type(self, node_id: int) -> NodePerformanceType:
        """
        Get or assign performance type for a specific node.
        
        Uses pre-assigned performance types if available, otherwise falls back to 
        random assignment based on distribution.
        
        Args:
            node_id: Node ID
            
        Returns:
            NodePerformanceType enum value
        """
        if node_id not in self.node_performance_types:
            # Check if we have a pre-assigned performance type
            if node_id in self.node_performance_assignments:
                perf_type_str = self.node_performance_assignments[node_id]
                try:
                    # Convert string to enum
                    perf_type = NodePerformanceType(perf_type_str)
                    self.node_performance_types[node_id] = perf_type
                except ValueError:
                    # Invalid performance type, fall back to average
                    print(f"Warning: Invalid performance type '{perf_type_str}' for node {node_id}, using AVERAGE_PERFORMANCE")
                    self.node_performance_types[node_id] = NodePerformanceType.AVERAGE_PERFORMANCE
            else:
                # Fallback to random assignment based on distribution
                # Use node_id as seed for deterministic assignment
                node_random = random.Random(node_id if self.random_seed is None else self.random_seed + node_id)
                rand = node_random.random()
                cumulative = 0.0
                
                for perf_type, probability in self.default_performance_distribution.items():
                    cumulative += probability
                    if rand <= cumulative:
                        self.node_performance_types[node_id] = perf_type
                        break
                else:
                    # Fallback to average if something goes wrong
                    self.node_performance_types[node_id] = NodePerformanceType.AVERAGE_PERFORMANCE
                
        return self.node_performance_types[node_id]
        
    def get_node_performance_factor(self, node_id: int) -> float:
        """
        Get performance factor for a specific node.
        
        Args:
            node_id: Node ID
            
        Returns:
            Performance factor (1.0 = average, higher = faster)
        """
        if node_id not in self.node_performance_factors:
            # Get base performance type
            perf_type = self.get_node_performance_type(node_id)
            base_factor = self.performance_configs[perf_type]["cpu_factor"]
            
            # Add deterministic random variation based on node_id
            node_random = random.Random(node_id if self.random_seed is None else self.random_seed + node_id + 1000)
            variation = self.node_performance_variation
            factor_variation = node_random.uniform(-variation, variation)
            final_factor = base_factor * (1.0 + factor_variation)
            
            # Ensure minimum performance
            self.node_performance_factors[node_id] = max(0.3, final_factor)
            
        return self.node_performance_factors[node_id]
    
    def set_node_performance_assignments(self, assignments: Dict[int, str]):
        """
        Set performance assignments for nodes (called by simulator).
        
        Args:
            assignments: Dict mapping node_id -> performance_type_string
        """
        self.node_performance_assignments = assignments
        # Clear existing cache to force recalculation
        self.node_performance_types.clear()
        self.node_performance_factors.clear()
    
    def get_base_config(self, node_id: int) -> Dict[str, float]:
        """Get base configuration for a node based on its performance type."""
        perf_type = self.get_node_performance_type(node_id)
        return self.performance_configs[perf_type]
    
    # =============================================================================
    # NEW: HEADER VALIDATION FUNCTION FOR COUGAR PROTOCOL
    # =============================================================================
    
    def calculate_header_validation_delay(self, node_id: int, header_data: Dict[str, Any]) -> float:
        """
        Calculate computational delay for validating a block header.
        
        Header validation involves:
        - Block hash verification
        - Timestamp validation  
        - Previous block hash verification
        - Merkle root validation
        - Proof-of-Work/Proof-of-Stake verification
        - Digital signature verification
        - Block structure validation
        
        Args:
            node_id: Node ID performing the validation
            header_data: Block header dictionary containing metadata
            
        Returns:
            Header validation delay in seconds
        """
        performance_factor = self.get_node_performance_factor(node_id)
        base_config = self.get_base_config(node_id)
        
        # Base header processing time
        processing_time = 0.0
        
        # 1. Block header parsing and deserialization
        header_size = len(json.dumps(header_data).encode('utf-8'))
        processing_time += (header_size / 1024) * self.processing_overheads["json_parsing_per_kb"]
        
        # 2. Hash verification (SHA-256 computation)
        # Headers typically contain previous hash, merkle root hash verification
        hash_operations = 2  # prev_hash + merkle_root verification
        processing_time += hash_operations * base_config["hash_computation"]
        
        # 3. Timestamp validation (lightweight)
        processing_time += self.processing_overheads["timestamp_validation"]
        
        # 4. Digital signature verification (if present)
        # This is the most CPU-intensive part of header validation
        if header_data.get("signature") or header_data.get("validator_signature"):
            # Signature verification is computationally expensive
            processing_time += base_config["signature_verification"]
        
        # 5. Proof-of-Work verification (if applicable)
        if header_data.get("nonce") is not None or header_data.get("difficulty"):
            # PoW verification involves hash difficulty check
            processing_time += base_config["pow_verification"]
        
        # 6. Proof-of-Stake verification (if applicable) 
        if header_data.get("validator_id") or header_data.get("stake_proof"):
            # PoS verification is generally lighter than PoW
            processing_time += base_config["pos_verification"]
        
        # 7. Block structure validation
        # Check required fields, data format, size limits
        processing_time += self.processing_overheads["structure_validation"]
        
        # 8. Consensus rule validation
        # Protocol-specific validation rules
        processing_time += self.processing_overheads["consensus_validation"]
        
        # Apply node performance factor
        final_delay = processing_time / performance_factor
        
        # Ensure realistic header validation time bounds
        # Based on Cougar paper's 5ms header validation and academic literature
        min_header_time = 0.001  # Minimum 1ms
        max_header_time = 0.010  # Maximum 10ms for complex headers
        
        return max(min_header_time, min(final_delay, max_header_time))
    
    # =============================================================================
    # EXISTING FUNCTIONS - ENHANCED
    # =============================================================================
        
    def calculate_block_generation_delay(self, node_id: int, transactions: List[Dict], block_size: int) -> float:
        """
        Calculate delay for generating a block (mining/validator node).
        
        Args:
            node_id: Node ID of the generator
            transactions: List of transaction dictionaries with 'type' field
            block_size: Size of the final block in bytes
            
        Returns:
            Generation delay in seconds
        """
        performance_factor = self.get_node_performance_factor(node_id)
        base_config = self.get_base_config(node_id)
        
        # Base processing time
        processing_time = base_config["base_validation"]
        
        # Transaction processing based on types
        for tx in transactions:
            try:
                tx_type = TxType[tx.get("type", "TOKEN_TRANSFER")]
                processing_time += self.tx_processing_times[tx_type]["total_generation"]
            except (KeyError, TypeError):
                # Fallback to TOKEN_TRANSFER if type is unknown
                processing_time += self.tx_processing_times[TxType.TOKEN_TRANSFER]["total_generation"]
        
        # Merkle tree construction
        processing_time += len(transactions) * self.processing_overheads["merkle_tree_per_tx"]
        
        # Block header generation
        processing_time += self.processing_overheads["block_header_generation"]
        
        # FEC encoding preparation overhead
        processing_time += (block_size / 1024) * base_config["fec_processing"]
        
        # Apply node performance factor
        return processing_time / performance_factor
        
    def calculate_block_processing_delay(self, node_id: int, block_size: int, transactions: List[Dict]) -> float:
        """
        Calculate delay for processing/validating a received block.
        
        Args:
            node_id: Node ID of the processor
            block_size: Size of the block in bytes
            transactions: List of transaction dictionaries with 'type' field
            
        Returns:
            Processing delay in seconds
        """
        performance_factor = self.get_node_performance_factor(node_id)
        base_config = self.get_base_config(node_id)
        
        # Base validation time
        processing_time = base_config["base_validation"]
        
        # Block deserialization
        processing_time += (block_size / 1024) * self.processing_overheads["json_parsing_per_kb"]
        
        # Transaction validation based on types
        for tx in transactions:
            try:
                tx_type = TxType[tx.get("type", "TOKEN_TRANSFER")]
                processing_time += self.tx_processing_times[tx_type]["total_validation"]
            except (KeyError, TypeError):
                # Fallback to TOKEN_TRANSFER if type is unknown
                processing_time += self.tx_processing_times[TxType.TOKEN_TRANSFER]["total_validation"]
        
        # Merkle root verification
        processing_time += len(transactions) * self.processing_overheads["merkle_tree_per_tx"]
        
        # FEC decoding overhead (if applicable)
        processing_time += (block_size / 1024) * base_config["fec_processing"] * 0.6  # Decoding is faster than encoding
        
        # Apply node performance factor
        return processing_time / performance_factor
        
    def calculate_chunk_processing_delay(self, node_id: int, chunk_size: int) -> float:
        """
        Calculate delay for processing a single chunk.
        
        Args:
            node_id: Node ID
            chunk_size: Size of the chunk in bytes
            
        Returns:
            Processing delay in seconds
        """
        performance_factor = self.get_node_performance_factor(node_id)
        
        # Chunk processing is very lightweight
        processing_time = self.processing_overheads["chunk_header_parsing"]  # Header parsing
        processing_time += (chunk_size / 1024) * 0.000005  # Memory storage per KB
        processing_time += 0.00001  # Duplicate detection check
        
        # Apply node performance factor
        return processing_time / performance_factor
        
    def calculate_block_reconstruction_delay(self, node_id: int, chunk_count: int, original_size: int, fec_used: bool = True) -> float:
        """
        Calculate delay for reconstructing a block from chunks.
        
        Args:
            node_id: Node ID
            chunk_count: Number of chunks to process
            original_size: Original block size in bytes
            fec_used: Whether FEC decoding is needed
            
        Returns:
            Reconstruction delay in seconds
        """
        performance_factor = self.get_node_performance_factor(node_id)
        base_config = self.get_base_config(node_id)
        
        # Chunk assembly overhead
        processing_time = chunk_count * self.processing_overheads["chunk_assembly_per_chunk"]
        
        # FEC decoding if used
        if fec_used:
            processing_time += (original_size / 1024) * base_config["fec_processing"]
        
        # Block assembly from chunks
        processing_time += (original_size / 1024) * self.processing_overheads["json_parsing_per_kb"] * 0.5
        
        # Basic integrity check
        processing_time += self.processing_overheads["integrity_check"]
        
        # Apply node performance factor
        return processing_time / performance_factor
        
    def calculate_chunkify_delay(self, node_id: int, block_size: int, chunk_size: int, fec_ratio: float) -> float:
        """
        Calculate delay for splitting a block into chunks with FEC.
        
        Args:
            node_id: Node ID
            block_size: Size of the block in bytes
            chunk_size: Size of individual chunks in bytes
            fec_ratio: FEC overhead ratio (e.g., 0.2 for 20% overhead)
            
        Returns:
            Chunkify delay in seconds
        """
        performance_factor = self.get_node_performance_factor(node_id)
        base_config = self.get_base_config(node_id)
        
        # Block serialization validation (minimal)
        processing_time = (block_size / 1024) * 0.00001
        
        # Chunk count calculation
        processing_time += 0.00002
        
        # FEC encoding overhead
        if fec_ratio > 0:
            processing_time += (block_size / 1024) * base_config["fec_processing"]
        
        # Individual chunk creation
        chunk_count = math.ceil(block_size / chunk_size)
        processing_time += chunk_count * 0.000005
        
        # Apply node performance factor
        return processing_time / performance_factor
        
    def calculate_transaction_validation_delay(self, node_id: int, tx_type: TxType, tx_data: Dict) -> float:
        """
        Calculate delay for validating a single transaction.
        
        Args:
            node_id: Node ID
            tx_type: Type of transaction
            tx_data: Transaction data dictionary
            
        Returns:
            Validation delay in seconds
        """
        performance_factor = self.get_node_performance_factor(node_id)
        
        # Get transaction-specific processing time
        if tx_type in self.tx_processing_times:
            processing_time = self.tx_processing_times[tx_type]["total_validation"]
        else:
            # Fallback to TOKEN_TRANSFER
            processing_time = self.tx_processing_times[TxType.TOKEN_TRANSFER]["total_validation"]
        
        # Apply node performance factor
        return processing_time / performance_factor
    
    # =============================================================================
    # UTILITY AND SUMMARY FUNCTIONS
    # =============================================================================
    
    def get_node_performance_summary(self, node_id: int) -> Dict[str, Any]:
        """
        Get summary of node performance characteristics.
        
        Args:
            node_id: Node ID
            
        Returns:
            Dictionary with performance information
        """
        perf_type = self.get_node_performance_type(node_id)
        perf_factor = self.get_node_performance_factor(node_id)
        config = self.get_base_config(node_id)
        
        return {
            "node_id": node_id,
            "performance_type": perf_type.value,
            "performance_factor": perf_factor,
            "description": config["description"],
            "cpu_factor": config["cpu_factor"],
            "base_validation_time": config["base_validation"],
            "per_tx_validation_time": config["per_tx_validation"],
            "fec_processing_time": config["fec_processing"],
            "signature_verification_time": config["signature_verification"],
            "hash_computation_time": config["hash_computation"],
            "assigned_deterministically": node_id in self.node_performance_assignments
        }
    
    def get_performance_distribution_summary(self) -> Dict[str, Any]:
        """
        Get summary of performance type distribution across all assigned nodes.
        
        Returns:
            Dictionary with distribution statistics
        """
        type_counts = {}
        total_assigned = 0
        
        for node_id, perf_type in self.node_performance_types.items():
            type_name = perf_type.value
            type_counts[type_name] = type_counts.get(type_name, 0) + 1
            total_assigned += 1
        
        return {
            "total_nodes_assigned": total_assigned,
            "distribution_counts": type_counts,
            "distribution_percentages": {
                type_name: (count / total_assigned * 100) if total_assigned > 0 else 0
                for type_name, count in type_counts.items()
            },
            "pre_assigned_nodes": len(self.node_performance_assignments),
            "randomly_assigned_nodes": total_assigned - len(self.node_performance_assignments)
        }
    
    def get_validation_time_estimates(self, node_id: int) -> Dict[str, float]:
        """
        Get estimated validation times for different operations.
        
        Args:
            node_id: Node ID
            
        Returns:
            Dictionary with time estimates in seconds
        """
        base_config = self.get_base_config(node_id)
        performance_factor = self.get_node_performance_factor(node_id)
        
        # Sample header for estimation
        sample_header = {
            "prev_hash": "sample_hash",
            "merkle_root": "sample_merkle", 
            "timestamp": 1234567890,
            "signature": "sample_signature"
        }
        
        return {
            "header_validation": self.calculate_header_validation_delay(node_id, sample_header),
            "single_token_transfer": self.calculate_transaction_validation_delay(
                node_id, TxType.TOKEN_TRANSFER, {}
            ),
            "single_smart_contract": self.calculate_transaction_validation_delay(
                node_id, TxType.SMART_CONTRACT, {}
            ),
            "base_validation_time": base_config["base_validation"] / performance_factor,
            "signature_verification": base_config["signature_verification"] / performance_factor,
            "hash_computation": base_config["hash_computation"] / performance_factor
        }