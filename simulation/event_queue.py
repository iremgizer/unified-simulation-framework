"""
Event-driven simulation engine for blockchain protocol simulation.

This module provides the core event queue system for discrete event simulation.

ENHANCED: Added Cougar and Mercury event types while keeping the original architecture.
"""

import heapq
import time
import logging
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger("EventQueue")

class EventType(Enum):
    """Types of events in the simulation - ENHANCED with new protocol events."""
    
    # =============================================================================
    # EXISTING EVENTS (unchanged)
    # =============================================================================
    # Network events
    MESSAGE = "MESSAGE"
    CHUNK_RECEIVE = "CHUNK_RECEIVE"
    BLOCK_PROCESSED = "BLOCK_PROCESSED"
    MESSAGE_SEND = "MESSAGE_SEND" 
    
    # Protocol events
    PING = "PING"
    PONG = "PONG"
    FIND_NODE = "FIND_NODE"
    NODES_RESPONSE = "NODES_RESPONSE"
    CHUNK_SEND = "CHUNK_SEND"
    CHUNK_REQUEST = "REQ"
    
    # Simulation events
    BLOCK_GENERATE = "BLOCK_GENERATE"
    PEER_DISCOVERY = "PEER_DISCOVERY"
    BOOTSTRAP_COMPLETE = "BOOTSTRAP_COMPLETE"
    SIMULATION_START = "SIMULATION_START"
    SIMULATION_END = "SIMULATION_END"
    
    # Timeout events
    TIMEOUT = "TIMEOUT"
    
    # =============================================================================
    # NEW COUGAR PROTOCOL EVENTS
    # =============================================================================
    COUGAR_MESSAGE_RECEIVED = "COUGAR_MESSAGE_RECEIVED"
    COUGAR_BLOCK_GENERATE = "COUGAR_BLOCK_GENERATE"
    COUGAR_BLOCK_GENERATED = "COUGAR_BLOCK_GENERATED"
    COUGAR_BLOCK_SEND = "COUGAR_BLOCK_SEND"
    COUGAR_HEADER_VALIDATED = "COUGAR_HEADER_VALIDATED"
    COUGAR_BODY_VALIDATED = "COUGAR_BODY_VALIDATED"
    COUGAR_BODY_REQUEST_PROCESSED = "COUGAR_BODY_REQUEST_PROCESSED"
    COUGAR_BODY_REQUEST_TIMEOUT = "COUGAR_BODY_REQUEST_TIMEOUT"
    COUGAR_BLOCK_COMPLETED = "COUGAR_BLOCK_COMPLETED"
    

    # ChunkedKad Core Events
    CHUNKEDKAD_CLUSTER_ASSIGNMENT = "CHUNKEDKAD_CLUSTER_ASSIGNMENT"
    CHUNKEDKAD_PROACTIVE_EXCHANGE = "CHUNKEDKAD_PROACTIVE_EXCHANGE"
    CHUNKEDKAD_EXCHANGE_RESPONSE = "CHUNKEDKAD_EXCHANGE_RESPONSE"
    CHUNKEDKAD_BLOCK_HEADER = "CHUNKEDKAD_BLOCK_HEADER"
    CHUNKEDKAD_CHUNK_FORWARD = "CHUNKEDKAD_CHUNK_FORWARD"
    
   
    DELAYED_CLUSTER_ASSIGNMENT = "DELAYED_CLUSTER_ASSIGNMENT"
    DELAYED_EXCHANGE_INITIATION = "DELAYED_EXCHANGE_INITIATION"
    CASCADE_SEEDING_PHASE_1 = "CASCADE_SEEDING_PHASE_1"
    CASCADE_SEEDING_PHASE_2 = "CASCADE_SEEDING_PHASE_2"
    CASCADE_SEEDING_PHASE_3 = "CASCADE_SEEDING_PHASE_3"
    CASCADE_SEEDING_PHASE_4 = "CASCADE_SEEDING_PHASE_4"
    
    # ChunkedKad Optimization Events
    CHUNK_AVAILABILITY_VERIFICATION = "CHUNK_AVAILABILITY_VERIFICATION"
    EXCHANGE_RETRY_ATTEMPT = "EXCHANGE_RETRY_ATTEMPT"
    EMERGENCY_COVERAGE_BOOST = "EMERGENCY_COVERAGE_BOOST"
    ADAPTIVE_TERMINATION_CHECK = "ADAPTIVE_TERMINATION_CHECK"
    
    # Memory and Performance Events
    CHUNKEDKAD_MEMORY_CLEANUP = "CHUNKEDKAD_MEMORY_CLEANUP"
    CHUNKEDKAD_PERFORMANCE_UPDATE = "CHUNKEDKAD_PERFORMANCE_UPDATE"
    CHUNKEDKAD_METRICS_COLLECTION = "CHUNKEDKAD_METRICS_COLLECTION"
    
    # =============================================================================
    # NEW MERCURY PROTOCOL EVENTS (Future expansion)
    # =============================================================================
    # Mercury Protocol Events (COMPLETE SET)
    MERCURY_VIVALDI_UPDATE = "MERCURY_VIVALDI_UPDATE"
    MERCURY_VIVALDI_RESPONSE = "MERCURY_VIVALDI_RESPONSE"
    MERCURY_BLOCK_DIGEST = "MERCURY_BLOCK_DIGEST"
    MERCURY_CHUNK_REQUEST = "MERCURY_CHUNK_REQUEST"
    MERCURY_CHUNK_RESPONSE = "MERCURY_CHUNK_RESPONSE"
    MERCURY_BLOCK_COMPLETE = "MERCURY_BLOCK_COMPLETE"
    MERCURY_BLOCK_GENERATE = "MERCURY_BLOCK_GENERATE"
    MERCURY_VCS_CONVERGENCE = "MERCURY_VCS_CONVERGENCE"
    MERCURY_CLUSTERING = "MERCURY_CLUSTERING"
    MERCURY_MESSAGE_RECEIVED = "MERCURY_MESSAGE_RECEIVED"
    
    # =============================================================================
    # LEGACY EVENTS (For backward compatibility)
    # =============================================================================
    # Keep old event names for compatibility
    CHUNK_FORWARD = "CHUNK_FORWARD"
    BLOCK_PROCESSING = "BLOCK_PROCESSING"

@dataclass
class Event:
    """Represents a discrete event in the simulation."""
    timestamp: float
    event_type: EventType
    node_id: int
    data: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0  # Lower values = higher priority
    
    def __lt__(self, other):
        """Comparison for priority queue (heapq)."""
        if self.timestamp != other.timestamp:
            return self.timestamp < other.timestamp
        return self.priority < other.priority
    
    def __str__(self):
        return f"Event({self.event_type.value}, t={self.timestamp:.3f}, node={self.node_id})"

class EventQueue:
    """
    Priority queue for managing simulation events.
    
    Uses heapq for efficient event scheduling and processing.
    """
    
    def __init__(self):
        """Initialize the event queue."""
        self.events = []  # Priority queue (heapq)
        self.current_time = 0.0
        self.event_count = 0
        self.processed_events = 0
        
    def push(self, event: Event):
        """
        Add an event to the queue.
        
        Args:
            event: Event to add
        """
        heapq.heappush(self.events, event)
        self.event_count += 1
        #logger.debug(f"Scheduled event: {event}")
    
    def pop(self) -> Optional[Event]:
        """
        Remove and return the next event from the queue.
        
        Returns:
            Next event or None if queue is empty
        """
        if not self.events:
            return None
        
        event = heapq.heappop(self.events)
        
        # CRITICAL: Check if current_time is advancing
        if event.timestamp < self.current_time:
            logger.error(f"TIME_TRAVEL_ERROR: Event timestamp {event.timestamp:.6f} is less than current_time {self.current_time:.6f}. This should not happen in a discrete event simulation.")
            # This indicates a serious logic error in event scheduling.

        self.current_time = event.timestamp
        self.processed_events += 1
        
        return event
    
    def peek(self) -> Optional[Event]:
        """
        Look at the next event without removing it.
        
        Returns:
            Next event or None if queue is empty
        """
        if not self.events:
            return None
        return self.events[0]
    
    def is_empty(self) -> bool:
        """Check if the queue is empty."""
        return len(self.events) == 0
    
    def size(self) -> int:
        """Get the number of events in the queue."""
        return len(self.events)
    
    def clear(self):
        """Clear all events from the queue."""
        self.events.clear()
        self.event_count = 0
        self.processed_events = 0
        self.current_time = 0.0
    
    def schedule_event(self, event_type: EventType, node_id: int, delay: float, 
                      data: Dict[str, Any] = None, priority: int = 0):
        """
        Schedule an event with a delay from current time.
        
        Args:
            event_type: Type of event
            node_id: Target node ID
            delay: Delay from current time
            data: Event data
            priority: Event priority (lower = higher priority)
        """
        if delay is None:
            logger.error(f"[EVENT ERROR] Cannot schedule event with None delay for node {node_id}")
            return  
        
        # CRITICAL: Ensure delay is non-negative. If it's negative, it can cause time travel.
        if delay < 0:
            logger.error(f"[EVENT ERROR] Scheduling event with negative delay ({delay:.6f}) for node {node_id}. Forcing to 0.")
            delay = 0.0 # Force non-negative delay
        
        event = Event(
            timestamp=self.current_time + delay,
            event_type=event_type,
            node_id=node_id,
            data=data or {},
            priority=priority
        )
        self.push(event)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get queue statistics.
        
        Returns:
            Dictionary with queue statistics
        """
        return {
            "current_time": self.current_time,
            "events_in_queue": len(self.events),
            "total_events_scheduled": self.event_count,
            "events_processed": self.processed_events,
            "next_event_time": self.events[0].timestamp if self.events else None
        }
    
    def remove_events_by_condition(self, condition_func):
        """
        Remove events that match the given condition.
         Single-pass filtering instead of multiple remove() calls
        
        Args:
            condition_func: Function that takes an event and returns True if it should be removed
            
        Returns:
            Number of events removed
        """
        original_count = len(self.events)
        
        if original_count == 0:
            return 0
        
        #  PERFORMANCE FIX: Single-pass filtering instead of O(n²) removal
        # Original: for each event -> self.events.remove() -> O(n) operation = O(n²) total
        # Optimized: Single list comprehension -> O(n) operation
        self.events = [event for event in self.events if not condition_func(event)]
        
        #  PERFORMANCE FIX: Single heapify instead of multiple heapify calls
        # Re-heapify once after all removals
        heapq.heapify(self.events)
        
        removed_count = original_count - len(self.events)
        
        if removed_count > 0:
            logger.info(f" Removed {removed_count} events in single pass "
                    f"(queue size: {original_count} → {len(self.events)})")
        else:
            logger.debug(f"No events matched removal condition")
        
        return removed_count



class SimulationEngine:
    """
    Main simulation engine that processes events.
    """
    
    def __init__(self, event_queue: EventQueue):
        """Initialize the simulation engine."""
        self.event_queue = event_queue
        self.event_handlers = {}  # event_type -> handler_function
        self.running = False
        self.max_time = float('inf')
        self.max_events = float('inf')
        
        # Metrics
        self.start_time = None
        self.end_time = None
        
    def register_handler(self, event_type: EventType, handler: Callable):
        """
        Register an event handler.
        
        Args:
            event_type: Type of event to handle
            handler: Handler function
        """
        self.event_handlers[event_type] = handler
        logger.debug(f"Registered handler for {event_type.value}")
    
    def schedule_event(self, event_type: EventType, node_id: int, delay: float,
                      data: Dict[str, Any] = None, priority: int = 0):
        """
        Schedule an event.
        
        Args:
            event_type: Type of event
            node_id: Target node ID
            delay: Delay from current time
            data: Event data
            priority: Event priority
        """
        self.event_queue.schedule_event(event_type, node_id, delay, data, priority)
    
    def run(self, max_time: float = None, max_events: int = None):
        """
        Run the simulation.
        
        Args:
            max_time: Maximum simulation time
            max_events: Maximum number of events to process
        """
        self.running = True
        self.start_time = time.time()
        self.max_time = max_time or float('inf')
        self.max_events = max_events or float('inf')
        
        logger.info(f"Starting simulation (max_time={self.max_time}, max_events={self.max_events})")
        
        try:
            # Log loop conditions at every iteration for detailed debugging
            while (self.running and 
                   not self.event_queue.is_empty() and
                   self.event_queue.current_time < self.max_time and
                   self.event_queue.processed_events < self.max_events):
                
                # Log current time and next event time at each iteration
                next_event = self.event_queue.peek()
                next_event_time = next_event.timestamp if next_event else float('inf')
                
                event = self.event_queue.pop()
                if event is None:
                    logger.warning("Event queue returned None, breaking loop.")
                    break
                
                self._process_event(event)
                
                # Log progress periodically
                if self.event_queue.processed_events % 10000 == 0:
                    stats = self.event_queue.get_stats()
                    logger.info(f"Processed {stats['events_processed']} events, "
                              f"time={stats['current_time']:.3f}, "
                              f"queue_size={stats['events_in_queue']}")
        
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user")
        except Exception as e:
            logger.error(f"Simulation error: {e}")
            raise
        finally:
            self.running = False
            self.end_time = time.time()
            
        # Final statistics
        stats = self.get_simulation_stats()
        logger.info(f"Simulation completed: {stats}")
    
    def stop(self):
        """Stop the simulation."""
        self.running = False 
        logger.info("Simulation stop requested")
    
    def _process_event(self, event: Event):
        """
        Process a single event.
        
        Args:
            event: Event to process
        """
        #logger.debug(f"Processing event: {event}")
        handler = self.event_handlers.get(event.event_type)
        if handler:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Error processing event {event}: {e}", exc_info=True) # Added exc_info for full traceback
        else:
            logger.warning(f"No handler for event type: {event.event_type.value}")

    def get_simulation_stats(self) -> Dict[str, Any]:
        """
        Get simulation statistics.
        
        Returns:
            Dictionary with simulation statistics
        """
        queue_stats = self.event_queue.get_stats()
        
        stats = {
            "simulation_time": queue_stats["current_time"],
            "real_time": (self.end_time - self.start_time) if self.end_time else None,
            "events_processed": queue_stats["events_processed"],
            "events_remaining": queue_stats["events_in_queue"],
            "running": self.running
        }
        
        if stats["real_time"]:
            stats["events_per_second"] = stats["events_processed"] / stats["real_time"]
        
        return stats
