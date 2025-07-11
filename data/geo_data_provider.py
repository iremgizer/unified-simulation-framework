"""
Geographic data provider for the blockchain simulation.

This module provides classes for loading and managing geographic data
for node distribution and RTT-based latency calculations.
"""

import json
import random
import math
import os
from typing import Dict, List, Tuple, Any, Optional

class GeoDataProvider:
    """
    Provides geographic data for node distribution and RTT calculations.
    
    Loads data from JSON files and provides methods for region/city assignment
    and RTT lookups.
    """
    
    def __init__(self, data_dir: str):
        """
        Initialize the geographic data provider.
        
        Args:
            data_dir: Directory containing geographic data files
        """
        self.data_dir = data_dir
        self.region_weights = {}
        self.city_regions = {}
        self.city_coordinates = {}
        self.rtt_matrix = {}
        
        # Load all data files (required)
        self._load_region_weights()
        self._load_city_regions()
        self._load_city_coordinates()
        self._load_rtt_matrix()
        
        # Validate data consistency
        self._validate_data()
        
    def _load_region_weights(self):
        """Load region weights from JSON file."""
        file_path = os.path.join(self.data_dir, "region_weights.json")
        try:
            with open(file_path, 'r') as f:
                self.region_weights = json.load(f)
            print(f"Loaded region weights from {file_path}")
        except Exception as e:
            raise FileNotFoundError(f"Required file not found: {file_path}. Error: {e}")
    
    def _load_city_regions(self):
        """Load city to region mappings from JSON file."""
        file_path = os.path.join(self.data_dir, "city_regions.json")
        try:
            with open(file_path, 'r') as f:
                self.city_regions = json.load(f)
            print(f"Loaded city regions from {file_path}")
        except Exception as e:
            raise FileNotFoundError(f"Required file not found: {file_path}. Error: {e}")
    
    def _load_city_coordinates(self):
        """Load city coordinates from JSON file."""
        file_path = os.path.join(self.data_dir, "city_coordinates.json")
        try:
            with open(file_path, 'r') as f:
                self.city_coordinates = json.load(f)
            print(f"Loaded city coordinates from {file_path}")
        except Exception as e:
            raise FileNotFoundError(f"Required file not found: {file_path}. Error: {e}")
    
    def _load_rtt_matrix(self):
        """Load RTT matrix from JSON file."""
        file_path = os.path.join(self.data_dir, "rtt_matrix.json")
        try:
            with open(file_path, 'r') as f:
                self.rtt_matrix = json.load(f)
            print(f"Loaded RTT matrix from {file_path}")
        except Exception as e:
            raise FileNotFoundError(f"Required file not found: {file_path}. Error: {e}")
    
    def _validate_data(self):
        """Validate consistency between data files."""
        # Get all cities from city_regions
        all_cities = set()
        for cities in self.city_regions.values():
            all_cities.update(cities)
        
        # Check if all cities have coordinates
        missing_coords = all_cities - set(self.city_coordinates.keys())
        if missing_coords:
            raise ValueError(f"Cities missing coordinates: {missing_coords}")
        
        # Check if all cities have RTT data
        missing_rtt = all_cities - set(self.rtt_matrix.keys())
        if missing_rtt:
            print(f"Warning: Cities missing RTT data: {missing_rtt}")
        
        print(f"Data validation complete: {len(all_cities)} cities across {len(self.city_regions)} regions")
    
    def get_rtt(self, city1: str, city2: str) -> float:
        """
        Get RTT between two cities.
        
        Args:
            city1: First city name
            city2: Second city name
            
        Returns:
            RTT in milliseconds
        """
        if city1 in self.rtt_matrix and city2 in self.rtt_matrix[city1]:
            return self.rtt_matrix[city1][city2]
        elif city2 in self.rtt_matrix and city1 in self.rtt_matrix[city2]:
            return self.rtt_matrix[city2][city1]
        else:
            # Fallback to distance-based approximation if RTT data missing
            coords1 = self.city_coordinates.get(city1)
            coords2 = self.city_coordinates.get(city2)
            
            if coords1 and coords2:
                distance = self._calculate_distance(coords1, coords2)
                # Approximate RTT as 0.1ms per km plus a base latency
                return distance * 0.1 + 10.0
            else:
                # Default RTT if cities not found
                print(f"Warning: RTT not found for {city1} -> {city2}, using default 100ms")
                return 100.0
    
    def _calculate_distance(self, coords1: List[float], coords2: List[float]) -> float:
        """
        Calculate distance between two coordinates using Haversine formula.
        
        Args:
            coords1: [latitude, longitude] of first point
            coords2: [latitude, longitude] of second point
            
        Returns:
            Distance in kilometers
        """
        lat1, lon1 = coords1
        lat2, lon2 = coords2
        
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371  # Radius of Earth in kilometers
        
        return c * r
    
    def assign_nodes_to_regions_and_cities(self, node_count: int) -> Dict[int, Dict[str, Any]]:
        """
        Assign nodes to regions and cities based on region weights.
        
        Args:
            node_count: Number of nodes to assign
            
        Returns:
            Dictionary mapping node indices to region and city information
            {node_index: {"region": region_name, "city": city_name, "coordinates": [lat, lon]}}
        """
        print(f"Assigning {node_count} nodes to regions and cities")
        
        # Calculate number of nodes per region based on weights
        total_weight = sum(self.region_weights.values())
        region_nodes = {}
        remaining = node_count
        
        for region, weight in self.region_weights.items():
            count = int(node_count * (weight / total_weight))
            region_nodes[region] = count
            remaining -= count
        
        # Distribute remaining nodes to regions with highest weights
        sorted_regions = sorted(self.region_weights.items(), key=lambda x: x[1], reverse=True)
        for i in range(remaining):
            region = sorted_regions[i % len(sorted_regions)][0]
            region_nodes[region] += 1
        
        print(f"Region distribution: {region_nodes}")
        
        # Assign nodes to cities within each region
        node_assignments = {}
        node_index = 0
        
        for region, count in region_nodes.items():
            # Get cities in this region
            cities = self.city_regions.get(region, [])
            if not cities:
                raise ValueError(f"No cities found for region {region}")
                
            print(f"Assigning {count} nodes to {len(cities)} cities in {region}")
            
            # Distribute nodes among cities in the region
            for i in range(count):
                if node_index >= node_count:
                    break
                    
                # Select city (round-robin distribution)
                city = random.choice(cities)
                
                # Get coordinates with small random variation
                base_coords = self.city_coordinates.get(city)
                if not base_coords:
                    raise ValueError(f"No coordinates found for city {city}")
                
                lat, lon = base_coords
                # Add small random variation (±0.1 degrees)
                lat_variation = random.uniform(-0.1, 0.1)
                lon_variation = random.uniform(-0.1, 0.1)
                coordinates = [lat + lat_variation, lon + lon_variation]
                
                node_assignments[node_index] = {
                    "region": region,
                    "city": city,
                    "coordinates": coordinates
                }
                
                print(f"Node {node_index} assigned to {city}, {region} at {coordinates}")
                node_index += 1
        
        print(f"Successfully assigned {len(node_assignments)} nodes")
        return node_assignments
    
    def get_total_available_cities(self) -> int:
        """
        Get total number of available cities across all regions.
        
        Returns:
            Total number of cities
        """
        total_cities = 0
        for cities in self.city_regions.values():
            total_cities += len(cities)
        return total_cities
    
    def get_region_info(self) -> Dict[str, Any]:
        """
        Get information about regions and cities.
        
        Returns:
            Dictionary with region information
        """
        info = {
            "total_regions": len(self.city_regions),
            "total_cities": self.get_total_available_cities(),
            "regions": {}
        }
        
        for region, cities in self.city_regions.items():
            info["regions"][region] = {
                "weight": self.region_weights.get(region, 0),
                "city_count": len(cities),
                "cities": cities
            }
        
        return info

