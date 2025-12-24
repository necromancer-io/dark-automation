#!/usr/bin/env python3
"""
Quantum Computing Simulator - Enterprise Quantum Algorithm Development Platform
Advanced quantum computing simulation and algorithm development environment.

Use of this code is at your own risk.
Author bears no responsibility for any damages caused by the code.
"""

import os
import sys
import json
import time
import cmath
import asyncio
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple, Complex
from dataclasses import dataclass, asdict, field
from enum import Enum
import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
import threading
import math

class GateType(Enum):
    """Quantum gate types."""
    PAULI_X = "pauli_x"
    PAULI_Y = "pauli_y"
    PAULI_Z = "pauli_z"
    HADAMARD = "hadamard"
    PHASE = "phase"
    ROTATION_X = "rotation_x"
    ROTATION_Y = "rotation_y"
    ROTATION_Z = "rotation_z"
    CNOT = "cnot"
    TOFFOLI = "toffoli"
    SWAP = "swap"
    CONTROLLED_Z = "controlled_z"
    MEASUREMENT = "measurement"

class QuantumAlgorithm(Enum):
    """Quantum algorithms."""
    GROVER = "grover"
    SHOR = "shor"
    DEUTSCH_JOZSA = "deutsch_jozsa"
    BERNSTEIN_VAZIRANI = "bernstein_vazirani"
    QUANTUM_FOURIER_TRANSFORM = "qft"
    VARIATIONAL_QUANTUM_EIGENSOLVER = "vqe"
    QUANTUM_APPROXIMATE_OPTIMIZATION = "qaoa"
    QUANTUM_TELEPORTATION = "teleportation"

@dataclass
class QuantumGate:
    """Quantum gate definition."""
    gate_type: GateType
    target_qubits: List[int]
    control_qubits: List[int] = field(default_factory=list)
    parameters: Dict[str, float] = field(default_factory=dict)
    matrix: Optional[np.ndarray] = None

@dataclass
class QuantumCircuit:
    """Quantum circuit definition."""
    circuit_id: str
    name: str
    num_qubits: int
    gates: List[QuantumGate] = field(default_factory=list)
    measurements: List[int] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    description: str = ""

@dataclass
class QuantumState:
    """Quantum state representation."""
    amplitudes: np.ndarray
    num_qubits: int
    basis_states: List[str] = field(default_factory=list)
    probabilities: Optional[np.ndarray] = None

@dataclass
class SimulationResult:
    """Quantum simulation result."""
    circuit_id: str
    final_state: QuantumState
    measurement_results: Dict[str, int]
    execution_time_ms: float
    shots: int
    fidelity: float = 1.0
    entanglement_entropy: float = 0.0
    gate_count: int = 0

class QuantumGateLibrary:
    """Library of quantum gates and their matrices."""
    
    @staticmethod
    def pauli_x() -> np.ndarray:
        """Pauli-X (NOT) gate."""
        return np.array([[0, 1], [1, 0]], dtype=complex)
    
    @staticmethod
    def pauli_y() -> np.ndarray:
        """Pauli-Y gate."""
        return np.array([[0, -1j], [1j, 0]], dtype=complex)
    
    @staticmethod
    def pauli_z() -> np.ndarray:
        """Pauli-Z gate."""
        return np.array([[1, 0], [0, -1]], dtype=complex)
    
    @staticmethod
    def hadamard() -> np.ndarray:
        """Hadamard gate."""
        return np.array([[1, 1], [1, -1]], dtype=complex) / np.sqrt(2)
    
    @staticmethod
    def phase(phi: float) -> np.ndarray:
        """Phase gate with angle phi."""
        return np.array([[1, 0], [0, np.exp(1j * phi)]], dtype=complex)
    
    @staticmethod
    def rotation_x(theta: float) -> np.ndarray:
        """Rotation around X-axis."""
        cos_half = np.cos(theta / 2)
        sin_half = np.sin(theta / 2)
        return np.array([
            [cos_half, -1j * sin_half],
            [-1j * sin_half, cos_half]
        ], dtype=complex)
    
    @staticmethod
    def rotation_y(theta: float) -> np.ndarray:
        """Rotation around Y-axis."""
        cos_half = np.cos(theta / 2)
        sin_half = np.sin(theta / 2)
        return np.array([
            [cos_half, -sin_half],
            [sin_half, cos_half]
        ], dtype=complex)
    
    @staticmethod
    def rotation_z(phi: float) -> np.ndarray:
        """Rotation around Z-axis."""
        return np.array([
            [np.exp(-1j * phi / 2), 0],
            [0, np.exp(1j * phi / 2)]
        ], dtype=complex)
    
    @staticmethod
    def cnot() -> np.ndarray:
        """Controlled-NOT gate."""
        return np.array([
            [1, 0, 0, 0],
            [0, 1, 0, 0],
            [0, 0, 0, 1],
            [0, 0, 1, 0]
        ], dtype=complex)
    
    @staticmethod
    def toffoli() -> np.ndarray:
        """Toffoli (CCX) gate."""
        matrix = np.eye(8, dtype=complex)
        matrix[6, 6] = 0
        matrix[7, 7] = 0
        matrix[6, 7] = 1
        matrix[7, 6] = 1
        return matrix
    
    @staticmethod
    def swap() -> np.ndarray:
        """SWAP gate."""
        return np.array([
            [1, 0, 0, 0],
            [0, 0, 1, 0],
            [0, 1, 0, 0],
            [0, 0, 0, 1]
        ], dtype=complex)
    
    @staticmethod
    def controlled_z() -> np.ndarray:
        """Controlled-Z gate."""
        return np.array([
            [1, 0, 0, 0],
            [0, 1, 0, 0],
            [0, 0, 1, 0],
            [0, 0, 0, -1]
        ], dtype=complex)

class QuantumSimulator:
    """Quantum circuit simulator."""
    
    def __init__(self, max_qubits: int = 20):
        self.max_qubits = max_qubits
        self.gate_library = QuantumGateLibrary()
        self.logger = logging.getLogger('QuantumSimulator')
        
        # Check memory requirements
        max_states = 2 ** max_qubits
        memory_gb = max_states * 16 / (1024**3)  # Complex128 = 16 bytes
        
        if memory_gb > 8:  # Limit to 8GB
            self.max_qubits = int(np.log2(8 * 1024**3 / 16))
            self.logger.warning(f"Reduced max qubits to {self.max_qubits} due to memory constraints")
    
    def create_circuit(self, num_qubits: int, name: str = "", description: str = "") -> QuantumCircuit:
        """Create new quantum circuit."""
        if num_qubits > self.max_qubits:
            raise ValueError(f"Number of qubits ({num_qubits}) exceeds maximum ({self.max_qubits})")
        
        circuit_id = str(uuid.uuid4())
        
        circuit = QuantumCircuit(
            circuit_id=circuit_id,
            name=name or f"Circuit_{circuit_id[:8]}",
            num_qubits=num_qubits,
            description=description
        )
        
        self.logger.info(f"Created quantum circuit with {num_qubits} qubits")
        
        return circuit
    
    def add_gate(self, circuit: QuantumCircuit, gate_type: GateType, 
                target_qubits: List[int], control_qubits: List[int] = None,
                parameters: Dict[str, float] = None) -> QuantumCircuit:
        """Add gate to quantum circuit."""
        if control_qubits is None:
            control_qubits = []
        if parameters is None:
            parameters = {}
        
        # Validate qubit indices
        all_qubits = target_qubits + control_qubits
        if any(q >= circuit.num_qubits or q < 0 for q in all_qubits):
            raise ValueError("Invalid qubit index")
        
        # Get gate matrix
        gate_matrix = self._get_gate_matrix(gate_type, parameters)
        
        gate = QuantumGate(
            gate_type=gate_type,
            target_qubits=target_qubits,
            control_qubits=control_qubits,
            parameters=parameters,
            matrix=gate_matrix
        )
        
        circuit.gates.append(gate)
        
        return circuit
    
    def _get_gate_matrix(self, gate_type: GateType, parameters: Dict[str, float]) -> np.ndarray:
        """Get matrix representation of quantum gate."""
        if gate_type == GateType.PAULI_X:
            return self.gate_library.pauli_x()
        elif gate_type == GateType.PAULI_Y:
            return self.gate_library.pauli_y()
        elif gate_type == GateType.PAULI_Z:
            return self.gate_library.pauli_z()
        elif gate_type == GateType.HADAMARD:
            return self.gate_library.hadamard()
        elif gate_type == GateType.PHASE:
            phi = parameters.get('phi', 0.0)
            return self.gate_library.phase(phi)
        elif gate_type == GateType.ROTATION_X:
            theta = parameters.get('theta', 0.0)
            return self.gate_library.rotation_x(theta)
        elif gate_type == GateType.ROTATION_Y:
            theta = parameters.get('theta', 0.0)
            return self.gate_library.rotation_y(theta)
        elif gate_type == GateType.ROTATION_Z:
            phi = parameters.get('phi', 0.0)
            return self.gate_library.rotation_z(phi)
        elif gate_type == GateType.CNOT:
            return self.gate_library.cnot()
        elif gate_type == GateType.TOFFOLI:
            return self.gate_library.toffoli()
        elif gate_type == GateType.SWAP:
            return self.gate_library.swap()
        elif gate_type == GateType.CONTROLLED_Z:
            return self.gate_library.controlled_z()
        else:
            raise ValueError(f"Unknown gate type: {gate_type}")
    
    def initialize_state(self, num_qubits: int, initial_state: str = None) -> QuantumState:
        """Initialize quantum state."""
        num_states = 2 ** num_qubits
        amplitudes = np.zeros(num_states, dtype=complex)
        
        if initial_state is None:
            # Initialize to |0...0⟩
            amplitudes[0] = 1.0
        else:
            # Parse initial state string (e.g., "101" for |101⟩)
            if len(initial_state) != num_qubits:
                raise ValueError("Initial state length must match number of qubits")
            
            state_index = int(initial_state, 2)
            amplitudes[state_index] = 1.0
        
        # Generate basis state labels
        basis_states = [format(i, f'0{num_qubits}b') for i in range(num_states)]
        
        return QuantumState(
            amplitudes=amplitudes,
            num_qubits=num_qubits,
            basis_states=basis_states
        )
    
    def apply_gate(self, state: QuantumState, gate: QuantumGate) -> QuantumState:
        """Apply quantum gate to state."""
        new_amplitudes = state.amplitudes.copy()
        
        if len(gate.control_qubits) == 0:
            # Single-qubit gate or multi-qubit gate without controls
            new_amplitudes = self._apply_uncontrolled_gate(new_amplitudes, gate, state.num_qubits)
        else:
            # Controlled gate
            new_amplitudes = self._apply_controlled_gate(new_amplitudes, gate, state.num_qubits)
        
        return QuantumState(
            amplitudes=new_amplitudes,
            num_qubits=state.num_qubits,
            basis_states=state.basis_states
        )
    
    def _apply_uncontrolled_gate(self, amplitudes: np.ndarray, gate: QuantumGate, num_qubits: int) -> np.ndarray:
        """Apply uncontrolled gate to state amplitudes."""
        if len(gate.target_qubits) == 1:
            # Single-qubit gate
            target = gate.target_qubits[0]
            return self._apply_single_qubit_gate(amplitudes, gate.matrix, target, num_qubits)
        
        elif len(gate.target_qubits) == 2:
            # Two-qubit gate
            target1, target2 = gate.target_qubits
            return self._apply_two_qubit_gate(amplitudes, gate.matrix, target1, target2, num_qubits)
        
        else:
            raise NotImplementedError("Multi-qubit gates with >2 qubits not implemented")
    
    def _apply_controlled_gate(self, amplitudes: np.ndarray, gate: QuantumGate, num_qubits: int) -> np.ndarray:
        """Apply controlled gate to state amplitudes."""
        new_amplitudes = amplitudes.copy()
        
        # For each basis state, check if control qubits are in |1⟩ state
        for i, amplitude in enumerate(amplitudes):
            if amplitude == 0:
                continue
            
            # Convert index to binary representation
            binary_state = format(i, f'0{num_qubits}b')
            
            # Check control qubits
            controls_active = all(
                binary_state[num_qubits - 1 - ctrl] == '1' 
                for ctrl in gate.control_qubits
            )
            
            if controls_active:
                # Apply gate to target qubits
                if len(gate.target_qubits) == 1:
                    target = gate.target_qubits[0]
                    
                    # Find the state with target qubit flipped
                    target_bit = num_qubits - 1 - target
                    j = i ^ (1 << target_bit)  # Flip target bit
                    
                    # Apply 2x2 gate matrix
                    if binary_state[target_bit] == '0':
                        # |0⟩ component
                        new_val = gate.matrix[0, 0] * amplitudes[i] + gate.matrix[0, 1] * amplitudes[j]
                        new_amplitudes[i] = new_val
                    else:
                        # |1⟩ component
                        new_val = gate.matrix[1, 0] * amplitudes[j] + gate.matrix[1, 1] * amplitudes[i]
                        new_amplitudes[i] = new_val
        
        return new_amplitudes
    
    def _apply_single_qubit_gate(self, amplitudes: np.ndarray, gate_matrix: np.ndarray, 
                                target: int, num_qubits: int) -> np.ndarray:
        """Apply single-qubit gate."""
        new_amplitudes = amplitudes.copy()
        
        # Iterate through all basis states
        for i in range(len(amplitudes)):
            if amplitudes[i] == 0:
                continue
            
            # Check target qubit state
            target_bit = num_qubits - 1 - target
            
            if (i >> target_bit) & 1 == 0:
                # Target qubit is |0⟩
                j = i | (1 << target_bit)  # Corresponding |1⟩ state
                
                # Apply gate matrix
                new_amplitudes[i] = gate_matrix[0, 0] * amplitudes[i] + gate_matrix[0, 1] * amplitudes[j]
                new_amplitudes[j] = gate_matrix[1, 0] * amplitudes[i] + gate_matrix[1, 1] * amplitudes[j]
        
        return new_amplitudes
    
    def _apply_two_qubit_gate(self, amplitudes: np.ndarray, gate_matrix: np.ndarray,
                             target1: int, target2: int, num_qubits: int) -> np.ndarray:
        """Apply two-qubit gate."""
        new_amplitudes = np.zeros_like(amplitudes)
        
        # Create mapping for two-qubit subspace
        target1_bit = num_qubits - 1 - target1
        target2_bit = num_qubits - 1 - target2
        
        # Group states by non-target qubits
        processed = set()
        
        for i in range(len(amplitudes)):
            if i in processed:
                continue
            
            # Get the four states in the two-qubit subspace
            base = i & ~((1 << target1_bit) | (1 << target2_bit))
            
            states = [
                base,  # |00⟩
                base | (1 << target2_bit),  # |01⟩
                base | (1 << target1_bit),  # |10⟩
                base | (1 << target1_bit) | (1 << target2_bit)  # |11⟩
            ]
            
            # Get current amplitudes
            current_amps = [amplitudes[s] for s in states]
            
            # Apply gate matrix
            new_amps = gate_matrix @ np.array(current_amps)
            
            # Update amplitudes
            for j, state in enumerate(states):
                new_amplitudes[state] = new_amps[j]
                processed.add(state)
        
        return new_amplitudes
    
    def measure(self, state: QuantumState, qubits: List[int] = None, shots: int = 1000) -> Dict[str, int]:
        """Measure quantum state."""
        if qubits is None:
            qubits = list(range(state.num_qubits))
        
        # Calculate probabilities
        probabilities = np.abs(state.amplitudes) ** 2
        
        # Perform measurements
        measurement_results = {}
        
        for _ in range(shots):
            # Sample from probability distribution
            measured_state_index = np.random.choice(len(probabilities), p=probabilities)
            
            # Convert to binary and extract measured qubits
            full_binary = format(measured_state_index, f'0{state.num_qubits}b')
            measured_bits = ''.join(full_binary[state.num_qubits - 1 - q] for q in qubits)
            
            measurement_results[measured_bits] = measurement_results.get(measured_bits, 0) + 1
        
        return measurement_results
    
    def simulate_circuit(self, circuit: QuantumCircuit, shots: int = 1000, 
                        initial_state: str = None) -> SimulationResult:
        """Simulate quantum circuit execution."""
        start_time = time.time()
        
        # Initialize state
        state = self.initialize_state(circuit.num_qubits, initial_state)
        
        # Apply gates sequentially
        for gate in circuit.gates:
            if gate.gate_type != GateType.MEASUREMENT:
                state = self.apply_gate(state, gate)
        
        # Perform measurements
        measurement_qubits = circuit.measurements if circuit.measurements else list(range(circuit.num_qubits))
        measurement_results = self.measure(state, measurement_qubits, shots)
        
        # Calculate metrics
        execution_time_ms = (time.time() - start_time) * 1000
        entanglement_entropy = self._calculate_entanglement_entropy(state)
        
        result = SimulationResult(
            circuit_id=circuit.circuit_id,
            final_state=state,
            measurement_results=measurement_results,
            execution_time_ms=execution_time_ms,
            shots=shots,
            entanglement_entropy=entanglement_entropy,
            gate_count=len(circuit.gates)
        )
        
        self.logger.info(f"Simulated circuit {circuit.circuit_id} in {execution_time_ms:.2f}ms")
        
        return result
    
    def _calculate_entanglement_entropy(self, state: QuantumState) -> float:
        """Calculate entanglement entropy (simplified for bipartition)."""
        if state.num_qubits < 2:
            return 0.0
        
        # Bipartition: first half vs second half
        n_a = state.num_qubits // 2
        n_b = state.num_qubits - n_a
        
        # Reshape state vector into matrix
        state_matrix = state.amplitudes.reshape(2**n_a, 2**n_b)
        
        # Calculate reduced density matrix for subsystem A
        rho_a = state_matrix @ state_matrix.conj().T
        
        # Calculate eigenvalues
        eigenvals = np.linalg.eigvals(rho_a)
        eigenvals = eigenvals[eigenvals > 1e-12]  # Remove numerical zeros
        
        # Calculate von Neumann entropy
        entropy = -np.sum(eigenvals * np.log2(eigenvals))
        
        return float(entropy)

class QuantumAlgorithmLibrary:
    """Library of quantum algorithms."""
    
    def __init__(self, simulator: QuantumSimulator):
        self.simulator = simulator
        self.logger = logging.getLogger('QuantumAlgorithmLibrary')
    
    def grover_search(self, num_qubits: int, marked_items: List[int]) -> QuantumCircuit:
        """Implement Grover's search algorithm."""
        circuit = self.simulator.create_circuit(
            num_qubits, 
            "Grover Search", 
            f"Grover's algorithm searching for items: {marked_items}"
        )
        
        # Initialize superposition
        for i in range(num_qubits):
            circuit = self.simulator.add_gate(circuit, GateType.HADAMARD, [i])
        
        # Calculate optimal number of iterations
        N = 2 ** num_qubits
        optimal_iterations = int(np.pi / 4 * np.sqrt(N / len(marked_items)))
        
        for iteration in range(optimal_iterations):
            # Oracle: mark target items
            for item in marked_items:
                # Convert item to binary and apply phase flip
                binary_item = format(item, f'0{num_qubits}b')
                
                # Apply controlled-Z gates to mark the item
                # (Simplified implementation)
                for i, bit in enumerate(binary_item):
                    if bit == '0':
                        circuit = self.simulator.add_gate(circuit, GateType.PAULI_X, [i])
                
                # Multi-controlled Z gate (simplified as phase gate on last qubit)
                if num_qubits > 1:
                    circuit = self.simulator.add_gate(
                        circuit, GateType.CONTROLLED_Z, 
                        [num_qubits-1], list(range(num_qubits-1))
                    )
                
                # Undo X gates
                for i, bit in enumerate(binary_item):
                    if bit == '0':
                        circuit = self.simulator.add_gate(circuit, GateType.PAULI_X, [i])
            
            # Diffusion operator
            for i in range(num_qubits):
                circuit = self.simulator.add_gate(circuit, GateType.HADAMARD, [i])
                circuit = self.simulator.add_gate(circuit, GateType.PAULI_X, [i])
            
            # Multi-controlled Z
            if num_qubits > 1:
                circuit = self.simulator.add_gate(
                    circuit, GateType.CONTROLLED_Z,
                    [num_qubits-1], list(range(num_qubits-1))
                )
            
            for i in range(num_qubits):
                circuit = self.simulator.add_gate(circuit, GateType.PAULI_X, [i])
                circuit = self.simulator.add_gate(circuit, GateType.HADAMARD, [i])
        
        self.logger.info(f"Created Grover circuit with {optimal_iterations} iterations")
        
        return circuit
    
    def quantum_fourier_transform(self, num_qubits: int) -> QuantumCircuit:
        """Implement Quantum Fourier Transform."""
        circuit = self.simulator.create_circuit(
            num_qubits,
            "Quantum Fourier Transform",
            f"QFT on {num_qubits} qubits"
        )
        
        for i in range(num_qubits):
            # Hadamard gate
            circuit = self.simulator.add_gate(circuit, GateType.HADAMARD, [i])
            
            # Controlled rotation gates
            for j in range(i + 1, num_qubits):
                angle = 2 * np.pi / (2 ** (j - i + 1))
                circuit = self.simulator.add_gate(
                    circuit, GateType.PHASE, [j], [i], {'phi': angle}
                )
        
        # Swap qubits to reverse order
        for i in range(num_qubits // 2):
            circuit = self.simulator.add_gate(
                circuit, GateType.SWAP, [i, num_qubits - 1 - i]
            )
        
        self.logger.info(f"Created QFT circuit for {num_qubits} qubits")
        
        return circuit
    
    def deutsch_jozsa(self, num_qubits: int, oracle_function: List[int]) -> QuantumCircuit:
        """Implement Deutsch-Jozsa algorithm."""
        circuit = self.simulator.create_circuit(
            num_qubits + 1,  # +1 for ancilla qubit
            "Deutsch-Jozsa",
            "Determine if function is constant or balanced"
        )
        
        # Initialize ancilla qubit to |1⟩
        circuit = self.simulator.add_gate(circuit, GateType.PAULI_X, [num_qubits])
        
        # Apply Hadamard to all qubits
        for i in range(num_qubits + 1):
            circuit = self.simulator.add_gate(circuit, GateType.HADAMARD, [i])
        
        # Oracle implementation (simplified)
        for i, output in enumerate(oracle_function):
            if output == 1:
                # Apply phase flip for this input
                binary_input = format(i, f'0{num_qubits}b')
                
                # Mark this input state
                for j, bit in enumerate(binary_input):
                    if bit == '0':
                        circuit = self.simulator.add_gate(circuit, GateType.PAULI_X, [j])
                
                # Controlled phase flip on ancilla
                circuit = self.simulator.add_gate(
                    circuit, GateType.CNOT, [num_qubits], list(range(num_qubits))
                )
                
                # Undo marking
                for j, bit in enumerate(binary_input):
                    if bit == '0':
                        circuit = self.simulator.add_gate(circuit, GateType.PAULI_X, [j])
        
        # Final Hadamard on input qubits
        for i in range(num_qubits):
            circuit = self.simulator.add_gate(circuit, GateType.HADAMARD, [i])
        
        # Measure input qubits
        circuit.measurements = list(range(num_qubits))
        
        self.logger.info(f"Created Deutsch-Jozsa circuit for {num_qubits} qubits")
        
        return circuit
    
    def quantum_teleportation(self) -> QuantumCircuit:
        """Implement quantum teleportation protocol."""
        circuit = self.simulator.create_circuit(
            3,
            "Quantum Teleportation",
            "Teleport quantum state from qubit 0 to qubit 2"
        )
        
        # Prepare Bell pair between qubits 1 and 2
        circuit = self.simulator.add_gate(circuit, GateType.HADAMARD, [1])
        circuit = self.simulator.add_gate(circuit, GateType.CNOT, [2], [1])
        
        # Bell measurement on qubits 0 and 1
        circuit = self.simulator.add_gate(circuit, GateType.CNOT, [1], [0])
        circuit = self.simulator.add_gate(circuit, GateType.HADAMARD, [0])
        
        # Conditional operations on qubit 2 based on measurement results
        # (In real implementation, these would be conditional on classical bits)
        circuit = self.simulator.add_gate(circuit, GateType.CNOT, [2], [1])
        circuit = self.simulator.add_gate(circuit, GateType.CONTROLLED_Z, [2], [0])
        
        self.logger.info("Created quantum teleportation circuit")
        
        return circuit

class QuantumComputingPlatform:
    """Enterprise quantum computing platform."""
    
    def __init__(self, max_qubits: int = 20):
        self.simulator = QuantumSimulator(max_qubits)
        self.algorithm_library = QuantumAlgorithmLibrary(self.simulator)
        self.circuits = {}
        self.results = {}
        self.logger = self._setup_logging()
        self.executor = ThreadPoolExecutor(max_workers=4)
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('QuantumComputingPlatform')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def create_circuit(self, num_qubits: int, name: str = "", description: str = "") -> str:
        """Create new quantum circuit."""
        circuit = self.simulator.create_circuit(num_qubits, name, description)
        self.circuits[circuit.circuit_id] = circuit
        
        self.logger.info(f"Created circuit {circuit.circuit_id} with {num_qubits} qubits")
        
        return circuit.circuit_id
    
    def add_gate_to_circuit(self, circuit_id: str, gate_type: str, 
                           target_qubits: List[int], control_qubits: List[int] = None,
                           parameters: Dict[str, float] = None) -> bool:
        """Add gate to existing circuit."""
        if circuit_id not in self.circuits:
            return False
        
        try:
            gate_enum = GateType(gate_type.lower())
            circuit = self.circuits[circuit_id]
            
            self.simulator.add_gate(
                circuit, gate_enum, target_qubits, 
                control_qubits or [], parameters or {}
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add gate to circuit {circuit_id}: {e}")
            return False
    
    def run_algorithm(self, algorithm: str, **kwargs) -> str:
        """Run predefined quantum algorithm."""
        try:
            algorithm_enum = QuantumAlgorithm(algorithm.lower())
            
            if algorithm_enum == QuantumAlgorithm.GROVER:
                num_qubits = kwargs.get('num_qubits', 3)
                marked_items = kwargs.get('marked_items', [5])
                circuit = self.algorithm_library.grover_search(num_qubits, marked_items)
            
            elif algorithm_enum == QuantumAlgorithm.QUANTUM_FOURIER_TRANSFORM:
                num_qubits = kwargs.get('num_qubits', 3)
                circuit = self.algorithm_library.quantum_fourier_transform(num_qubits)
            
            elif algorithm_enum == QuantumAlgorithm.DEUTSCH_JOZSA:
                num_qubits = kwargs.get('num_qubits', 3)
                oracle_function = kwargs.get('oracle_function', [0, 1, 1, 0, 1, 0, 0, 1])
                circuit = self.algorithm_library.deutsch_jozsa(num_qubits, oracle_function)
            
            elif algorithm_enum == QuantumAlgorithm.QUANTUM_TELEPORTATION:
                circuit = self.algorithm_library.quantum_teleportation()
            
            else:
                raise NotImplementedError(f"Algorithm {algorithm} not implemented")
            
            self.circuits[circuit.circuit_id] = circuit
            
            self.logger.info(f"Created {algorithm} algorithm circuit: {circuit.circuit_id}")
            
            return circuit.circuit_id
            
        except Exception as e:
            self.logger.error(f"Failed to run algorithm {algorithm}: {e}")
            raise
    
    def simulate_circuit(self, circuit_id: str, shots: int = 1000, 
                        initial_state: str = None) -> str:
        """Simulate quantum circuit."""
        if circuit_id not in self.circuits:
            raise ValueError(f"Circuit {circuit_id} not found")
        
        circuit = self.circuits[circuit_id]
        
        # Run simulation
        result = self.simulator.simulate_circuit(circuit, shots, initial_state)
        
        # Store result
        result_id = str(uuid.uuid4())
        self.results[result_id] = result
        
        self.logger.info(f"Simulated circuit {circuit_id}, result: {result_id}")
        
        return result_id
    
    def get_simulation_result(self, result_id: str) -> Optional[Dict[str, Any]]:
        """Get simulation result."""
        if result_id not in self.results:
            return None
        
        result = self.results[result_id]
        
        return {
            'circuit_id': result.circuit_id,
            'measurement_results': result.measurement_results,
            'execution_time_ms': result.execution_time_ms,
            'shots': result.shots,
            'entanglement_entropy': result.entanglement_entropy,
            'gate_count': result.gate_count,
            'fidelity': result.fidelity
        }
    
    def get_circuit_info(self, circuit_id: str) -> Optional[Dict[str, Any]]:
        """Get circuit information."""
        if circuit_id not in self.circuits:
            return None
        
        circuit = self.circuits[circuit_id]
        
        return {
            'circuit_id': circuit.circuit_id,
            'name': circuit.name,
            'num_qubits': circuit.num_qubits,
            'num_gates': len(circuit.gates),
            'description': circuit.description,
            'created_at': circuit.created_at.isoformat(),
            'gates': [
                {
                    'type': gate.gate_type.value,
                    'targets': gate.target_qubits,
                    'controls': gate.control_qubits,
                    'parameters': gate.parameters
                }
                for gate in circuit.gates
            ]
        }
    
    def list_circuits(self) -> List[Dict[str, Any]]:
        """List all circuits."""
        return [
            {
                'circuit_id': circuit.circuit_id,
                'name': circuit.name,
                'num_qubits': circuit.num_qubits,
                'num_gates': len(circuit.gates),
                'created_at': circuit.created_at.isoformat()
            }
            for circuit in self.circuits.values()
        ]
    
    def get_platform_stats(self) -> Dict[str, Any]:
        """Get platform statistics."""
        return {
            'max_qubits': self.simulator.max_qubits,
            'total_circuits': len(self.circuits),
            'total_simulations': len(self.results),
            'supported_algorithms': [alg.value for alg in QuantumAlgorithm],
            'supported_gates': [gate.value for gate in GateType],
            'platform_version': '1.0.0'
        }


def main():
    """Example usage of Quantum Computing Platform."""
    platform = QuantumComputingPlatform(max_qubits=10)
    
    try:
        print("🔬 Quantum Computing Simulator Platform")
        
        # Show platform capabilities
        stats = platform.get_platform_stats()
        print(f"   Max Qubits: {stats['max_qubits']}")
        print(f"   Supported Algorithms: {len(stats['supported_algorithms'])}")
        
        # Create simple circuit
        print("\n🔧 Creating quantum circuit...")
        circuit_id = platform.create_circuit(3, "Bell State Circuit", "Create Bell state")
        
        # Add gates to create Bell state
        platform.add_gate_to_circuit(circuit_id, "hadamard", [0])
        platform.add_gate_to_circuit(circuit_id, "cnot", [1], [0])
        
        print(f"✅ Created circuit: {circuit_id}")
        
        # Simulate circuit
        print("\n⚡ Simulating circuit...")
        result_id = platform.simulate_circuit(circuit_id, shots=1000)
        
        result = platform.get_simulation_result(result_id)
        print(f"📊 Simulation Results:")
        print(f"   Execution time: {result['execution_time_ms']:.2f}ms")
        print(f"   Entanglement entropy: {result['entanglement_entropy']:.3f}")
        print(f"   Measurement results:")
        
        for state, count in result['measurement_results'].items():
            probability = count / result['shots']
            print(f"     |{state}⟩: {count} ({probability:.1%})")
        
        # Run Grover's algorithm
        print("\n🔍 Running Grover's search algorithm...")
        grover_circuit_id = platform.run_algorithm(
            "grover", 
            num_qubits=3, 
            marked_items=[5]  # Search for |101⟩
        )
        
        grover_result_id = platform.simulate_circuit(grover_circuit_id, shots=1000)
        grover_result = platform.get_simulation_result(grover_result_id)
        
        print(f"🎯 Grover Results:")
        for state, count in grover_result['measurement_results'].items():
            probability = count / grover_result['shots']
            print(f"   |{state}⟩: {count} ({probability:.1%})")
        
        # Run Quantum Fourier Transform
        print("\n🌊 Running Quantum Fourier Transform...")
        qft_circuit_id = platform.run_algorithm("quantum_fourier_transform", num_qubits=3)
        
        qft_result_id = platform.simulate_circuit(qft_circuit_id, shots=1000)
        qft_result = platform.get_simulation_result(qft_result_id)
        
        print(f"📈 QFT completed in {qft_result['execution_time_ms']:.2f}ms")
        
        # Show circuit information
        print("\n📋 Circuit Information:")
        circuits = platform.list_circuits()
        
        for circuit in circuits:
            print(f"   🔗 {circuit['name']}")
            print(f"      ID: {circuit['circuit_id']}")
            print(f"      Qubits: {circuit['num_qubits']}, Gates: {circuit['num_gates']}")
        
        print("\n✅ Quantum computing simulation completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()