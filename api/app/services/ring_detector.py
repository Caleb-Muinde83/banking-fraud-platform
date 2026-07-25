import uuid
import time
import networkx as nx
from typing import List
from app.schemas.graph import (
    NodeType, 
    EdgeType, 
    RingType, 
    FraudRingAlert, 
    RingDetectionResponse
)


class RingDetectorService:
    """
    Executes graph algorithms for fraud ring detection:
    1. Bounded circular path analysis (Depth-limited simple cycles <= max_hop_length).
    2. Shared infrastructure detection (Device/IP sharing across unlinked customers).
    """

    @staticmethod
    def analyze_graph(
        G: nx.MultiDiGraph, 
        max_hop_length: int = 6, 
        min_shared_customers: int = 3
    ) -> RingDetectionResponse:
        start_time = time.perf_counter()
        alerts: List[FraudRingAlert] = []

        # ---------------------------------------------------------------------
        # 1. Circular Transfer Path Detection (Account -> Account transfers)
        # ---------------------------------------------------------------------
        # Extract direct account-to-account transfer graph
        transfer_edges = [
            (u, v, data) for u, v, k, data in G.edges(keys=True, data=True)
            if data.get("relationship") == EdgeType.TRANSFERRED_TO.value
        ]
        
        tx_G = nx.DiGraph()
        for u, v, data in transfer_edges:
            amt = data.get("amount", 0.0)
            if tx_G.has_edge(u, v):
                tx_G[u][v]["total_amount"] += amt
                tx_G[u][v]["count"] += 1
            else:
                tx_G.add_edge(u, v, total_amount=amt, count=1)

        # Enumerate cycles with strict length upper bound (3 <= length <= max_hop_length)
        raw_cycles = nx.simple_cycles(tx_G)
        for cycle in raw_cycles:
            hop_count = len(cycle)
            if 3 <= hop_count <= max_hop_length:
                # Calculate total circulating volume along cycle edges
                cycle_edges = list(zip(cycle, cycle[1:] + [cycle[0]]))
                total_volume = sum(tx_G[u][v]["total_amount"] for u, v in cycle_edges if tx_G.has_edge(u, v))

                severity = "CRITICAL" if total_volume > 10000 else "HIGH"
                cycle_str = " -> ".join(cycle) + f" -> {cycle[0]}"

                alerts.append(
                    FraudRingAlert(
                        ring_id=f"RING-CIRCULAR-{uuid.uuid4().hex[:8]}",
                        ring_type=RingType.CIRCULAR_TRANSFER_RING,
                        member_nodes=cycle,
                        hop_count=hop_count,
                        total_volume=round(total_volume, 2),
                        severity=severity,
                        description=f"Circular transfer ring detected across {hop_count} accounts: [{cycle_str}] with volume ${total_volume:,.2f}"
                    )
                )

        # ---------------------------------------------------------------------
        # 2. Shared Infrastructure Cluster Detection (Device / IP)
        # ---------------------------------------------------------------------
        for node, data in G.nodes(data=True):
            node_label = data.get("label")
            if node_label in (NodeType.DEVICE.value, NodeType.IP.value):
                # Find all connected customer profiles (in-degree from CUSTOMER nodes)
                connected_customers = [
                    neighbor for neighbor in G.predecessors(node)
                    if G.nodes[neighbor].get("label") == NodeType.CUSTOMER.value
                ]

                unique_cust_count = len(set(connected_customers))
                if unique_cust_count >= min_shared_customers:
                    infra_type = "Device Fingerprint" if node_label == NodeType.DEVICE.value else "IP Address"
                    severity = "CRITICAL" if unique_cust_count >= 5 else "HIGH"

                    alerts.append(
                        FraudRingAlert(
                            ring_id=f"RING-INFRA-{uuid.uuid4().hex[:8]}",
                            ring_type=RingType.SHARED_INFRASTRUCTURE_CLUSTER,
                            member_nodes=set(connected_customers + [node]),
                            hop_count=2,
                            shared_entity_id=node,
                            severity=severity,
                            description=(
                                f"Suspicious shared infrastructure anomaly: {infra_type} '{node}' "
                                f"is actively linked across {unique_cust_count} distinct customer profiles."
                            )
                        )
                    )

        exec_time_ms = (time.perf_counter() - start_time) * 1000.0

        return RingDetectionResponse(
            detected_rings=alerts,
            scanned_nodes_count=G.number_of_nodes(),
            scanned_edges_count=G.number_of_edges(),
            execution_time_ms=round(exec_time_ms, 2)
        )