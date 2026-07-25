import networkx as nx
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models import domain as models
from app.schemas.graph import NodeType, EdgeType, NodeSchema, EdgeSchema, GraphResponse


class GraphBuilderService:
    """
    Asynchronously queries domain database entities (Accounts, Transactions, Devices, Logins)
    and constructs an in-memory NetworkX MultiDiGraph for relational and path analysis.
    """

    @staticmethod
    async def build_graph(
        db: AsyncSession, 
        lookback_hours: int = 24
    ) -> nx.MultiDiGraph:
        G = nx.MultiDiGraph()
        cutoff_time = datetime.utcnow() - timedelta(hours=lookback_hours)

        # 1. Customer -> Account (OWNS)
        acct_stmt = select(models.Account)
        acct_res = await db.execute(acct_stmt)
        for acct in acct_res.scalars().all():
            cust_node = f"CUST-{acct.customer_id}"
            acct_id_str = str(acct.account_id)
            acct_node = acct_id_str if acct_id_str.startswith("ACCT-") or acct_id_str.startswith("ACC-") else f"ACCT-{acct_id_str}"

            G.add_node(cust_node, label=NodeType.CUSTOMER.value, customer_id=str(acct.customer_id))
            G.add_node(acct_node, label=NodeType.ACCOUNT.value, account_id=acct_id_str)
            G.add_edge(cust_node, acct_node, relationship=EdgeType.OWNS.value)

        # 2. Account -> Account (TRANSFERRED_TO)
        tx_stmt = select(models.Transaction).where(models.Transaction.timestamp >= cutoff_time)
        tx_res = await db.execute(tx_stmt)
        for tx in tx_res.scalars().all():
            from_acc_str = str(tx.from_account)
            to_acc_str = str(tx.to_account)
            
            from_node = from_acc_str if from_acc_str.startswith("ACCT-") or from_acc_str.startswith("ACC-") else f"ACCT-{from_acc_str}"
            to_node = to_acc_str if to_acc_str.startswith("ACCT-") or to_acc_str.startswith("ACC-") else f"ACCT-{to_acc_str}"

            # Ensure nodes exist
            if not G.has_node(from_node):
                G.add_node(from_node, label=NodeType.ACCOUNT.value)
            if not G.has_node(to_node):
                G.add_node(to_node, label=NodeType.ACCOUNT.value)

            tx_timestamp = getattr(tx, "timestamp", None)
            tx_id = getattr(tx, "transaction_id", getattr(tx, "id", ""))

            G.add_edge(
                from_node, 
                to_node, 
                relationship=EdgeType.TRANSFERRED_TO.value,
                amount=float(tx.amount) if tx.amount is not None else 0.0,
                timestamp=tx_timestamp.isoformat() if tx_timestamp else None,
                tx_id=str(tx_id)
            )

        # 3. Customer -> Device (USED_DEVICE)
        dev_stmt = select(models.Device)
        dev_res = await db.execute(dev_stmt)
        for dev in dev_res.scalars().all():
            cust_node = f"CUST-{dev.customer_id}"
            dev_id_str = str(dev.device_id)
            dev_node = dev_id_str if dev_id_str.startswith("DEV-") else f"DEV-{dev_id_str}"

            if not G.has_node(cust_node):
                G.add_node(cust_node, label=NodeType.CUSTOMER.value, customer_id=str(dev.customer_id))
            G.add_node(dev_node, label=NodeType.DEVICE.value, device_id=dev_id_str)

            G.add_edge(
                cust_node, 
                dev_node, 
                relationship=EdgeType.USED_DEVICE.value,
                device_type=getattr(dev, "device_type", None),
                trust_score=float(dev.trust_score) if getattr(dev, "trust_score", None) is not None else None
            )

        # 4. Customer -> IP Address (LOGGED_IN_FROM) derived from LoginEvent
        login_stmt = select(models.LoginEvent).where(models.LoginEvent.timestamp >= cutoff_time)
        login_res = await db.execute(login_stmt)
        for login in login_res.scalars().all():
            if not login.ip_address:
                continue

            cust_node = f"CUST-{login.customer_id}"
            ip_node = f"IP-{login.ip_address}"

            if not G.has_node(cust_node):
                G.add_node(cust_node, label=NodeType.CUSTOMER.value, customer_id=str(login.customer_id))
            G.add_node(ip_node, label=NodeType.IP.value, ip_address=login.ip_address)

            login_timestamp = getattr(login, "timestamp", None)
            login_success = getattr(login, "success", True)

            G.add_edge(
                cust_node, 
                ip_node, 
                relationship=EdgeType.LOGGED_IN_FROM.value,
                status="SUCCESS" if login_success else "FAILED",
                timestamp=login_timestamp.isoformat() if login_timestamp else None
            )

        return G

    @staticmethod
    def extract_subgraph_schema(G: nx.MultiDiGraph, center_entity_id: str, depth: int = 2) -> GraphResponse:
        """
        Extracts an ego-network centered at `center_entity_id` up to `depth` hops.
        Designed to run in CPU thread pool via asyncio.to_thread().
        """
        if not G.has_node(center_entity_id):
            return GraphResponse(nodes=[], edges=[])

        # Compute ego graph (undirected radius traversal for neighborhood extraction)
        sub_nodes = set(nx.single_source_shortest_path_length(G.to_undirected(), center_entity_id, cutoff=depth).keys())
        subG = G.subgraph(sub_nodes)

        nodes_out = []
        for n, data in subG.nodes(data=True):
            label = data.get("label", NodeType.CUSTOMER.value)
            props = {k: v for k, v in data.items() if k != "label"}
            nodes_out.append(NodeSchema(id=n, label=NodeType(label), properties=props))

        edges_out = []
        for u, v, k, data in subG.edges(keys=True, data=True):
            rel = data.get("relationship", EdgeType.TRANSFERRED_TO.value)
            props = {k: v for k, v in data.items() if k != "relationship"}
            edges_out.append(EdgeSchema(source=u, target=v, relationship=EdgeType(rel), properties=props))

        return GraphResponse(nodes=nodes_out, edges=edges_out)