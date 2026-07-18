from collections import defaultdict, deque
from typing import Tuple, List, Set
from shared.models import DAG


class DAGValidator:
    def __init__(self, dag: DAG):
        self.dag = dag
        self.node_lookup = {n.id: n for n in dag.nodes}

    def validate(self) -> Tuple[bool, str]:
        for n in self.dag.nodes:
            for dep in n.dependencies:
                if dep not in self.node_lookup:
                    return False, f"Node '{n.id}' references missing dependency '{dep}'"

        if self._check_cycle():
            return False, "Circular dependency detected"

        return True, ""

    def _check_cycle(self) -> bool:
        in_deg = defaultdict(int)
        adj = defaultdict(list)

        for n in self.dag.nodes:
            if n.id not in in_deg:
                in_deg[n.id] = 0

        for n in self.dag.nodes:
            for dep in n.dependencies:
                adj[dep].append(n.id)
                in_deg[n.id] += 1

        q = deque([n.id for n in self.dag.nodes if in_deg[n.id] == 0])
        count = 0
        while q:
            curr = q.popleft()
            count += 1
            for neighbor in adj[curr]:
                in_deg[neighbor] -= 1
                if in_deg[neighbor] == 0:
                    q.append(neighbor)

        return count < len(self.dag.nodes)

    def get_executable_nodes(self, done: Set[str]) -> List[str]:
        ready = []
        for n in self.dag.nodes:
            deps_ok = all(d in done for d in n.dependencies)
            if deps_ok and n.id not in done:
                ready.append(n.id)
        return ready
