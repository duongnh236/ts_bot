"""Fast scene graph lookup for automatic world travel."""

from collections import defaultdict, deque
import json


class WorldNavStore:
    def __init__(self, path):
        with open(path, encoding="utf-8") as fh:
            self.data = json.load(fh)
        self.fingerprint = self.data["fingerprint"]
        self.gates = self.data["gates"]
        self.graph = defaultdict(list)
        self.reverse_graph = defaultdict(list)
        self.states = set()
        for edge in self.data["edges"]:
            self.graph[edge["from"]].append(edge)
            self.reverse_graph[edge["to"]].append(edge)
            self.states.add(edge["from"])
            self.states.add(edge["to"])
        for edges in self.graph.values():
            edges.sort(key=self._edge_key)
        for edges in self.reverse_graph.values():
            edges.sort(key=lambda edge: (edge["from"], *self._edge_key(edge)))
        self._target_cache = {}

    def _gate_unusable(self, edge):
        """Cong co image [0,0,0] = KHONG co hinh cua (warp event/script, vd 12011 door12
        event20) -> di bo toi + gui 0x14 08 KHONG chuyen map. Xep sau cong-cua-that de
        router uu tien cong di bo qua duoc khi co lua chon (bug party ket o 12011->12000)."""
        gate = self.gates.get(str(edge["scene"]), {}).get(str(edge["door"]))
        if gate is None:
            return 1
        return 1 if list(gate.get("image", [0, 0, 0])) == [0, 0, 0] else 0

    def _edge_key(self, edge):
        return self._gate_unusable(edge), edge["priority"], edge["door"], edge["to"]

    def _links_to(self, target_scene):
        target_scene = int(target_scene)
        if target_scene in self._target_cache:
            return self._target_cache[target_scene]

        targets = sorted(
            state for state in self.states if state // 1000 == target_scene
        )
        if not targets:
            self._target_cache[target_scene] = {}
            return {}

        links = {state: None for state in targets}
        queue = deque(targets)
        while queue:
            current = queue.popleft()
            for edge in self.reverse_graph.get(current, ()):
                source = edge["from"]
                if source in links:
                    continue
                links[source] = (edge, current)
                queue.append(source)
        self._target_cache[target_scene] = links
        return links

    @staticmethod
    def _build_route(links, state):
        legs = []
        while links[state] is not None:
            edge, state = links[state]
            legs.append(edge)
        return legs

    def find_scene_routes(self, source_scene, target_scene):
        links = self._links_to(target_scene)
        matches = [
            self._build_route(links, state)
            for state in links
            if state // 1000 == int(source_scene)
        ]
        unique = {}
        for legs in matches:
            key = tuple(
                (leg["from"], leg["to"], leg["door"])
                for leg in legs
            )
            unique[key] = legs
        return sorted(
            unique.values(),
            key=lambda legs: (
                len(legs),
                tuple(self._edge_key(edge) for edge in legs),
            ),
        )

    def rank_cities(self, target_scene):
        links = self._links_to(target_scene)
        states_by_scene = defaultdict(list)
        for state in links:
            states_by_scene[state // 1000].append(state)
        ranked = []
        for city in self.data["cities"]:
            for state in states_by_scene.get(city["city"], ()):
                legs = self._build_route(links, state)
                ranked.append({**city, "legs": legs, "gate_count": len(legs)})
        return sorted(
            ranked,
            key=lambda item: (item["gate_count"], item["city"]),
        )

    def get_gate(self, scene_id, gate_id):
        return self.gates.get(str(int(scene_id)), {}).get(str(int(gate_id)))
