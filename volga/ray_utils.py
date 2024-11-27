import ray


def get_head_node_id() -> str:
    all_nodes = ray.nodes()
    for n in all_nodes:
        _resources = n['Resources']
        is_head = 'node:__internal_head__' in _resources
        if is_head:
            return n['NodeID']
    raise RuntimeError('Unable to locate head node')