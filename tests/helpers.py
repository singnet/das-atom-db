from typing import cast

from hyperon_das_atomdb.database import (
    AtomDB,
    AtomT,
    CustomAttributesT,
    LinkParamsT,
    LinkT,
    NodeParamsT,
    NodeT,
)


def check_handle(handle):
    return all((isinstance(handle, str), len(handle) == 32, int(handle, 16)))


def add_node(
    db: AtomDB, node_name, node_type, adapter, custom_attributes: CustomAttributesT | None = None
):
    node_params = NodeParamsT(name=node_name, type=node_type, custom_attributes=custom_attributes)
    node = db.add_node(node_params)
    if adapter != "in_memory_db":
        db.commit()
    return node


def add_link(
    db: AtomDB, link_type, targets: list[NodeParamsT | LinkParamsT], adapter, is_toplevel=True
):
    link = db.add_link(LinkParamsT(type=link_type, targets=targets), toplevel=is_toplevel)
    if adapter != "in_memory_db":
        db.commit()
    return link


def atom_to_params(atom: AtomT) -> NodeParamsT | LinkParamsT:
    if isinstance(atom, NodeT):
        node = cast(NodeT, atom)
        return NodeParamsT(type=node.named_type, name=node.name)
    link = cast(LinkT, atom)
    targets = [atom_to_params(t) for t in link.targets]
    return LinkParamsT(type=link.named_type, targets=targets)


def dict_to_node_params(node_dict: dict) -> NodeParamsT:
    return NodeParamsT(**node_dict)


def dict_to_link_params(link_dict: dict) -> LinkParamsT:
    return LinkParamsT(
        type=link_dict["type"],
        targets=[
            dict_to_link_params(target) if "targets" in target else dict_to_node_params(target)
            for target in link_dict["targets"]
        ],
    )
