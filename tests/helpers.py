from copy import deepcopy
from typing import cast

from hyperon_das_atomdb.database import (
    AtomDB,
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


def atom_to_params(atom: LinkT | NodeT) -> NodeParamsT | LinkParamsT:
    if isinstance(atom, NodeT):
        node = cast(NodeT, atom)
        return NodeParamsT(type=node.named_type, name=node.name)

    def _build_targets(_targets: list[LinkT | NodeT]) -> list[LinkParamsT | NodeParamsT]:
        return [
            LinkParamsT(type=_target.named_type, targets=_build_targets(_target.targets_documents))
            if isinstance(_target, LinkT)
            else NodeParamsT(type=_target.named_type, name=_target.name)
            for _target in _targets
        ]

    link = cast(LinkT, atom)
    assert link.targets_documents is not None, "only links with targets_documents are supported"
    return LinkParamsT(
        type=link.named_type,
        targets=_build_targets(link.targets_documents),
    )


def dict_to_node_params(node_dict: dict) -> NodeParamsT:
    return NodeParamsT(**node_dict)


def dict_to_link_params(link_dict: dict) -> LinkParamsT:
    targets = [
        dict_to_link_params(target) if "targets" in target else dict_to_node_params(target)
        for target in link_dict["targets"]
    ]
    params = deepcopy(link_dict)
    params.update({"targets": targets})
    return LinkParamsT(**params)
