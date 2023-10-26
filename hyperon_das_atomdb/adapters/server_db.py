import json
import os
from typing import Any, Dict, List, Optional, Tuple

from requests import exceptions, request

from hyperon_das_atomdb.i_database import IAtomDB
from hyperon_das_atomdb.utils.decorators import retry


class ServerDB(IAtomDB):
    """A concrete implementation using servers databases.
    AwsLambda and OpenFaas"""

    def __repr__(self) -> str:
        return "<Atom database Server>"  # pragma no cover

    def __init__(
        self,
        database_name: str = 'das',
        ip_address: str = None,
        port: Optional[str] = None,
    ) -> None:
        self.database_name = database_name
        self.ip = ip_address
        self.port = port if port else '8080'
        self.openfaas_uri = f'{self.ip}:{port}/ui/'
        self.aws_lambda_uri = f'{self.ip}/prod'
        self._connect_server()

    @retry(attempts=5, timeout_seconds=120)
    def _connect_server(self) -> None:
        self.url = None
        if self._is_server_connect(self.openfaas_uri):
            self.url = self.openfaas_uri
        elif self._is_server_connect(self.aws_lambda_uri):
            self.url = self.aws_lambda_uri

    def _is_server_connect(self, url: str) -> bool:
        response = self._send_request({'action': 'healthy_check', 'url': url})
        if response:
            return True
        return False

    def _send_request(self, payload) -> dict:
        try:
            response = request("POST", url=self.url, data=json.dumps(payload))
            if response.status_code == 200:
                return response.json()
        except exceptions.RequestException as e:
            raise e

    def _get_node_information(
        self,
        action: str,
        node_handle: str = None,
        node_type: str = None,
        node_name: str = None,
        substring: str = None,
        names: bool = None,
    ):
        payload = {
            'action': action,
            'database_name': self.database_name,
        }
        if node_handle:
            payload['node_handle'] = node_handle
        if node_type:
            payload['node_type'] = node_type
        if node_name:
            payload['node_name'] = node_name
        if substring:
            payload['substring'] = substring
        if names is not None:
            payload['names'] = names

        return self._send_request(payload)

    def _get_link_information(
        self,
        action: str,
        link_type: str = None,
        target_handles: List[str] = None,
        link_handle: str = None,
        template: List[Any] = None,
        extra_parameters: Optional[Dict[str, Any]] = None,
    ):
        payload = {
            'action': action,
            'database_name': self.database_name,
        }
        if link_type:
            payload['link_type'] = link_type
        if target_handles:
            payload['target_handles'] = target_handles
        if link_handle:
            payload['link_handle'] = link_handle
        if template:
            payload['template'] = template
        if extra_parameters:
            payload['extra_parameters'] = extra_parameters

        return self._send_request(payload)

    def _get_atom_information(self, action: str, handle: str, arity: int = -1):
        payload = {'action': action, 'database_name': self.database_name}
        if action != 'count_atoms':
            payload['handle'] = handle
            payload['arity'] = arity

        return self._send_request(payload)

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        return self._get_node_information(
            'get_node_handle', node_type=node_type, node_name=node_name
        )

    def get_node_name(self, node_handle: str) -> str:
        return self._get_node_information(
            'get_node_name', node_handle=node_handle
        )

    def get_node_type(self, node_handle: str) -> str:
        return self._get_node_information(
            'get_node_type', node_handle=node_handle
        )

    def get_matched_node_name(self, node_type: str, substring: str) -> str:
        return self._get_node_information(
            'get_node_type', node_type=node_type, substring=substring
        )

    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        return self._get_node_information(
            'get_node_type', node_type=node_type, names=names
        )

    def get_link_handle(
        self, link_type: str, target_handles: List[str]
    ) -> str:
        return self._get_link_information(
            'get_link_handle',
            link_type=link_type,
            target_handles=target_handles,
        )

    def get_link_targets(self, link_handle: str) -> List[str]:
        return self._get_link_information(
            'get_link_targets', link_handle=link_handle
        )

    def is_ordered(self, link_handle: str) -> bool:
        return self._get_link_information(
            'is_ordered', link_handle=link_handle
        )

    def get_matched_links(
        self,
        link_type: str,
        target_handles: List[str],
        extra_parameters: Optional[Dict[str, Any]] = None,
    ):
        return self._get_link_information(
            'get_matched_links',
            link_type=link_type,
            target_handles=target_handles,
            extra_parameters=extra_parameters,
        )

    def get_matched_type_template(
        self,
        template: List[Any],
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        return self._get_link_information(
            'get_matched_type_template',
            template=template,
            extra_parameters=extra_parameters,
        )

    def get_matched_type(
        self, link_type: str, extra_parameters: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        return self._get_link_information(
            'get_matched_type',
            link_type=link_type,
            extra_parameters=extra_parameters,
        )

    def get_link_type(self, link_handle: str) -> str:
        return self._get_link_information(
            'get_link_type', link_handle=link_handle
        )

    def get_atom_as_dict(self, handle: str, arity=-1) -> dict:
        return self._get_atom_information(
            'get_atom_as_dict', handle=handle, arity=arity
        )

    def get_atom_as_deep_representation(self, handle: str, arity=-1) -> str:
        return self._get_atom_information(
            'get_atom_as_deep_representation', handle=handle, arity=arity
        )

    def count_atoms(self) -> Tuple[int, int]:
        return self._get_atom_information('count_atoms')

    def clear_database(self) -> None:
        payload = {
            'action': 'clear_database',
            'database_name': self.database_name,
        }
        response = self._send_request(payload)
        if response.get('status_code') == 200:
            return response.get('message', '')
        return ''

    def prefetch(self) -> None:
        payload = {
            'action': 'prefetch',
            'database_name': self.database_name,
        }
        response = self._send_request(payload)
        if response.get('status_code') == 200:
            return response.get('message', '')
        return ''
