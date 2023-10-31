import json
from typing import Any, Dict, List, Optional, Tuple

from requests import Session, exceptions

from hyperon_das_atomdb.exceptions import (
    ConnectionServerException,
    NodeDoesNotExistException,
)
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
        if not ip_address:
            raise ConnectionServerException(
                message='You must send ip_address parameter'
            )
        self.database_name = database_name
        self.ip = ip_address
        self.port = port if port else '8080'
        self.openfaas_uri = f'http://{self.ip}:{self.port}/function/atomdb'
        self.aws_lambda_uri = f'https://q49t4szcke.execute-api.us-east-1.amazonaws.com/prod/atomdb'
        self.session = Session()
        self._connect_server()

    @retry(attempts=5, timeout_seconds=120)
    def _connect_server(self) -> str | None:
        self.url = None
        if self._is_server_connect(self.openfaas_uri):
            self.url = self.openfaas_uri
        elif self._is_server_connect(self.aws_lambda_uri):
            self.url = self.aws_lambda_uri
        return self.url

    def _is_server_connect(self, url: str) -> bool:
        try:
            response = self.session.post(
                url=url, data=json.dumps({"action": "ping", "input": {}})
            )
        except Exception as e:
            raise e
        if response.status_code == 200:
            return True
        return False

    def _send_request(self, payload) -> str | dict | int:
        try:
            response = self.session.post(
                url=self.url, data=json.dumps(payload)
            )
            # TODO: WIP - Refactor this part
            if response.status_code == 200:
                text = response.text
                try:
                    result = eval(text.split('\n')[-2])
                    return result
                except Exception:
                    return text.split('\n')[-2]
            else:
                return response.text
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
            'input': {},
        }
        if node_handle:
            payload['input']['node_handle'] = node_handle
        if node_type:
            payload['input']['node_type'] = node_type
        if node_name:
            payload['input']['node_name'] = node_name
        if substring:
            payload['input']['substring'] = substring
        if names is not None:
            payload['input']['names'] = names

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
            'input': {},
        }
        if link_type:
            payload['input']['link_type'] = link_type
        if target_handles:
            payload['input']['target_handles'] = target_handles
        if link_handle:
            payload['input']['link_handle'] = link_handle
        if template:
            payload['input']['template'] = template
        if extra_parameters:
            payload['input']['extra_parameters'] = extra_parameters

        return self._send_request(payload)

    def _get_atom_information(
        self, action: str, handle: str = None, arity: int = -1
    ):
        payload = {'action': action, 'input': {}}
        if action != 'count_atoms':
            payload['input']['handle'] = handle
            payload['input']['arity'] = arity
        return self._send_request(payload)

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        result = self._get_node_information(
            'get_node_handle', node_type=node_type, node_name=node_name
        )
        if 'error' in result:
            raise NodeDoesNotExistException(
                message='This node does not exist',
                details=f'{node_type}:{node_name}',
            )
        return result

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
            'get_matched_node_name', node_type=node_type, substring=substring
        )

    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        return self._get_node_information(
            'get_all_nodes', node_type=node_type, names=names
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
        payload = {'action': 'clear_database', 'input': {}}
        response = self._send_request(payload)
        if response.get('status_code') == 200:
            return response.get('message', '')
        return ''

    def prefetch(self) -> None:
        payload = {'action': 'clear_database', 'input': {}}
        response = self._send_request(payload)
        if response.get('status_code') == 200:
            return response.get('message', '')
        return ''
