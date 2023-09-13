from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass
class Link:
    arity_1: Dict[str, Any]
    arity_2: Dict[str, Any]
    arity_n: Dict[str, Any]

    def get_arity(self, arity: int):
        if arity == 1:
            return self.arity_1
        if arity == 2:
            return self.arity_2
        if arity > 2:
            return self.arity_n

    def all_arities(self) -> Dict[str, Any]:
        # TODO: Validate if there is a possibility of duplicate keys
        all_arities = {}
        all_arities.update(self.arity_1)
        all_arities.update(self.arity_2)
        all_arities.update(self.arity_3)
        return all_arities


@dataclass
class Database:
    atom_type: Dict[str, Any]
    node: Dict[str, Any]
    link: Link
    outgoing_set: Dict[str, Any]
    ingoing_set: Dict[str, Any]
    patterns: Dict[str, List[Tuple]]
    templates: Dict[str, List[Tuple]]
    names: Dict[str, str]
    
    # def __post_init__(self):
    #     for key, value in self.node.items():
    #         if not isinstance(value, dict) or 'composite_type_hash' not in value or 'name' not in value or 'named_type' not in value:
    #             raise ValueError(f"'node[{key}]' is invalid.")


# db = Database(
#     atom_type={
#         '0cc896470995bd4187a844163b6f1012': {
#             'composite_type_hash': '26a3ccbc2d2c83e0e39a01e0eccc4fd1',
#             'named_type': 'Similarity',
#             'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
#         },
#     },
#     node={
#         'af12f10f9ae2002a1607ba0b47ba8407': {
#             'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
#             'name': 'human',
#             'named_type': 'Concept',
#         },
#         '1cdffc6b0b89ff41d68bec237481d1e1': {
#             'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
#             'name': 'monkey',
#             'named_type': 'Concept',
#         },
#     },
#     link={
#         '2e9c59947cae7dd133af21bbfcf79902': {
#             'composite_type_hash': 'db6163e5526ce17d293f16fe88a9948c',
#             'is_toplevel': True,
#             'composite_type': [
#                 'a9dea78180588431ec64d6bc4872fdbc',
#                 '99e9bae675b12967251c175696f00a70',
#                 'd0763edaa9d9bd2a9516280e9044d885',
#             ],
#             'named_type': 'Similarity',
#             'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
#             'key_0': 'bd497eb24420dd50fed5f3d2e6cdd7c1',
#             'key_1': '305e7d502a0ce80b94374ff0d79a6464',
#         }
#     },
#     outgoing_set={
#         '062558af0dc7bd964acec001e694c984': [
#             'a1fb3a4de5c459bfa4bd87dc423019c3',
#             'bd497eb24420dd50fed5f3d2e6cdd7c1',
#         ],
#         '3afd51603c2d1b64c5b9455ec5a8c166': [
#             'd1ec11ec366a1deb24a079dc39863c68',
#             'c90242e2dbece101813762cc2a83d726',
#         ],
#     },
#     patterns={
#         '7ead6cfa03894c62761162b7603aa885': [
#             (
#                 '62034e96aae3327359a996af1b7f4dfb',
#                 (
#                     '98870929d76a80c618e70a0393055b31',
#                     '81ec21b0f1b03e18c55e056a56179fef',
#                 ),
#             ),
#             (
#                 '85366aa321bd5a01774da82563460bc1',
#                 (
#                     'c77b519f8ab36dfea8e2a532a7603d9a',
#                     'd1ec11ec366a1deb24a079dc39863c68',
#                 ),
#             ),
#             (
#                 'eb1c966e37db95bc3df9ddd1434b9700',
#                 (
#                     'c90242e2dbece101813762cc2a83d726',
#                     '81ec21b0f1b03e18c55e056a56179fef',
#                 ),
#             ),
#             (
#                 '998720ba240adb15bd6665a6418959b0',
#                 (
#                     '0e37558e8f5397de131a3c3b03f81730',
#                     '81ec21b0f1b03e18c55e056a56179fef',
#                 ),
#             ),
#             (
#                 'fd1a31a515f9b662fbfc9db060fbf59d',
#                 (
#                     'fa77994f6835fad256902605a506c59c',
#                     '98870929d76a80c618e70a0393055b31',
#                 ),
#             ),
#             (
#                 '071782f97962e769e19213c4d52fcd42',
#                 (
#                     'a1fb3a4de5c459bfa4bd87dc423019c3',
#                     '98870929d76a80c618e70a0393055b31',
#                 ),
#             ),
#             (
#                 '62d52d8fd07f3c0c06a38146d540168b',
#                 (
#                     '305e7d502a0ce80b94374ff0d79a6464',
#                     '98870929d76a80c618e70a0393055b31',
#                 ),
#             ),
#             (
#                 '062558af0dc7bd964acec001e694c984',
#                 (
#                     'e2d9b15ab3461228d75502e754137caa',
#                     'c90242e2dbece101813762cc2a83d726',
#                 ),
#             ),
#             (
#                 'c955f36c1ed2aee6ded094d88aa0ac98',
#                 (
#                     '683d1b2246dac6cd167a02121cf03a26',
#                     '9b7d6c5e3f564af36322282e370cb59f',
#                 ),
#             ),
#             (
#                 'f289012d080749f29bb0bbb1a610a6eb',
#                 (
#                     'bd497eb24420dd50fed5f3d2e6cdd7c1',
#                     '98870929d76a80c618e70a0393055b31',
#                 ),
#             ),
#             (
#                 '3afd51603c2d1b64c5b9455ec5a8c166',
#                 (
#                     'd1ec11ec366a1deb24a079dc39863c68',
#                     'c90242e2dbece101813762cc2a83d726',
#                 ),
#             ),
#             (
#                 '7fa1b01e053fe4bb3c229be608ce2588',
#                 (
#                     '05684ad07e3ee172e2289bb0d8623cda',
#                     '9b7d6c5e3f564af36322282e370cb59f',
#                 ),
#             ),
#         ]
#     },
#     templates={
#         '7ead6cfa03894c62761162b7603aa885': [
#             (
#                 '62034e96aae3327359a996af1b7f4dfb',
#                 (
#                     '98870929d76a80c618e70a0393055b31',
#                     '81ec21b0f1b03e18c55e056a56179fef',
#                 ),
#             ),
#             (
#                 '85366aa321bd5a01774da82563460bc1',
#                 (
#                     'c77b519f8ab36dfea8e2a532a7603d9a',
#                     'd1ec11ec366a1deb24a079dc39863c68',
#                 ),
#             ),
#         ]
#     },
#     names={
#         'af12f10f9ae2002a1607ba0b47ba8407': 'human',
#         'b99ae727c787f1b13b452fd4c9ce1b9a': 'reptile',
#     },
# )
