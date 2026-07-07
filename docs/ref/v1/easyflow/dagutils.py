from networkx import DiGraph
from networkx.algorithms.dag import is_directed_acyclic_graph




    def find_root(self, G):
        # task: 没有上游且没有完成
        for node in G.nodes:
            print('-'*30)
            print('node', node, type(node))
            print('ance', nx.ancestors(G, node))
            # print('desc', node.descendants)


