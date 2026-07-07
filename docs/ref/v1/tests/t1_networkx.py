import networkx as nx


#ref: https://mungingdata.com/python/dag-directed-acyclic-graph-networkx/
graph = nx.DiGraph()
graph.add_edges_from([("root", "a"), ("a", "b"), ("a", "e"), ("b", "c"), ("b", "d"), ("d", "e")])

print(graph.nodes())

rv = nx.shortest_path(graph, 'root', 'e') # => ['root', 'a', 'e']
print(rv)

rv = nx.dag_longest_path(graph)
print('longest', rv)

ll = list(nx.topological_sort(graph)) # => ['root', 'a', 'b', 'd', 'e', 'c']
print(ll)


rv = nx.is_directed(graph) # => True
print('is_directed', rv)

rv = nx.is_directed_acyclic_graph(graph) # => True
print('is_directed_acyclic', rv)


print('#'*50)
graph = nx.DiGraph()
graph.add_edges_from([(1, 2), (2, 3), (3, 4), (4, 1)])
print(graph.nodes())
nx.is_directed(graph) # => True
rv = nx.is_directed_acyclic_graph(graph) # => False
print('is_directed_acyclic', rv)


graph = nx.Graph()
graph.add_edges_from([('x', 'y'), ('y', 'z')])
rv = nx.is_directed(graph) # => False
print(rv)
rv = nx.is_directed_acyclic_graph(graph) # => False
print(rv)

def test_plot():
    from matplotlib import pyplot as plt
    g1 = nx.DiGraph()
    g1.add_edges_from([("root", "a"), ("a", "b"), ("a", "e"), ("b", "c"), ("b", "d"), ("d", "e")])
    plt.tight_layout()
    nx.draw_networkx(g1, arrows=True)
    plt.savefig("g1.png", format="PNG")
    # tell matplotlib you're done with the plot: https://stackoverflow.com/questions/741877/how-do-i-tell-matplotlib-that-i-am-done-with-a-plot
    plt.show()
    #plt.clf()


test_plot()
