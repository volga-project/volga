
def print_digraph(dg, name='example.png'):
    dg.layout(prog="dot")
    dg.draw(name, format='png')