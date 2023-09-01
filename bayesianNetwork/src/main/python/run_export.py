import json
import glob
import argparse
from typing import NamedTuple, List, Dict, Any

class Entry(NamedTuple):
    node_id: int
    degrees: int
    name: str
    parents: List[int]
    scores: List[float]


def __to_entry(d: Dict[str, Any]):
    return Entry(node_id=d["_1"],
                 degrees=d["_2"],
                 name=d["_3"],
                 parents=d["_4"],
                 scores=d["_5"])


def run_export(input_path: str, output_path: str, edge_threshold: float) \
        -> None:
    out_f = open(output_path, "w+")
    out_f.write("digraph BayesianNetwork {\n")

    with open(input_path, "r") as input_f:
        entries = [__to_entry(json.loads(line)) for line in input_f.readlines()]

    nodes = [(entry.node_id, entry.name) for entry in entries]

    def _find(nid: int) -> str:
        return list(filter(lambda p: p[0] == nid, nodes))[0][1]

    for node in nodes:
        out_f.write(f"{node[1]} [label={node[1]}];")

    for entry in entries:
        for (idx, pid) in enumerate(entry.parents):
            if entry.scores[idx] < edge_threshold:
                continue
            s_score = "{:.2f}".format(entry.scores[idx])
            out_f.write(
                f'{_find(pid)} -> {entry.name} [label="{s_score}"];\n')

    out_f.write("}")
    out_f.flush()
    out_f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog = "Graph export",
                    description = "Convert graph from json format to dot")
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("-t", "--edge-threshold", required=True, type=float)
    args = parser.parse_args()
    print(f"Exporting DOT file from {args.input} to {args.output}")
    run_export(args.input, args.output, args.edge_threshold)
