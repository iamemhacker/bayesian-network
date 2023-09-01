import json
import argparse
from typing import Dict, Any


def __ommit_edges(entry: Dict[str, Any], threshold: float) -> Dict[str, Any]:
    # print("Before:")
    # print(entry)
    edges = filter(lambda p: p[1] >= threshold, zip(entry["_4"], entry["_5"]))
    # print(", ".join([str(e) for e in edges]))
    ret = entry.copy()
    ret["_4"] = []
    ret["_5"] = []
    for (e, s) in edges:
        ret["_4"].append(e)
        ret["_5"].append(s)
    # print("After:")
    # print(ret)
    
    return ret


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Edge Ommitter",
        description="Ommits graph edges by a given threshold")

    parser.add_argument("-i", "--input-file", required=True)
    parser.add_argument("-t", "--threshold", type=float, required=True)
    parser.add_argument("-o", "--output-file", required=True)
    args = parser.parse_args()

    with open(args.input_file, "r") as f_json:
        lines = [line for line in f_json.readlines() if line]
        print('\n'.join(lines))
        entries = [json.loads(line) for line in lines]
        modified_entries = map(lambda e: __ommit_edges(e, args.threshold),
                               entries)
    with open(args.output_file, "w") as f_out:
        for e in modified_entries:
            f_out.write(f"{json.dumps(e)}\n")

