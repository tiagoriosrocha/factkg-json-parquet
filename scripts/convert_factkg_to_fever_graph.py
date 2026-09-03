#!/usr/bin/env python3
"""Export FactKG records to a FEVER-graph-like JSON representation.

FactKG evidence is a relation-query pattern, rather than a natural-language
evidence passage.  Consequently, ``evidencia`` and ``grafo_evidencia`` emitted
by this script describe that pattern; they are not fabricated Wikipedia text.
"""

from __future__ import annotations

import argparse
import json
import pickle
import re
import uuid
import zipfile
from pathlib import Path
from typing import Any


NAMESPACE = uuid.UUID("f6aa891d-6138-4bce-a8d8-2bf3e9546723")
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def readable(value: str) -> str:
    """Turn a DBpedia identifier into a display label without changing IDs."""
    return value.strip('"').replace("_", " ")


def stable_id(record_key: str, role: str) -> str:
    return str(uuid.uuid5(NAMESPACE, f"{record_key}:{role}"))


def normalise_paths(value: Any) -> list[list[str]]:
    """FactKG stores every entity's paths as a list of relation lists."""
    if not isinstance(value, list):
        raise ValueError(f"Unexpected Evidence value: {value!r}")
    if not value:
        return []
    if all(isinstance(item, str) for item in value):
        return [value]
    if all(isinstance(item, list) and all(isinstance(r, str) for r in item) for item in value):
        return value
    raise ValueError(f"Unexpected Evidence path: {value!r}")


def make_graph(record_key: str, claim: str, entities: list[str], evidence: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    """Create a graph from FactKG's entity set and relation paths.

    A path endpoint is deliberately represented as an anonymous node because
    FactKG's Evidence field supplies relation paths, not the DBpedia triples
    or an unambiguous endpoint for each individual path.
    """
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    entity_nodes: dict[str, int] = {}

    def add_node(text: str, node_type: str, role: str) -> int:
        node_id = len(nodes)
        nodes.append({"id": node_id, "original_id": stable_id(record_key, role), "text": text, "type": node_type})
        return node_id

    for position, entity in enumerate(entities):
        node_id = add_node(readable(entity), "Entity", f"entity:{entity}:{position}")
        entity_nodes.setdefault(entity, node_id)

    for entity, raw_paths in (evidence or {}).items():
        source = entity_nodes.get(entity)
        if source is None:
            source = add_node(readable(entity), "Entity", f"evidence-entity:{entity}")
        for path_index, relations in enumerate(normalise_paths(raw_paths)):
            current = source
            for hop, relation in enumerate(relations):
                endpoint = add_node(
                    f"unknown endpoint ({path_index + 1}.{hop + 1})",
                    "EvidenceVariable",
                    f"path:{entity}:{path_index}:{hop}",
                )
                edges.append({"source": current, "target": endpoint, "type": relation})
                current = endpoint
    return {"nodes": nodes, "edges": edges}


def evidence_text(evidence: dict[str, Any] | None) -> str:
    if not evidence:
        return "FactKG test split: no evidence pattern is released for this claim."
    pieces = []
    for entity, raw_paths in evidence.items():
        paths = [" -> ".join(path) for path in normalise_paths(raw_paths)]
        pieces.append(f"{readable(entity)}: " + " | ".join(paths))
    return "FactKG DBpedia relation pattern: " + "; ".join(pieces)


def export_record(index: int, split: str, claim: str, record: dict[str, Any]) -> dict[str, Any]:
    evidence = record.get("Evidence")
    key = f"{split}:{index}:{claim}"
    graph = make_graph(key, claim, record["Entity_set"], evidence)
    label = "SUPPORTS" if record["Label"][0] is True else "REFUTES"
    split = "test" if split == "test" or split == "dev" else "train"  # FactKG has no dev split
    return {
        "id": str(index),
        "split": split,
        "claim": claim,
        "evidencia": evidence_text(evidence),
        "label": label,
        "grafo_claim": graph,
        # In FactKG the same released relation pattern is the only evidence
        # available; unlike FEVER, there is no separate evidence passage.
        "grafo_evidencia": graph,
        "factkg": {
            "label_original": record["Label"][0],
            "entity_set": record["Entity_set"],
            "evidence_pattern": evidence,
            "types": record.get("types", []),
        },
    }


def load_split(archive: Path, split: str) -> dict[str, dict[str, Any]]:
    filename = f"factkg_{split}.pickle"
    with zipfile.ZipFile(archive) as zf:
        if filename not in zf.namelist():
            raise FileNotFoundError(f"{filename} was not found in {archive}")
        return pickle.loads(zf.read(filename))


def next_file_number(output_dir: Path) -> int:
    """Return the next sequence number from JSON files already in output_dir.

    Both ``0002172.json`` and FEVER-style names such as
    ``0002172_71351.json`` contribute their leading numeric prefix.
    """
    largest = -1
    for path in output_dir.glob("*.json"):
        match = re.match(r"^(\d+)", path.stem)
        if match:
            largest = max(largest, int(match.group(1)))
    return largest + 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw" / "factkg.zip",
    )
    parser.add_argument("--split", choices=("train", "dev", "test"), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--limit", type=int, help="Export only the first N records (useful for validation).")
    args = parser.parse_args()

    records = load_split(args.input, args.split)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    selected = list(records.items())[: args.limit]
    first_file_number = next_file_number(args.output_dir)
    for index, (claim, record) in enumerate(selected):
        result = export_record(index, args.split, claim, record)
        file_number = first_file_number + index
        (args.output_dir / f"{file_number:07d}_{index}.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    print(
        f"Exported {len(selected)} {args.split} records to {args.output_dir} "
        f"(file sequence starts at {first_file_number:07d})"
    )


if __name__ == "__main__":
    main()
