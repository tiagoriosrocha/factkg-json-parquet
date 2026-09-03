from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


PROJECT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_DIR = PROJECT_DIR / "data" / "processed" / "json"
DEFAULT_OUTPUT_DIR = PROJECT_DIR / "data" / "processed" / "parquet"
GRAPH_SIDES = (
    ("claim", "grafo_claim"),
    ("evidencia", "grafo_evidencia"),
)
UNKNOWN_SPLIT = "unknown"

GRAPH_SCHEMA = pa.schema(
    [
        ("graph_id", pa.string()),
        ("json_file", pa.string()),
        ("row_number", pa.int64()),
        ("id", pa.string()),
        ("claim", pa.string()),
        ("evidencia", pa.string()),
        ("label", pa.string()),
        ("split", pa.string()),
        ("claim_num_nodes", pa.int64()),
        ("claim_num_edges", pa.int64()),
        ("evidencia_num_nodes", pa.int64()),
        ("evidencia_num_edges", pa.int64()),
    ]
)

NODE_SCHEMA = pa.schema(
    [
        ("graph_id", pa.string()),
        ("json_file", pa.string()),
        ("row_number", pa.int64()),
        ("id", pa.string()),
        ("label", pa.string()),
        ("split", pa.string()),
        ("graph_side", pa.string()),
        ("node_id", pa.int64()),
        ("node_original_id", pa.string()),
        ("node_type", pa.string()),
        ("text", pa.string()),
    ]
)

EDGE_SCHEMA = pa.schema(
    [
        ("graph_id", pa.string()),
        ("json_file", pa.string()),
        ("row_number", pa.int64()),
        ("id", pa.string()),
        ("label", pa.string()),
        ("split", pa.string()),
        ("graph_side", pa.string()),
        ("edge_id", pa.int64()),
        ("source", pa.int64()),
        ("target", pa.int64()),
        ("edge_type", pa.string()),
    ]
)


@dataclass
class ExportCounters:
    files: int = 0
    graphs: int = 0
    nodes: int = 0
    edges: int = 0
    invalid_nodes: int = 0
    invalid_edges: int = 0
    unknown_split: int = 0
    split_from_json: int = 0
    split_counts: Counter[str] | None = None

    def __post_init__(self) -> None:
        if self.split_counts is None:
            self.split_counts = Counter()


class ParquetWriters:
    def __init__(self, output_dir: Path, compression: str | None) -> None:
        self.output_dir = output_dir
        self.compression = compression
        self.schemas = {
            "graphs": GRAPH_SCHEMA,
            "nodes": NODE_SCHEMA,
            "edges": EDGE_SCHEMA,
        }
        self.paths = {
            "graphs": output_dir / "graphs.parquet",
            "nodes": output_dir / "nodes.parquet",
            "edges": output_dir / "edges.parquet",
        }
        self.writers: dict[str, pq.ParquetWriter] = {}
        self.row_counts = Counter()

    def __enter__(self) -> "ParquetWriters":
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for name, schema in self.schemas.items():
            self.writers[name] = pq.ParquetWriter(
                self.paths[name],
                schema=schema,
                compression=self.compression,
            )
        return self

    def write(self, name: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        table = pa.Table.from_pylist(rows, schema=self.schemas[name])
        self.writers[name].write_table(table)
        self.row_counts[name] += table.num_rows

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            if exc_type is None:
                for name, writer in self.writers.items():
                    if self.row_counts[name] == 0:
                        empty_table = pa.Table.from_pylist([], schema=self.schemas[name])
                        writer.write_table(empty_table)
        finally:
            for writer in self.writers.values():
                writer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exporta os JSONs de grafos FactKG para tabelas Parquet no esquema FEVER."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Pasta com os JSONs gerados. Padrao: data/processed/json/",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Pasta onde os Parquets serao gravados. Padrao: data/processed/parquet/",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Quantidade de JSONs processados por row group. Padrao: 1000.",
    )
    parser.add_argument(
        "--compression",
        default="zstd",
        choices=["zstd", "snappy", "gzip", "brotli", "none"],
        help="Compressao dos Parquets. Padrao: zstd.",
    )
    parser.add_argument(
        "--fail-on-unknown-split",
        action="store_true",
        help="Falha se algum JSON nao possuir o campo split.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        help="Mostra progresso a cada N JSONs. Use 0 para silenciar. Padrao: 1000.",
    )
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def normalize_id(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def normalize_split(value: Any) -> str:
    split = normalize_text(value).strip().lower()
    if split == "validation":
        return "val"
    return split


def resolve_project_path(path: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (PROJECT_DIR / path).resolve()


def parse_row_number(json_file: Path) -> int | None:
    prefix = json_file.stem.split("_", 1)[0]
    try:
        return int(prefix)
    except ValueError:
        return None


def parse_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def list_json_files(input_dir: Path) -> list[Path]:
    if not input_dir.exists():
        raise FileNotFoundError(f"Pasta de JSONs nao encontrada: {input_dir}")
    json_files = sorted(input_dir.rglob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"Nenhum JSON encontrado em {input_dir}")
    return json_files


def load_json(json_file: Path) -> dict[str, Any]:
    with json_file.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        raise ValueError(f"JSON precisa conter um objeto no topo: {json_file}")
    return payload


def graph_items(payload: dict[str, Any], graph_key: str) -> tuple[list[Any], list[Any]]:
    graph = payload.get(graph_key)
    if not isinstance(graph, dict):
        return [], []

    nodes = graph.get("nodes") or []
    edges = graph.get("edges") or []
    if not isinstance(nodes, list):
        nodes = []
    if not isinstance(edges, list):
        edges = []
    return nodes, edges


def append_graph_side_rows(
    *,
    payload: dict[str, Any],
    graph_side: str,
    graph_key: str,
    context: dict[str, Any],
    node_rows: list[dict[str, Any]],
    edge_rows: list[dict[str, Any]],
    counters: ExportCounters,
) -> tuple[int, int]:
    nodes, edges = graph_items(payload, graph_key)
    valid_node_count = 0
    valid_edge_count = 0

    for node_index, node in enumerate(nodes):
        if not isinstance(node, dict):
            counters.invalid_nodes += 1
            continue

        node_id = parse_int(node.get("id"))
        if node_id is None:
            node_id = node_index

        node_rows.append(
            {
                **context,
                "graph_side": graph_side,
                "node_id": node_id,
                "node_original_id": normalize_text(node.get("original_id")),
                "node_type": normalize_text(node.get("type")),
                "text": normalize_text(node.get("text")),
            }
        )
        valid_node_count += 1

    for edge_index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            counters.invalid_edges += 1
            continue

        source = parse_int(edge.get("source"))
        target = parse_int(edge.get("target"))
        if source is None or target is None:
            counters.invalid_edges += 1
            continue

        edge_rows.append(
            {
                **context,
                "graph_side": graph_side,
                "edge_id": edge_index,
                "source": source,
                "target": target,
                "edge_type": normalize_text(edge.get("type")),
            }
        )
        valid_edge_count += 1

    return valid_node_count, valid_edge_count


def split_for_payload(
    *,
    json_file: Path,
    payload: dict[str, Any],
    counters: ExportCounters,
) -> str:
    split = normalize_split(payload.get("split"))
    if split:
        counters.split_from_json += 1
        return split

    counters.unknown_split += 1
    logging.debug("Split nao encontrado para %s", json_file.name)
    return UNKNOWN_SPLIT


def make_context(
    json_file: Path,
    payload: dict[str, Any],
    counters: ExportCounters,
) -> dict[str, Any]:
    row_number = parse_row_number(json_file)
    item_id = normalize_id(payload.get("id"))
    split = split_for_payload(
        json_file=json_file,
        payload=payload,
        counters=counters,
    )

    return {
        "graph_id": json_file.stem,
        "json_file": json_file.name,
        "row_number": row_number,
        "id": item_id,
        "label": normalize_text(payload.get("label")),
        "split": split,
    }


def export_batch(
    json_files: list[Path],
    writers: ParquetWriters,
    counters: ExportCounters,
) -> None:
    graph_rows: list[dict[str, Any]] = []
    node_rows: list[dict[str, Any]] = []
    edge_rows: list[dict[str, Any]] = []

    for json_file in json_files:
        payload = load_json(json_file)
        context = make_context(json_file, payload, counters)
        split = context["split"]
        assert counters.split_counts is not None
        counters.split_counts[split] += 1

        side_counts: dict[str, tuple[int, int]] = {}
        for graph_side, graph_key in GRAPH_SIDES:
            side_counts[graph_side] = append_graph_side_rows(
                payload=payload,
                graph_side=graph_side,
                graph_key=graph_key,
                context=context,
                node_rows=node_rows,
                edge_rows=edge_rows,
                counters=counters,
            )

        claim_counts = side_counts["claim"]
        evidencia_counts = side_counts["evidencia"]
        graph_rows.append(
            {
                **context,
                "claim": normalize_text(payload.get("claim")),
                "evidencia": normalize_text(payload.get("evidencia")),
                "claim_num_nodes": claim_counts[0],
                "claim_num_edges": claim_counts[1],
                "evidencia_num_nodes": evidencia_counts[0],
                "evidencia_num_edges": evidencia_counts[1],
            }
        )

    writers.write("graphs", graph_rows)
    writers.write("nodes", node_rows)
    writers.write("edges", edge_rows)

    counters.files += len(json_files)
    counters.graphs += len(graph_rows)
    counters.nodes += len(node_rows)
    counters.edges += len(edge_rows)


def batched(values: list[Path], batch_size: int) -> list[list[Path]]:
    if batch_size < 1:
        raise ValueError("--batch-size precisa ser maior que zero.")
    return [values[index : index + batch_size] for index in range(0, len(values), batch_size)]


def main() -> int:
    configure_logging()
    args = parse_args()
    compression = None if args.compression == "none" else args.compression

    input_dir = resolve_project_path(args.input_dir)
    output_dir = resolve_project_path(args.output_dir)
    json_files = list_json_files(input_dir)

    counters = ExportCounters()
    logging.info("Exportando %s JSONs de %s", len(json_files), input_dir)

    with ParquetWriters(output_dir, compression=compression) as writers:
        for batch_index, batch in enumerate(batched(json_files, args.batch_size), start=1):
            export_batch(batch, writers, counters)
            if args.progress_every and counters.files % args.progress_every == 0:
                logging.info(
                    "Processados %s JSONs em %s lotes",
                    counters.files,
                    batch_index,
                )

    if args.fail_on_unknown_split and counters.unknown_split:
        raise ValueError(f"{counters.unknown_split} JSONs ficaram com split=unknown.")

    logging.info(
        "Parquets gerados em %s: graphs=%s linhas, nodes=%s linhas, edges=%s linhas",
        output_dir,
        counters.graphs,
        counters.nodes,
        counters.edges,
    )
    logging.info("Distribuicao split: %s", dict(sorted(counters.split_counts.items())))
    logging.info(
        "Origem do split: json=%s, unknown=%s",
        counters.split_from_json,
        counters.unknown_split,
    )
    if counters.invalid_nodes or counters.invalid_edges:
        logging.warning(
            "Itens invalidos ignorados: nodes=%s, edges=%s",
            counters.invalid_nodes,
            counters.invalid_edges,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
