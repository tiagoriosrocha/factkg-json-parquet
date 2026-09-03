# FactKG → grafos FEVER e Parquet

Utilitários em Python para transformar o dataset **FactKG** em registros JSON
compatíveis com uma estrutura de grafo inspirada no FEVER e exportá-los para
tabelas Parquet.

## Fluxo

1. `scripts/convert_factkg_to_fever_graph.py` lê um split do arquivo `factkg.zip` e gera um JSON por afirmação.
2. `scripts/export_parquet.py` lê esses JSONs e cria três tabelas: `graphs`, `nodes` e `edges`.

Os caminhos de evidência do FactKG descrevem relações do DBpedia; o projeto não inventa trechos de texto de evidência. Por isso, o grafo de evidência representa o padrão de relações disponibilizado pelo dataset.

## Requisitos

- Python 3.10 ou mais recente
- `pyarrow`
- O arquivo de distribuição do FactKG em `data/raw/factkg.zip`, contendo arquivos como `factkg_train.pickle` e `factkg_test.pickle`

Instale a dependência:

```bash
python3 -m pip install -r requirements.txt
```

## Gerar JSONs de grafo

Crie registros a partir de um split do FactKG. O exemplo abaixo exporta os primeiros 10.000 registros de treino:

```bash
python3 scripts/convert_factkg_to_fever_graph.py \
  --split train \
  --output-dir data/processed/json/factkg_train \
  --limit 10000
```

Para exportar todo o split, omita `--limit`. Os splits aceitos são `train`, `dev` e `test`. Os arquivos são nomeados com uma sequência numérica para evitar sobrescrever arquivos de execuções anteriores no mesmo diretório.

Cada JSON contém, entre outros, os campos `claim`, `evidencia`, `label`, `grafo_claim`, `grafo_evidencia` e `split`.

## Exportar Parquet

Converta os JSONs gerados em três arquivos Parquet:

```bash
python3 scripts/export_parquet.py \
  --input-dir data/processed/json/factkg_train \
  --output-dir data/processed/parquet/factkg_train
```

Opções úteis:

```bash
# Usar compressão Snappy e lotes de 5.000 arquivos
python3 scripts/export_parquet.py \
  --input-dir data/processed/json/factkg_train \
  --output-dir data/processed/parquet/factkg_train \
  --compression snappy \
  --batch-size 5000

# Falhar se algum JSON não tiver o campo split
python3 scripts/export_parquet.py \
  --input-dir data/processed/json/factkg_train \
  --fail-on-unknown-split
```

O resultado possui esta organização:

| Arquivo          | Conteúdo                                                                     |
| ---------------- | ---------------------------------------------------------------------------- |
| `graphs.parquet` | Uma linha por registro, com texto, rótulo, split e contagens de nós/arestas. |
| `nodes.parquet`  | Nós de `claim` e `evidencia`, com tipo, texto e ID original.                 |
| `edges.parquet`  | Arestas de `claim` e `evidencia`, com origem, destino e tipo de relação.     |

## Estrutura do repositório

```text
scripts/                          # Executáveis do pipeline
data/raw/                         # FactKG e outros insumos grandes
data/processed/json/              # JSONs de grafo gerados (não versionados)
data/processed/parquet/           # Parquets gerados (não versionados)
docs/references/                  # Artigos e material de referência
docs/notes/                       # Notas e registros de execução
```

## Dados grandes

Arquivos de dados, caches Python e saídas geradas são ignorados pelo Git. Cada pessoa que executar o pipeline deve obter o FactKG de sua fonte oficial e gerar localmente os artefatos necessários.
