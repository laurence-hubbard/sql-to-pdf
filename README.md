# sql-to-pdf

Aim: To convert SQL into a visual diagram of data flow in PDF format.

## Supported outputs

- `pdf` (default, via Graphviz)
- `dot` (raw Graphviz DOT)
- `mermaid` (Mermaid flowchart)

## Usage

```bash
./sql-to-pdf.sh path/to/file.sql
./sql-to-pdf.sh path/to/file.sql mermaid
./sql-to-pdf.sh path/to/file.sql dot
```

Mermaid output is written to `./target/mermaid/<filename>.mmd`.
