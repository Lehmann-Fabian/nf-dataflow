# Nextflow nf-dataflow Plugin – Understand Your Pipeline's Dataflow

This plugin helps you analyze and visualize the data flow within your Nextflow pipeline.

## Features
- **Visual Representation**: Generates a DAG to illustrate the data flow between processes.
- **Detailed Analysis**: Captures inputs and outputs of each process for in-depth examination.
- **Pipeline Overview**: Summarizes data volume processed per process and dependencies.

## Installation
No installation required. Simply add the plugin to your Nextflow config:

```nextflow
plugins {
   id 'nf-dataflow'
}
```

## Usage

### 1. Visualizing the Dataflow
Specify an output file in your Nextflow config:

```nextflow
dataflow.plot.file = "physicalDag.dot"
```

This generates a `physicalDag.dot` file in your working directory.  
Supported formats: dot and all Graphviz formats.
Graphviz is required to visualize formats other than `dot`.

Example visualization created with Graphviz, details activated:  
![Dataflow](ExampleDagDetails.svg)

Example visualization created with Graphviz, details deactivated (recommended for large DAGs), no legend, and filtering MULTIQC:
![Dataflow](ExampleDagNoDetails.svg)
Additional details are available on mouse hover if you view it on [full size](https://raw.githubusercontent.com/Lehmann-Fabian/nf-dataflow/refs/heads/master/ExampleDagNoDetails.svg).

The following options control the DAG’s rendering:
Options are set in the **`dataflow.plot`** block in your Nextflow config.


| Option         | Default Value | Description                                                                                                          |
|----------------|---------------|----------------------------------------------------------------------------------------------------------------------|
| **`file`**     | ---           | The path where the DAG will be stored.                                                                               |
| **`rankdir`**  | `TB`          | Direction of the graph layout. Options: `TB` (top to bottom), `LR` (left to right).                                  |
| **`detailed`** | `false`       | If `true`, shows detailed information about each process in the DAG. Otherwise only on mouse over.                   |
| **`external`** | `true`        | If `true`, shows external inputs in the DAG.                                                                         |
| **`legend`**   | `true`        | If `true`, shows a legend in the DAG.                                                                                |
| **`cluster`**  | `false`       | If `true`, clusters processes by their tag.                                                                          |
| **`tagNames`** | `true`        | If `true`, shows tag name in the cluster. Only takes effect if cluster is set to `true`.                             |
| **`filter`**   | `[]`          | List of regex filters. If one regex matches a process name task instances of this process will **not** be displayed. |

Additionally, you can set a path to write the physical DAG as a `JSON` with the following options:

```nextflow
dataflow.persist = "dag.json"
```
This file can be used for further analyses.

### 2. Analyzing Process Inputs & Outputs
Create CSV files to track input and output files:

```nextflow
dataflow {
    input = "input.csv"
    output = "output.csv"
}
```

Generated files:
- `input.csv`
- `output.csv`

#### Columns:
| Column    | Description                       |
|-----------|-----------------------------------|
| **name**  | Process instance name             |
| **hash**  | Process hash (matches trace file) |
| **path**  | Input/output file path            |
| **type**  | `f` (file) or `d` (directory)     |
| **size**  | Size of the file/directory        |

### 3. Analyzing Task Dependencies
Generate a summary file with process dependencies and data volume:

```nextflow
dataflow {
    summary = "summary.csv"
}
```

#### Summary Columns:
| Column         | Description                       |
|----------------|-----------------------------------|
| **task**       | Process instance name             |
| **hash**       | Process hash (matches trace file) |
| **inputs**     | Number of inputs                  |
| **inputSize**  | Total input size                  |
| **outputs**    | Number of outputs                 |
| **outputSize** | Total output size                 |
| **usedBy**     | Number of dependent processes     |

### 4. Additional Options
- **`overwrite`**: If `true`, overwrites existing output files (default: `false`).
