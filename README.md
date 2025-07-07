# Large Dataset Acquisition Workflow Template (LDAWT)

A Python-based distributed parallel workflow system for downloading and processing large datasets using the TaskVine framework. LDAWT enables efficient acquisition of large-scale datasets across multiple machines with comprehensive monitoring, error handling, and optimization capabilities.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Scripts](#core-scripts)
  - [Dataset Management](#dataset-management)
  - [Image Downloading](#image-downloading)
  - [TaskVine Workflows](#taskvine-workflows)
  - [Monitoring and Utilities](#monitoring-and-utilities)
  - [HPC Integration](#hpc-integration)
  - [Data Upload](#data-upload)
- [Configuration Files](#configuration-files)
- [Jupyter Notebooks](#jupyter-notebooks)
- [Usage Examples](#usage-examples)
- [Performance Optimization](#performance-optimization)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

## Overview

LDAWT (Large Dataset Acquisition Workflow Template) is designed to handle the challenges of downloading large-scale datasets efficiently. It uses a manager-worker paradigm powered by the TaskVine framework, enabling:

- **Distributed Parallel Processing**: Leverage multiple machines for concurrent downloads
- **Fault Tolerance**: Comprehensive error handling and retry mechanisms
- **Resource Optimization**: Intelligent resource allocation and monitoring
- **Scalability**: Support for cloud environments, HPC clusters, and local machines
- **Monitoring**: Real-time performance tracking and system monitoring

The system partitions large datasets into manageable chunks, distributes download tasks across worker machines, and consolidates results on the manager machine, achieving significantly faster acquisition speeds than single-machine approaches.

## Prerequisites

- **Operating System**: Linux, macOS, or Windows with WSL
- **Python**: 3.10 or higher
- **Conda**: Package and environment management
- **Network**: Stable internet connection for distributed operations

## Installation

1. **Clone the Repository**
   ```bash
   git clone https://github.com/zkdeng-uofa/LDAWT
   cd LDAWT
   ```

2. **Create Conda Environment**
   ```bash
   conda env create -f environment.yml
   conda activate LDAWT
   ```

3. **Verify Installation**
   ```bash
   python bin/TaskvineMonitor.py --help
   ```

## Quick Start

### Basic Workflow

1. **Prepare Data**: Split your dataset into parquet files
   ```bash
   python bin/SplitParquet.py --parquet data.parquet --grouping_col species_name --groups 10 --output_folder split_data
   ```

2. **Start Monitor** (recommended)
   ```bash
   python bin/TaskvineMonitor.py --port 9124 --interval 10
   ```

3. **Launch Manager**
   ```bash
   python bin/TaskvineLDAWTCloud.py --config_file config.json
   ```

4. **Start Workers** (on separate machines)
   ```bash
   python bin/StartWorker.py --manager_host MANAGER_IP --manager_port 9124
   ```

## Core Scripts

LDAWT provides a comprehensive suite of Python scripts organized by functionality:

### Dataset Management

#### bin/SplitParquet.py
Intelligently partitions large parquet files into balanced groups for distributed processing.

**Purpose**: Creates balanced data splits based on grouping columns to ensure even workload distribution across workers.

**Key Features**:
- Greedy grouping algorithm for load balancing
- Configurable number of output groups
- Preserves data relationships within groups

**Usage**:
```bash
python bin/SplitParquet.py \
    --parquet input.parquet \
    --grouping_col species_name \
    --groups 20 \
    --output_folder split_output
```

**Parameters**:
- --parquet: Input parquet file path
- --grouping_col: Column name to group data by
- --groups: Number of output groups to create
- --output_folder: Directory for output parquet files

#### bin/CalcDatasetSize.py
Estimates total storage requirements for image datasets by analyzing URL headers.

**Purpose**: Provides size estimates for planning storage and bandwidth requirements before downloading.

**Key Features**:
- Asynchronous URL analysis for speed
- Multiple HTTP method fallbacks (HEAD, GET with range)
- Handles various image formats and missing extensions
- Detailed reporting of unresolvable URLs

**Usage**:
```bash
python bin/CalcDatasetSize.py \
    --directory data/parquet_files \
    --url_column photo_url
```

### Image Downloading

#### bin/ImgDownload.py
Basic asynchronous image downloader with format detection and retry mechanisms.

**Purpose**: Downloads images from URLs with automatic format detection and error handling.

**Key Features**:
- Asynchronous downloading for performance
- Automatic file extension detection
- MIME type-based format determination
- Fallback extension retry mechanism
- Organized output by class/category

**Usage**:
```bash
python bin/ImgDownload.py \
    --input_path dataset.parquet \
    --output_tar images.tar.gz \
    --url_name photo_url \
    --class_name species_name
```

#### bin/ImgDownloadBW.py
Enhanced image downloader with bandwidth monitoring, size limits, and advanced error handling.

**Purpose**: Production-ready image downloading with comprehensive monitoring and optimization.

**Key Features**:
- Bandwidth tracking and monitoring
- File size limits and validation
- Graceful shutdown handling (SIGINT/SIGTERM)
- Configurable timeout and concurrency
- Detailed progress reporting

**Usage**:
```bash
python bin/ImgDownloadBW.py \
    --input_path dataset.parquet \
    --output_tar images.tar.gz \
    --url_name photo_url \
    --class_name species_name \
    --concurrent_downloads 50 \
    --timeout 30 \
    --max_file_size 52428800
```

#### bin/ImgReconstruct.py
Extracts and reconstructs images from parquet files containing binary image data.

**Purpose**: Converts stored binary image data back to standard image formats.

**Key Features**:
- Supports multiple output formats (PNG, JPG, etc.)
- Handles nested binary data structures
- Organizes output by labels/classes
- Progress tracking with error reporting

**Usage**:
```bash
python bin/ImgReconstruct.py \
    --input_path images.parquet \
    --output_tar reconstructed.tar.gz \
    --image_col image_data \
    --label_col class_label \
    --image_format png
```

### TaskVine Workflows

#### bin/TaskvineLDAWT.py
Basic TaskVine manager for distributed image downloading workflows.

**Purpose**: Orchestrates basic distributed downloading across multiple worker machines.

**Key Features**:
- TaskVine integration for distributed processing
- Automatic task submission for parquet files
- Input/output file management
- Basic task monitoring

**Usage**:
```bash
python bin/TaskvineLDAWT.py --config_file config.json
```

#### bin/TaskvineLDAWTCloud.py
Enhanced cloud-optimized TaskVine manager with comprehensive monitoring and error handling.

**Purpose**: Production-ready distributed workflow manager with advanced features for cloud and HPC environments.

**Key Features**:
- Enhanced error handling and retry logic
- Resource requirement specification
- Detailed task monitoring and reporting
- Configurable timeout and retry settings
- Cloud storage integration (CyVerse upload)

**Usage**:
```bash
python bin/TaskvineLDAWTCloud.py --config_file taskvineCloud.json
```

**Configuration Example**:
```json
{
    "port_number": 9124,
    "parquets_directory": "examples/2split",
    "class_col": "species_name",
    "url_col": "photo_url",
    "timeout_minutes": 30,
    "max_retries": 3
}
```

### Monitoring and Utilities

#### bin/TaskvineMonitor.py
Real-time system and TaskVine performance monitoring tool.

**Purpose**: Provides comprehensive monitoring of system resources and TaskVine workflow performance.

**Key Features**:
- Real-time CPU, memory, and disk monitoring
- TaskVine port status checking
- Network I/O tracking
- File descriptor monitoring
- Alert system for resource thresholds

**Usage**:
```bash
python bin/TaskvineMonitor.py \
    --port 9124 \
    --log_file monitor.log \
    --interval 10 \
    --disk_path /data/storage
```

#### bin/StartWorker.py
Optimized TaskVine worker startup with automatic resource detection.

**Purpose**: Simplifies worker deployment with intelligent resource allocation and monitoring.

**Key Features**:
- Automatic system resource detection
- Optimal CPU, memory, and disk allocation
- Configurable resource overrides
- Worker health monitoring

**Usage**:
```bash
python bin/StartWorker.py \
    --manager_host 192.168.1.100 \
    --manager_port 9124 \
    --cores 8 \
    --memory 16000 \
    --disk 50000
```

#### bin/compare_files.py
Utility for comparing file listings between cloud storage and local systems.

**Purpose**: Helps synchronize and verify file transfers between different storage systems.

**Usage**:
```bash
python bin/compare_files.py
```

**Input/Output**: Compares new_insects_cloud.txt vs new_insects_local.txt and generates difference reports.

### HPC Integration

#### bin/MakeTaskvineSlurm.py
Generates SLURM batch scripts for HPC cluster integration.

**Purpose**: Facilitates deployment of TaskVine workers on SLURM-managed HPC clusters.

**Key Features**:
- Automatic IP address detection
- SLURM script generation
- Configurable resource requests

**Usage**:
```bash
python bin/MakeTaskvineSlurm.py
```

### Data Upload

#### bin/upload_to_cyverse.py
Parallel file upload utility for CyVerse data storage.

**Purpose**: Efficiently uploads large datasets to CyVerse data store using parallel processing.

**Key Features**:
- Parallel upload processing (25 concurrent by default)
- Progress tracking and reporting
- Error handling and retry logic
- File existence verification

**Usage**:
```bash
python bin/upload_to_cyverse.py
```

## Configuration Files

### environment.yml
Conda environment specification with all required dependencies including:
- Python 3.10.8
- TaskVine (ndcctools=7.14.3)
- Data processing: pandas, pyarrow, numpy
- Async operations: aiohttp
- System monitoring: psutil
- Progress tracking: tqdm
- Cloud integration: gocommands

## Jupyter Notebooks

### jupyter/LDAWT.ipynb
Main workflow demonstration notebook showcasing complete LDAWT usage including environment setup, dataset size calculation, TaskVine workflow execution, and performance monitoring.

### jupyter/LDAWTest.ipynb
Testing and example notebook for development and validation with quick testing workflows, configuration validation, and small-scale dataset examples.

## Usage Examples

### Complete Workflow Example

1. **Split Dataset**
   ```bash
   python bin/SplitParquet.py --parquet large_dataset.parquet --grouping_col species_name --groups 50 --output_folder split_data
   ```

2. **Estimate Storage**
   ```bash
   python bin/CalcDatasetSize.py --directory split_data --url_column photo_url
   ```

3. **Start Monitoring**
   ```bash
   python bin/TaskvineMonitor.py --port 9124 --interval 10 --log_file workflow.log
   ```

4. **Launch Manager**
   ```bash
   python bin/TaskvineLDAWTCloud.py --config_file config.json
   ```

5. **Deploy Workers** (on multiple machines)
   ```bash
   python bin/StartWorker.py --manager_host MANAGER_IP --manager_port 9124
   ```

## Performance Optimization

### Best Practices
- Use moderate concurrency (50-100 downloads per worker)
- Monitor system resources with TaskvineMonitor
- Configure appropriate timeouts based on file sizes
- Deploy multiple workers for horizontal scaling
- Regular cleanup of temporary files

For detailed optimization guidance, see [OPTIMIZATION_GUIDE.md](OPTIMIZATION_GUIDE.md).

## Troubleshooting

### Common Issues

**Tasks Stop After Limited Executions**: Use optimized scripts with proper resource management

**Workers Disconnect**: Use StartWorker.py for auto-resource detection

**High Memory Usage**: Reduce concurrency and implement file size limits

**Network Timeouts**: Configure appropriate timeouts and connection pooling

### Diagnostic Commands

```bash
# Monitor system resources
python bin/TaskvineMonitor.py --port 9124

# Check worker status
vine_status MANAGER_IP:9124

# View logs
tail -f taskvine_monitor.log
```

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add proper documentation and tests
4. Submit a pull request

## License

Please consult zkdeng@arizona.edu for licensing and usage terms.

## Contact

**Primary Contact**: Zi Deng - zkdeng@arizona.edu

**Project Repository**: [https://github.com/zkdeng-uofa/LDAWT](https://github.com/zkdeng-uofa/LDAWT)

**Acknowledgments**:
- University of Arizona Data Science Institute
- Nirav Merchant - nirav@arizona.edu
- TaskVine Development Team at University of Notre Dame
