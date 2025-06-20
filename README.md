# Large Dataset Acquisition Workflow Template (LDAWT)

A Python-based distributed parallel workflow system for downloading and processing large datasets using the TaskVine framework. LDAWT enables efficient acquisition of large-scale datasets across multiple machines with comprehensive monitoring, error handling, and optimization capabilities.

## Table of Contents

- [Large Dataset Acquisition Workflow Template (LDAWT)](#large-dataset-acquisition-workflow-template-ldawt)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Quick Start](#quick-start)
    - [Basic Workflow](#basic-workflow)
  - [Core Scripts](#core-scripts)
    - [Dataset Management](#dataset-management)
      - [`bin/SplitParquet.py`](#binsplitparquetpy)
      - [`bin/CalcDatasetSize.py`](#bincalcdatasetsizepy)
    - [Image Downloading](#image-downloading)
      - [`bin/ImgDownload.py`](#binimgdownloadpy)
      - [`bin/ImgDownloadBW.py`](#binimgdownloadbwpy)
      - [`bin/ImgReconstruct.py`](#binimgreconstructpy)
    - [TaskVine Workflows](#taskvine-workflows)
      - [`bin/TaskvineLDAWT.py`](#bintaskvineldawtpy)
      - [`bin/TaskvineLDAWTCloud.py`](#bintaskvineldawtcloudpy)
    - [Monitoring and Utilities](#monitoring-and-utilities)
      - [`bin/TaskvineMonitor.py`](#bintaskvinemonitorpy)
      - [`bin/StartWorker.py`](#binstartworkerpy)
      - [`bin/compare_files.py`](#bincompare_filespy)
    - [HPC Integration](#hpc-integration)
      - [`bin/MakeTaskvineSlurm.py`](#binmaketaskvineslurmpy)
    - [Data Upload](#data-upload)
      - [`bin/upload_to_cyverse.py`](#binupload_to_cyversepy)
  - [Configuration Files](#configuration-files)
    - [`environment.yml`](#environmentyml)
    - [Configuration JSON Format](#configuration-json-format)
  - [Jupyter Notebooks](#jupyter-notebooks)
    - [`jupyter/LDAWT.ipynb`](#jupyterldawtipynb)
    - [`jupyter/LDAWTest.ipynb`](#jupyterldawtestipynb)
  - [Usage Examples](#usage-examples)
    - [Complete Workflow Example](#complete-workflow-example)
    - [Cloud Integration Example](#cloud-integration-example)
    - [HPC Cluster Example](#hpc-cluster-example)
  - [Performance Optimization](#performance-optimization)
    - [System Configuration](#system-configuration)
    - [TaskVine Optimization](#taskvine-optimization)
    - [Best Practices](#best-practices)
  - [Troubleshooting](#troubleshooting)
    - [Common Issues](#common-issues)
    - [Diagnostic Commands](#diagnostic-commands)
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

#### `bin/SplitParquet.py`
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
- `--parquet`: Input parquet file path
- `--grouping_col`: Column name to group data by
- `--groups`: Number of output groups to create
- `--output_folder`: Directory for output parquet files

#### `bin/CalcDatasetSize.py`
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

**Output**: Total dataset size in MB/GB and count of unresolvable URLs

### Image Downloading

#### `bin/ImgDownload.py`
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

#### `bin/ImgDownloadBW.py`
Enhanced image downloader with bandwidth monitoring, size limits, and advanced error handling.

**Purpose**: Production-ready image downloading with comprehensive monitoring and optimization.

**Key Features**:
- Bandwidth tracking and monitoring
- File size limits and validation
- Graceful shutdown handling (SIGINT/SIGTERM)
- Configurable timeout and concurrency
- Detailed progress reporting
- Resource usage optimization

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

**Advanced Parameters**:
- `--concurrent_downloads`: Concurrent download limit (default: 1000)
- `--timeout`: Individual download timeout in seconds (default: 30)
- `--max_file_size`: Maximum file size in bytes (default: 500MB)

#### `bin/ImgReconstruct.py`
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

#### `bin/TaskvineLDAWT.py`
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

**Configuration Requirements**:
```json
{
    "port_number": 9124,
    "parquets_directory": "split_data",
    "url_col": "photo_url",
    "class_col": "species_name"
}
```

#### `bin/TaskvineLDAWTCloud.py`
Enhanced cloud-optimized TaskVine manager with comprehensive monitoring and error handling.

**Purpose**: Production-ready distributed workflow manager with advanced features for cloud and HPC environments.

**Key Features**:
- Enhanced error handling and retry logic
- Resource requirement specification
- Detailed task monitoring and reporting
- Configurable timeout and retry settings
- Cloud storage integration (CyVerse upload)
- Performance optimization settings

**Usage**:
```bash
python bin/TaskvineLDAWTCloud.py --config_file taskvineCloud.json
```

**Advanced Configuration**:
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

**Key Improvements over Basic Version**:
- Comprehensive task status monitoring
- Resource allocation optimization
- Enhanced error reporting
- Cloud integration capabilities
- Performance tuning options

### Monitoring and Utilities

#### `bin/TaskvineMonitor.py`
Real-time system and TaskVine performance monitoring tool.

**Purpose**: Provides comprehensive monitoring of system resources and TaskVine workflow performance.

**Key Features**:
- Real-time CPU, memory, and disk monitoring
- TaskVine port status checking
- Network I/O tracking
- File descriptor monitoring
- Configurable monitoring intervals
- Alert system for resource thresholds
- Persistent logging

**Usage**:
```bash
python bin/TaskvineMonitor.py \
    --port 9124 \
    --log_file monitor.log \
    --interval 10 \
    --disk_path /data/storage
```

**Monitoring Capabilities**:
- System resource utilization
- TaskVine manager connectivity
- Network bandwidth usage
- Storage space monitoring
- Performance alerts and warnings

#### `bin/StartWorker.py`
Optimized TaskVine worker startup with automatic resource detection.

**Purpose**: Simplifies worker deployment with intelligent resource allocation and monitoring.

**Key Features**:
- Automatic system resource detection
- Optimal CPU, memory, and disk allocation
- Configurable resource overrides
- Worker health monitoring
- Automatic cleanup on shutdown
- Integration with TaskVine monitoring

**Usage**:
```bash
python bin/StartWorker.py \
    --manager_host 192.168.1.100 \
    --manager_port 9124 \
    --cores 8 \
    --memory 16000 \
    --disk 50000
```

**Auto-Detection Features**:
- CPU cores (reserves 1 for system)
- Memory allocation (uses 80% of available)
- Disk space (uses 70% of free space)

#### `bin/compare_files.py`
Utility for comparing file listings between cloud storage and local systems.

**Purpose**: Helps synchronize and verify file transfers between different storage systems.

**Key Features**:
- Compares cloud vs local file inventories
- Identifies missing files in either location
- Generates synchronization reports
- Supports different file listing formats

**Usage**:
```bash
python bin/compare_files.py
```

**Input Files**:
- `new_insects_cloud.txt`: Cloud file listing
- `new_insects_local.txt`: Local file listing

**Output Files**:
- `new_insects_not_cloud.txt`: Files in cloud but not local
- `new_insects_not_local.txt`: Files in local but not cloud

### HPC Integration

#### `bin/MakeTaskvineSlurm.py`
Generates SLURM batch scripts for HPC cluster integration.

**Purpose**: Facilitates deployment of TaskVine workers on SLURM-managed HPC clusters.

**Key Features**:
- Automatic IP address detection
- SLURM script generation
- Configurable resource requests
- Environment setup automation

**Usage**:
```bash
python bin/MakeTaskvineSlurm.py
```

**Generated Script Features**:
- SLURM job configuration
- Conda environment activation
- Automatic worker submission
- Resource specification

### Data Upload

#### `bin/upload_to_cyverse.py`
Parallel file upload utility for CyVerse data storage.

**Purpose**: Efficiently uploads large datasets to CyVerse data store using parallel processing.

**Key Features**:
- Parallel upload processing (configurable concurrency)
- Progress tracking and reporting
- Error handling and retry logic
- File existence verification
- Upload time monitoring

**Usage**:
```bash
python bin/upload_to_cyverse.py
```

**Configuration**:
- Reads from `new_insects_not_local.txt`
- Uploads to `AIIRA_New_Insects` directory
- Uses 25 concurrent uploads by default

## Configuration Files

### `environment.yml`
Conda environment specification with all required dependencies:

**Key Dependencies**:
- Python 3.10.8
- TaskVine (ndcctools=7.14.3)
- Data processing: pandas, pyarrow, numpy
- Async operations: aiohttp
- System monitoring: psutil
- Progress tracking: tqdm
- Cloud integration: gocommands
- Jupyter support: ipykernel

### Configuration JSON Format
TaskVine workflows use JSON configuration files:

```json
{
    "port_number": 9124,
    "parquets_directory": "data/split",
    "class_col": "species_name",
    "url_col": "photo_url",
    "timeout_minutes": 30,
    "max_retries": 3
}
```

## Jupyter Notebooks

### `jupyter/LDAWT.ipynb`
Main workflow demonstration notebook showcasing complete LDAWT usage.

**Contents**:
- Environment setup and verification
- Dataset size calculation examples
- TaskVine workflow execution
- IP address configuration for distributed setup
- Performance monitoring examples

### `jupyter/LDAWTest.ipynb`
Testing and example notebook for development and validation.

**Contents**:
- Quick testing workflows
- Configuration validation
- Small-scale dataset examples
- Performance benchmarking

## Usage Examples

### Complete Workflow Example

1. **Prepare Dataset**
   ```bash
   # Split large dataset into manageable chunks
   python bin/SplitParquet.py \
       --parquet large_dataset.parquet \
       --grouping_col species_name \
       --groups 50 \
       --output_folder split_data
   ```

2. **Estimate Storage Requirements**
   ```bash
   # Calculate total size needed
   python bin/CalcDatasetSize.py \
       --directory split_data \
       --url_column photo_url
   ```

3. **Start Monitoring**
   ```bash
   # Launch system monitor
   python bin/TaskvineMonitor.py \
       --port 9124 \
       --interval 10 \
       --log_file workflow.log
   ```

4. **Launch Manager**
   ```bash
   # Start TaskVine manager
   python bin/TaskvineLDAWTCloud.py \
       --config_file config.json
   ```

5. **Deploy Workers** (on multiple machines)
   ```bash
   # Start optimized workers
   python bin/StartWorker.py \
       --manager_host MANAGER_IP \
       --manager_port 9124
   ```

### Cloud Integration Example

```bash
# Complete workflow with cloud upload
python bin/TaskvineLDAWTCloud.py --config_file cloud_config.json

# Upload results to CyVerse
python bin/upload_to_cyverse.py
```

### HPC Cluster Example

```bash
# Generate SLURM script
python bin/MakeTaskvineSlurm.py

# Submit to SLURM
sbatch taskvine.slurm
```

## Performance Optimization

### System Configuration
- **Memory**: Allocate 80% of available memory to workers
- **CPU**: Reserve 1 core for system operations
- **Disk**: Ensure 70% free space for temporary files
- **Network**: Use appropriate concurrency limits (50-100 downloads per worker)

### TaskVine Optimization
- **Resource Allocation**: Specify CPU, memory, and disk requirements
- **Timeout Settings**: Balance between patience and efficiency
- **Retry Logic**: Configure appropriate retry counts for network failures
- **Monitoring**: Use real-time monitoring to identify bottlenecks

### Best Practices
1. Start with monitoring to establish baseline performance
2. Use moderate concurrency to avoid overwhelming servers
3. Monitor network bandwidth to prevent connection saturation
4. Set appropriate timeouts based on expected file sizes
5. Deploy multiple workers for horizontal scaling
6. Regular cleanup of temporary files and logs

For detailed optimization guidance, see [OPTIMIZATION_GUIDE.md](OPTIMIZATION_GUIDE.md).

## Troubleshooting

### Common Issues

**Tasks Stop After Limited Executions**
- **Cause**: Resource exhaustion or missing output declarations
- **Solution**: Use optimized scripts with proper resource management

**Workers Disconnect Frequently**
- **Cause**: Resource limits or network timeouts
- **Solution**: Use `StartWorker.py` for auto-resource detection

**High Memory Usage**
- **Cause**: Too many concurrent downloads or large files
- **Solution**: Reduce concurrency and implement file size limits

**Network Timeouts**
- **Cause**: No request timeouts or poor connection handling
- **Solution**: Configure appropriate timeouts and connection pooling

### Diagnostic Commands

```bash
# Check system resources
python bin/TaskvineMonitor.py --port 9124

# View worker status
vine_status MANAGER_IP:9124

# Check log files
tail -f taskvine_monitor.log
```

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch
3. Make your changes with proper documentation
4. Add tests for new functionality
5. Submit a pull request

## License

Please consult zkdeng@arizona.edu for licensing and usage terms.

## Contact

**Primary Contact**: Zi Deng - zkdeng@arizona.edu

**Project Repository**: [https://github.com/zkdeng-uofa/LDAWT](https://github.com/zkdeng-uofa/LDAWT)

**Acknowledgments**:
- University of Arizona Data Science Institute
- Nirav Merchant - nirav@arizona.edu
- TaskVine Development Team at University of Notre Dame 