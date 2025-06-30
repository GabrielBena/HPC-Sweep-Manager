# 🚀 **HSM Distributed Computing - Quick Start Guide**

HSM now supports **distributed computing across multiple machines!** This allows you to run sweeps simultaneously on your local machine + multiple SSH servers.

## 🎯 **Overview**

The distributed computing system orchestrates jobs across:
- **Local machine** - parallel execution on your current system
- **SSH remotes** - multiple remote servers via SSH
- **Automatic load balancing** - jobs distributed based on availability
- **Result synchronization** - all outputs collected locally

## ⚡ **Quick Setup**

### 1. Initialize Distributed Computing

```bash
cd /path/to/your/project
hsm distributed init
```

This adds distributed computing configuration to your `hsm_config.yaml`.

### 2. Add Remote Compute Sources

```bash
# Add your existing "blossom" server (already configured)
hsm distributed add gpu-server gpu.university.edu --key ~/.ssh/id_ed25519 --max-jobs 2

# Add more servers
hsm distributed add hpc-node1 hpc1.cluster.edu --max-jobs 4
hsm distributed add workstation ws.lab.edu --max-jobs 1
```

### 3. Test All Sources

```bash
# Test all compute sources
hsm distributed test --all

# Test specific sources
hsm distributed test local blossom gpu-server
```

### 4. Run Distributed Sweep

```bash
# Run sweep across ALL available sources
hsm sweep --mode distributed

# Your jobs will be automatically distributed across:
# - Local machine (4 parallel jobs)
# - blossom server (1 parallel job)  
# - gpu-server (2 parallel jobs)
# - hpc-node1 (4 parallel jobs)
# - workstation (1 parallel job)
# Total: 12 parallel jobs!
```

## 🔧 **Management Commands**

### List Compute Sources
```bash
hsm distributed list
```

### Health Check
```bash
# Check all sources
hsm distributed health --all

# Continuous monitoring
hsm distributed health --all --watch
```

### Remove Source
```bash
hsm distributed remove gpu-server
```

## 📊 **Example Output**

When you run `hsm sweep --mode distributed`, you'll see:

```
🚀 HPC Sweep Manager
Config: sweeps/sweep.yaml
Mode: distributed

✓ Using HSM config: sweeps/hsm_config.yaml
✓ Distributed job manager created successfully

Setting up distributed computing environment...
Discovering SSH source: blossom...
✓ Added SSH source: blossom (1 max jobs)
Discovering SSH source: gpu-server...
✓ Added SSH source: gpu-server (2 max jobs)
✓ Added local compute source (4 max jobs)

Initializing 3 compute sources...
✓ Setup successful for local
✓ Setup successful for blossom  
✓ Setup successful for gpu-server
✓ 3 compute sources ready for distributed execution

✓ Distributed environment ready

Starting distributed execution across 3 sources...
[1/24] Job sweep_20241215_143022_task_001 submitted to local
[2/24] Job sweep_20241215_143022_task_002 submitted to blossom
[3/24] Job sweep_20241215_143022_task_003 submitted to gpu-server
[4/24] Job sweep_20241215_143022_task_004 submitted to local
...

Progress: 10/24 (41.7%) - ✓ 8 completed, ✗ 0 failed
Progress: 20/24 (83.3%) - ✓ 18 completed, ✗ 0 failed

✓ Distributed sweep completed successfully!
Collecting results from distributed sources...
✓ Results collected from 2/2 SSH sources
✓ Results collected successfully to sweeps/outputs/sweep_20241215_143022/tasks/
```

## ⚙️ **Configuration**

Your `hsm_config.yaml` will look like:

```yaml
# ... existing config ...

distributed:
  enabled: true
  strategy: "round_robin"  # round_robin, least_loaded, capability_based
  collect_interval: 300    # Collect results every 5 minutes
  max_retries: 3
  
  remotes:
    blossom:
      host: "blossom"
      ssh_key: ~/.ssh/id_ed25519
      max_parallel_jobs: 1
      enabled: true
    
    gpu-server:
      host: "gpu.university.edu"
      ssh_key: ~/.ssh/id_ed25519
      max_parallel_jobs: 2
      enabled: true
      
    hpc-node1:
      host: "hpc1.cluster.edu"
      ssh_port: 22
      max_parallel_jobs: 4
      enabled: true
```

## 🎛️ **Distribution Strategies**

- **`round_robin`** - Distribute jobs evenly across sources
- **`least_loaded`** - Send jobs to source with lowest utilization  
- **`capability_based`** - Prefer sources with more available slots

Change strategy in `hsm_config.yaml`:
```yaml
distributed:
  strategy: "least_loaded"
```

## 📁 **Result Organization**

Distributed sweeps create organized outputs:

```
sweeps/outputs/sweep_20241215_143022/
├── tasks/
│   ├── local/           # Local job results
│   │   ├── task_1/
│   │   ├── task_4/
│   │   └── task_7/
│   ├── remote_blossom/  # Results from blossom
│   │   ├── task_2/
│   │   ├── task_5/
│   │   └── task_8/
│   └── remote_gpu-server/ # Results from gpu-server
│       ├── task_3/
│       ├── task_6/
│       └── task_9/
├── logs/               # Execution logs
└── distributed_scripts/ # Generated scripts
```

## 🚨 **Requirements**

1. **SSH Access** - Passwordless SSH to all remote machines
2. **Project Sync** - Same codebase on all machines (auto-verified)
3. **Dependencies** - Python environment with required packages on all machines
4. **HSM Config** - `hsm_config.yaml` on each remote machine

## 🎉 **Benefits**

- **Massive Parallelization** - 10x+ speedup with multiple machines
- **Fault Tolerance** - Failed sources don't stop the sweep
- **Easy Management** - Single command controls everything
- **Auto-Discovery** - Automatically finds Python/project paths
- **Result Aggregation** - All outputs collected locally
- **Real-time Progress** - Monitor across all sources

## 🔍 **Example Use Cases**

1. **Academic Research** - Use lab machines + HPC cluster
2. **Multi-Cloud** - Combine AWS, GCP, Azure instances  
3. **Home Lab** - Local machine + spare computers
4. **Team Collaboration** - Share compute across team members

---

🎯 **Ready to go distributed?** Start with `hsm distributed init` and scale your ML experiments like never before! 