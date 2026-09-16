# Cloud Hardware & Virtualization Microbenchmarks

Empirical systems evaluation of CPU compute scaling, memory bus architecture, and network topology across multi-tenant AWS EC2 instances in `us-east-1` (N. Virginia) running Ubuntu 22.04 LTS.

---

## 1. Tooling & Test Methodology

Benchmarking on resource-constrained cloud VMs requires lightweight, deterministic tools:
- **Sysbench 1.0.20:** Selected over Phoronix Test Suite to avoid high memory overhead and out-of-memory errors on small burstable instances (`t2.micro` with 957 MB RAM).
- **Single-Core CPU:** `sysbench cpu --cpu-max-prime=20000 --threads=1 run`
- **Multi-Core CPU:** `sysbench cpu --cpu-max-prime=20000 --threads=$(nproc) run`
- **Memory Subsystem:** `sysbench memory --memory-total-size=10G --threads=$(nproc) run`
- **Network Bandwidth & Latency:** `iPerf3` for TCP bandwidth; ICMP `ping` for round-trip time (RTT).

---

## 2. CPU & Memory Subsystem Measurements

| Instance Type | vCPUs | RAM | Architecture | Single-Core (events/s) | Multi-Core (events/s) | Multi-Core Scaling | Memory Bandwidth (MiB/s) |
|---|---|---|---|---|---|---|---|
| `t2.micro` | 1 | 957 MB | Intel Xeon E5-2686 v4 (Broadwell) @ 2.30 GHz | 877.95 | 880.11 | $1.00\times$ | 521.58 |
| `t2.medium` | 2 | 3.8 GB | Intel Xeon E5-2686 v4 (Broadwell) @ 2.30 GHz | 882.32 | 1,565.29 | **$1.77\times$** | 855.68 |
| `c5d.large` | 2 | 3.7 GB | Intel Xeon Platinum 8124M (Skylake) @ 3.00 GHz | 450.82 | 703.32 | **$1.56\times$** | **7,578.52** |

### Microarchitectural Analysis

#### Physical Silicon Cores vs. SMT Hyperthreads
Inspecting CPU topology via `lscpu`:
- `t2.medium` allocates **2 independent physical cores** (`Thread(s) per core: 1`, `Core(s) per socket: 2`). Each thread gets dedicated ALU pipelines, execution ports, and private L1/L2 caches, delivering near-linear **$1.77\times$ scaling**.
- `c5d.large` allocates **2 logical threads on a single physical core** (`Thread(s) per core: 2`, `Core(s) per socket: 1`). Both threads share execution units and caches; under compute-intensive prime calculation, structural hazards bound scaling to **$1.56\times$**.

#### Memory Bandwidth: The 9× Skylake Disparity
`c5d.large` outperforms `t2.medium` by **$8.85\times$** in memory throughput ($7,578.52\text{ MiB/s}$ vs. $855.68\text{ MiB/s}$).

Theoretical memory bus bandwidth is calculated as:
$$\text{Theoretical Peak} = \text{Channels} \times \text{Bus Width} \times \text{Clock Frequency}$$

- **Intel Xeon E5-2686 v4 (Broadwell, `t2` instances):**
  $$\text{Peak} = 4 \text{ channels} \times 64\text{ bits} \times 1600\text{ MT/s} \div 8 = 51.2\text{ GB/s (DDR3-1600)}$$
- **Intel Xeon Platinum 8124M (Skylake, `c5d` instances):**
  $$\text{Peak} = 6 \text{ channels} \times 64\text{ bits} \times 2666\text{ MT/s} \div 8 = 127.9\text{ GB/s (DDR4-2666)}$$

The Skylake platform provides **50% more channels (6 vs. 4)** and **67% higher clock speed (2666 vs. 1600 MT/s)**. In addition, burstable `t2` tiers suffer from hypervisor-enforced memory controller throttling, while `c5d` receives unconstrained memory channel access.

---

## 3. Network Topology: Intra-VPC vs. Cross-Region WAN

### Intra-Region Proximity (`us-east-1`)

| Pairing | Link Type | TCP Bandwidth | RTT Latency |
|---|---|---|---|
| `c5n.large` $\leftrightarrow$ `c5n.large` | Intra-Region (Private IP) | 4.97 Gbps | **0.200 ms** |
| `m5.large` $\leftrightarrow$ `m5.large` | Intra-Region (Private IP) | 4.97 Gbps | 0.238 ms |
| `t3.medium` $\leftrightarrow$ `t3.medium` | Intra-Region (Private IP) | 4.09 Gbps | 0.248 ms |
| `t3.medium` $\leftrightarrow$ `c5n.large` | Intra-Region (Private IP, Heterogeneous) | 4.94 Gbps | 0.635 ms |
| `m5.large` $\leftrightarrow$ `c5n.large` | Intra-Region (Private IP, Heterogeneous) | 4.96 Gbps | 0.690 ms |
| `m5.large` $\leftrightarrow$ `t3.medium` | Intra-Region (Private IP, Heterogeneous) | 4.87 Gbps | 1.085 ms |

**Takeaway:** Same-type instances experience $0.200\text{ ms}$ RTT, whereas cross-type pairings jump up to $1.085\text{ ms}$ ($5.4\times$ latency penalty). AWS availability zones place identical instance families into the same physical datacenter rows and top-of-rack (ToR) switches.

### Cross-Region WAN Transit (`us-east-1` $\leftrightarrow$ `us-west-2`)

| Pairing | Region Transit | TCP Bandwidth | RTT Latency |
|---|---|---|---|
| `c5.large` $\leftrightarrow$ `c5.large` | Virginia $\leftrightarrow$ Oregon | **529 Mbps** | **54.800 ms** |

**Takeaway:** Crossing the continental US incurs a **$400\times$ latency penalty** ($0.2\text{ ms} \rightarrow 54.8\text{ ms}$) and an **$89\%$ throughput collapse** ($4.97\text{ Gbps} \rightarrow 529\text{ Mbps}$) due to multi-hop fiber routing and transit queueing.
