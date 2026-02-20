"""
MoSSo vs. Hybrid MoSSo Benchmark Runner.

This script automates the performance evaluation between the original KDD'20
MoSSo algorithm and the newly implemented Hybrid MoSSo (Mags-DM) algorithm.

Usage Examples:
---------------
1. Run the full remote dataset benchmark suite:
   $ python3 benchmark/benchmark.py --mode remote

2. Run a benchmark on a specific local graph file:
   $ python3 benchmark/benchmark.py --mode local --file example_graph.txt

3. Skip the Java compilation phase:
   $ python3 benchmark/benchmark.py --mode local --file example_graph.txt --skip-build

4. Override the default algorithm hyperparameters:
   $ python3 benchmark/benchmark.py --mode remote --samples 50 --escape 5 --interval 10000

Arguments:
----------
--mode: (REQUIRED) 'remote' to download and run remote datasets, or 'local' for a custom file.
--file: (Conditional) Path to the local text file. Required if --mode is 'local'.
--skip-build: (Optional) Flag to skip Git cloning and Java compilation.
--samples: (Optional) Number of random neighbors to sample. Default is 120.
--escape: (Optional) Escape probability parameter for the MoSSo algorithm. Default is 3.
--interval: (Optional) Logging interval for the Java output. Default is 1000.
"""

import argparse
import subprocess
import urllib.request
import os
import re
import gzip
import shutil
import pandas as pd
import matplotlib.pyplot as plt

# Configuration
ORIGINAL_REPO_URL = "https://github.com/jihoonko/kdd20-mosso"
DATASETS_DIR = "datasets"
OUTPUT_DIR = "output"
EXTERNAL_DIR = "external"
BASELINE_DIR = os.path.join(EXTERNAL_DIR, "kdd20-mosso")
BENCHMARK_DIR = os.path.join(OUTPUT_DIR, "benchmark")
JAR_ORIGINAL = "mosso-original.jar"
JAR_HYBRID = "mosso-hybrid.jar"
FASTUTIL = "fastutil-8.2.2.jar"

# Parameters
SAMPLE_NUMBER = 120
ESCAPE = 3
INTERVAL = 1000

# Remote Datasets Definition
datasets = {
    "small": [
        ("https://snap.stanford.edu/data/as-caida20071105.txt.gz", "as-caida20071105.txt"),
        ("https://snap.stanford.edu/data/email-Enron.txt.gz", "Email-Enron.txt"),
        ("https://snap.stanford.edu/data/loc-brightkite_edges.txt.gz", "Brightkite_edges.txt")
    ]
}

def setup_directories():
    os.makedirs(DATASETS_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(BENCHMARK_DIR, exist_ok=True)
    os.makedirs(EXTERNAL_DIR, exist_ok=True)

def build_original_jar():
    print(f"\n[*] Cloning baseline repository to {BASELINE_DIR}...")
    if os.path.exists(BASELINE_DIR):
        shutil.rmtree(BASELINE_DIR)
    try:
        subprocess.run(["git", "clone", ORIGINAL_REPO_URL, BASELINE_DIR],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        shutil.copy(FASTUTIL, os.path.join(BASELINE_DIR, FASTUTIL))
        print("[*] Compiling Original MoSSo...")
        subprocess.run(["sh", "compile.sh"], cwd=BASELINE_DIR, check=True, stdout=subprocess.DEVNULL)
        shutil.move(os.path.join(BASELINE_DIR, "mosso-1.0.jar"), JAR_ORIGINAL)
        print(f"[*] Success! Saved as {JAR_ORIGINAL}")
    except subprocess.CalledProcessError as e:
        print(f"[!] Original compilation failed: {e}")
        exit(1)

def build_local_hybrid_jar():
    print("\n[*] Compiling local Hybrid MoSSo code...")
    try:
        subprocess.run(["sh", "compile.sh"], check=True, stdout=subprocess.DEVNULL)
        shutil.move("mosso-1.0.jar", JAR_HYBRID)
        print(f"[*] Success! Saved as {JAR_HYBRID}")
    except subprocess.CalledProcessError as e:
        print(f"[!] Hybrid compilation failed: {e}")
        exit(1)

def build_jars():
    if not os.path.exists(FASTUTIL):
        print(f"[!] Error: {FASTUTIL} missing. Download it to root first.")
        exit(1)
    build_original_jar()
    build_local_hybrid_jar()

def download_and_prepare_dataset(url, filename):
    gz_path = os.path.join(DATASETS_DIR, filename + ".gz")
    txt_path = os.path.join(DATASETS_DIR, filename)
    if not os.path.exists(txt_path):
        if not os.path.exists(gz_path):
            print(f"[*] Downloading {filename}...")
            urllib.request.urlretrieve(url, gz_path)
        print(f"[*] Converting {filename} to MoSSo format...")
        with gzip.open(gz_path, 'rt') as f_in, open(txt_path, 'w') as f_out:
            for line in f_in:
                if line.startswith('#'): continue
                parts = line.strip().split()
                if len(parts) >= 2:
                    f_out.write(f"{parts[0]}\t{parts[1]}\t1\n")
        os.remove(gz_path)
    return txt_path

def run_mosso(jar_file, dataset_path, output_name, samples, escape, interval):
    classpath = f"{FASTUTIL}{os.pathsep}{jar_file}"
    out_file = os.path.join(OUTPUT_DIR, output_name)
    log_file = f"{out_file}.log"

    cmd = [
        "java", "-cp", classpath, "mosso.Run",
        dataset_path,
        output_name,
        "mosso",
        str(escape),
        str(samples),
        str(interval)
    ]

    print(f"    -> Running {jar_file}...")
    print(f"       (Saving logs to {log_file})")
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        output_lines = []

        with open(log_file, 'w') as log_f:
            for line in process.stdout:
                print(f"       {line}", end="")
                log_f.write(line)
                output_lines.append(line)

        process.wait()

        if process.returncode != 0:
            print(f"[!] Java error: Code {process.returncode}")
            return None, None

        output = "".join(output_lines)
        time_m = re.search(r"Execution time:\s*([\d.]+)s", output)
        ratio_m = re.search(r"Expected Compression Ratio:\s*([\d.]+)", output, re.IGNORECASE)

        return float(time_m.group(1)) if time_m else None, float(ratio_m.group(1)) if ratio_m else None
    except Exception as e:
        print(f"[!] Execution failed: {e}")
        return None, None

def plot_results(csv_file, plot_file):
    df = pd.read_csv(csv_file)
    if df.empty: return

    df = df.dropna(subset=["Time_Original", "Time_Hybrid"])

    if df.empty:
        print("[!] No successful runs to plot.")
        return

    fig, axes = plt.subplots(1, 2, figsize=(15, 6))

    df.plot(x="Dataset", y=["Ratio_Original", "Ratio_Hybrid"], kind="bar", ax=axes[0], color=["#e74c3c", "#2ecc71"])
    axes[0].set_title("Compression Ratio (Lower is Better)")
    axes[0].set_ylabel("Ratio")
    axes[0].tick_params(axis='x', rotation=45 if len(df) > 1 else 0)

    df.plot(x="Dataset", y=["Time_Original", "Time_Hybrid"], kind="bar", ax=axes[1], color=["#e74c3c", "#2ecc71"])
    axes[1].set_title("Execution Time (Seconds)")
    axes[1].set_ylabel("Seconds")
    axes[1].tick_params(axis='x', rotation=45 if len(df) > 1 else 0)

    plt.tight_layout()
    plt.savefig(plot_file)
    print(f"[*] Plot saved to {plot_file}")

def print_benchmark_summary(dataset_name, t_orig, t_hyb, r_orig, r_hyb):
    print(f"\n" + "="*50)
    print(f" FINAL RESULTS: {dataset_name}")
    print("="*50)

    # Check if any run failed and returned None
    if None in (t_orig, t_hyb, r_orig, r_hyb):
        print(" [!] One or more runs failed. Cannot calculate differences.")
        print("="*50 + "\n")
        return

    # Time Math (Higher time = slower)
    t_diff = t_orig - t_hyb
    t_pct = (t_diff / t_orig) * 100 if t_orig else 0
    t_status = "FASTER" if t_diff > 0 else "SLOWER"

    # Ratio Math (Lower ratio = better compression)
    r_diff = r_hyb - r_orig
    r_pct = (r_diff / r_orig) * 100 if r_orig else 0
    r_status = "WORSE" if r_diff > 0 else "BETTER"

    print(f" [Execution Time]")
    print(f"   - Original : {t_orig:.4f} seconds")
    print(f"   - Hybrid   : {t_hyb:.4f} seconds")
    print(f"   -> Hybrid is {abs(t_diff):.4f}s {t_status} ({abs(t_pct):.2f}%)\n")

    print(f" [Compression Ratio (Lower is Better)]")
    print(f"   - Original : {r_orig:.6f}")
    print(f"   - Hybrid   : {r_hyb:.6f}")
    print(f"   -> Hybrid is {abs(r_diff):.6f} {r_status} ({abs(r_pct):.4f}%)")
    print("="*50 + "\n")


# Mode 1: Remote Benchmark
def run_remote_suite(samples, escape, interval):
    results_file = os.path.join(BENCHMARK_DIR, "remote_results.csv")
    plot_file = os.path.join(BENCHMARK_DIR, "remote_comparison.png")
    results = []

    for category, data_list in datasets.items():
        print(f"\n=== REMOTE CATEGORY: {category.upper()} ===")
        for url, filename in data_list:
            dataset_name = filename.replace(".txt", "")
            path = download_and_prepare_dataset(url, filename)

            t1, r1 = run_mosso(JAR_ORIGINAL, path, f"orig_{filename}", samples, escape, interval)
            t2, r2 = run_mosso(JAR_HYBRID, path, f"hyb_{filename}", samples, escape, interval)

            print_benchmark_summary(dataset_name, t1, t2, r1, r2)

            results.append({
                "Dataset": dataset_name,
                "Time_Original": t1, "Time_Hybrid": t2,
                "Ratio_Original": r1, "Ratio_Hybrid": r2
            })
            pd.DataFrame(results).to_csv(results_file, index=False)

    plot_results(results_file, plot_file)

# Mode 2: Local File Benchmark
def run_local_suite(file_path, samples, escape, interval):
    if not os.path.exists(file_path):
        print(f"[!] Error: Cannot find file '{file_path}'")
        return

    filename = os.path.basename(file_path)
    dataset_name = filename.replace(".txt", "")
    results_file = os.path.join(BENCHMARK_DIR, f"local_{filename}_results.csv")
    plot_file = os.path.join(BENCHMARK_DIR, f"local_{filename}_comparison.png")

    print(f"\n=== BENCHMARKING LOCAL FILE: {filename} ===")
    t1, r1 = run_mosso(JAR_ORIGINAL, file_path, f"orig_local.txt", samples, escape, interval)
    t2, r2 = run_mosso(JAR_HYBRID, file_path, f"hyb_local.txt", samples, escape, interval)

    print_benchmark_summary(dataset_name, t1, t2, r1, r2)

    df = pd.DataFrame([{
        "Dataset": dataset_name,
        "Time_Original": t1, "Time_Hybrid": t2,
        "Ratio_Original": r1, "Ratio_Hybrid": r2
    }])
    df.to_csv(results_file, index=False)
    plot_results(results_file, plot_file)

def main():
    parser = argparse.ArgumentParser(description="MoSSo Graph Summarization Benchmarking Tool")
    parser.add_argument("--mode", choices=["remote", "local"], required=True,
                        help="Choose 'remote' to run the remote datasets, or 'local' for a specific file.")
    parser.add_argument("--file", type=str,
                        help="Path to your local graph file (Required if mode is 'local')")
    parser.add_argument("--skip-build", action="store_true",
                        help="Skip downloading and recompiling the Java JAR files.")

    parser.add_argument("--samples", type=int, default=120,
                        help="Number of random neighbors to sample (Default: 120)")
    parser.add_argument("--escape", type=int, default=3,
                        help="Escape probability parameter (Default: 3)")
    parser.add_argument("--interval", type=int, default=1000,
                        help="How often the Java code prints progress (Default: 1000)")

    args = parser.parse_args()

    if not args.skip_build:
        build_jars()

    setup_directories()

    if args.mode == "remote":
        run_remote_suite(args.samples, args.escape, args.interval)
    elif args.mode == "local":
        if not args.file:
            print("[!] Error: You must provide a --file argument when using --mode local.")
            return
        run_local_suite(args.file, args.samples, args.escape, args.interval)

if __name__ == "__main__":
    main()