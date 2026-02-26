"""
MoSSo vs. Hybrid MoSSo Benchmark Runner.

This script automates the performance evaluation between the original KDD '20
MoSSo algorithm and the newly implemented Hybrid MoSSo (Mags-DM) algorithm.

Usage Examples:
---------------
1. Run the full remote dataset benchmark suite:
   $ python3 benchmark/benchmark.py --mode remote

2. Run a benchmark on a specific local graph file:
   $ python3 benchmark/benchmark.py --mode local --file example_graph.txt

Arguments:
----------
--mode: (REQUIRED) 'remote' to download and run remote datasets, or 'local' for a custom file.
--file: (Conditional) Path to the local text file. Required if --mode is 'local'.
--skip-build: (Optional) Flag to skip Git cloning and Java compilation.
--samples: (Optional) Number of random neighbors to sample. Default is 120.
--escape: (Optional) Escape probability parameter for the MoSSo algorithm. Default is 3.
--interval: (Optional) Logging interval for the Java output. Default is 1000.
--runs: (Optional) See results over multiple runs to calculate averages.
--discard-summaries: (Optional) Throw away the generated summary graph files to save disk space.
"""

import argparse
import subprocess
import urllib.request
import os
import re
import gzip
import shutil
import stat
import pandas as pd
import glob

from plotter import plot_results, plot_runs_variance

# Configuration
ORIGINAL_REPO_URL = "https://github.com/jihoonko/kdd20-mosso"
DATASETS_DIR = "datasets"
OUTPUT_DIR = "output"
EXTERNAL_DIR = "external"
BASELINE_DIR = os.path.join(EXTERNAL_DIR, "kdd20-mosso")
BENCHMARK_DIR = os.path.join(OUTPUT_DIR, "benchmark")
RUNS_DIR = os.path.join(BENCHMARK_DIR, "runs")
SUMMARIZED_DIR = os.path.join(BENCHMARK_DIR, "summarized_graphs")
JAR_ORIGINAL = "mosso-original.jar"
JAR_HYBRID = "mosso-hybrid.jar"
fastutil_files = glob.glob("fastutil-*.jar")
FASTUTIL = fastutil_files[0] if fastutil_files else "fastutil-missing.jar"

# Remote Datasets
datasets = {
    "small": [
        ("https://snap.stanford.edu/data/as-caida20071105.txt.gz", "as-caida20071105.txt"),
        ("https://snap.stanford.edu/data/email-Enron.txt.gz", "Email-Enron.txt"),
        ("https://snap.stanford.edu/data/loc-brightkite_edges.txt.gz", "Brightkite_edges.txt")
    ]
}

def setup_directories():
    if os.path.exists(BENCHMARK_DIR):
        force_rmtree(BENCHMARK_DIR)

    os.makedirs(DATASETS_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(BENCHMARK_DIR, exist_ok=True)
    os.makedirs(EXTERNAL_DIR, exist_ok=True)
    os.makedirs(RUNS_DIR, exist_ok=True)
    os.makedirs(SUMMARIZED_DIR, exist_ok=True)

def _on_rm_error(func, path, _):
    try:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except OSError:
        pass

def force_rmtree(path):
    if os.path.exists(path):
        shutil.rmtree(path, onerror=_on_rm_error)

def build_original_jar():
    print(f"\n[*] Cloning baseline repository to {BASELINE_DIR}...")
    if os.name == "nt":
        subprocess.run(["attrib", "-R", "-S", "-H", f"{BASELINE_DIR}\\*", "/S", "/D"], check=False)
    if os.path.exists(BASELINE_DIR):
        force_rmtree(BASELINE_DIR)
    try:
        subprocess.run(["git", "clone", ORIGINAL_REPO_URL, BASELINE_DIR],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        shutil.copy(FASTUTIL, os.path.join(BASELINE_DIR, FASTUTIL))
        print("[*] Compiling Original MoSSo...")
        subprocess.run(["bash", "compile.sh"], cwd=BASELINE_DIR, check=True, stdout=subprocess.DEVNULL)
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

def run_multiple_mosso(jar_file, dataset_path, output_name, samples, escape, interval, runs, discard_summaries, bCandidates=None):
    times = []
    ratios = []
    for i in range(runs):
        if runs > 1:
            print(f"\n    [Iteration {i+1}/{runs}]")
        t, r = run_mosso(jar_file, dataset_path, f"{output_name}_run{i+1}", samples, escape, interval, discard_summaries, bCandidates)
        if t is not None and r is not None:
            times.append(t)
            ratios.append(r)

    return times, ratios

def run_mosso(jar_file, dataset_path, output_name, samples, escape, interval, discard_summaries, bCandidates=None):
    classpath = f"{FASTUTIL}{os.pathsep}{jar_file}"
    out_file = os.path.join(RUNS_DIR, output_name)
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
    
    if bCandidates is not None:
        cmd.append(str(bCandidates))

    print(f"    -> Running {jar_file}...")
    print(f"       (Saving logs to {log_file})")
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        output_lines = []

        with open(log_file, 'w') as log_f:
            for line in process.stdout:
                log_f.write(line)
                output_lines.append(line)

                if " : Elapsed time :" in line:
                    iteration = line.split(" : ")[0].strip()
                    print(f"\r       -> Processing edge: {iteration}...", end="", flush=True)

        process.wait()
        print("\r       -> Processing complete!                      ", flush=True)

        if process.returncode != 0:
            print(f"[!] Java error: Code {process.returncode}")
            return None, None

        # Clean up Java's hardcoded output handling
        java_output_file = os.path.join("output", output_name)
        if os.path.exists(java_output_file):
            if discard_summaries:
                os.remove(java_output_file)
            else:
                shutil.move(java_output_file, os.path.join(SUMMARIZED_DIR, output_name))

        output = "".join(output_lines)
        time_m = re.search(r"Execution time:\s*([\d.]+)s", output)
        ratio_m = re.search(r"Expected Compression Ratio:\s*([\d.]+)", output, re.IGNORECASE)

        return float(time_m.group(1)) if time_m else None, float(ratio_m.group(1)) if ratio_m else None
    except Exception as e:
        print(f"[!] Execution failed: {e}")
        return None, None

def print_benchmark_summary(dataset_name, t_orig, t_hyb, r_orig, r_hyb, runs):
    summary_lines = [f"\n" + "=" * 50, f" FINAL RESULTS: {dataset_name} (Average of {runs} runs)", "=" * 50]

    if None in (t_orig, t_hyb, r_orig, r_hyb):
        summary_lines.append(" [!] One or more runs failed. Cannot calculate differences.")
        summary_lines.append("="*50 + "\n")
    else:
        t_diff = t_orig - t_hyb
        t_pct = (t_diff / t_orig) * 100 if t_orig else 0
        t_status = "FASTER" if t_diff > 0 else "SLOWER"

        r_diff = r_hyb - r_orig
        r_pct = (r_diff / r_orig) * 100 if r_orig else 0
        r_status = "WORSE" if r_diff > 0 else "BETTER"

        summary_lines.append(f" [Average Execution Time]")
        summary_lines.append(f"   - Original : {t_orig:.4f} seconds")
        summary_lines.append(f"   - Hybrid   : {t_hyb:.4f} seconds")
        summary_lines.append(f"   -> Hybrid is {abs(t_diff):.4f}s {t_status} ({abs(t_pct):.2f}%)\n")

        summary_lines.append(f" [Average Compression Ratio (Lower is Better)]")
        summary_lines.append(f"   - Original : {r_orig:.6f}")
        summary_lines.append(f"   - Hybrid   : {r_hyb:.6f}")
        summary_lines.append(f"   -> Hybrid is {abs(r_diff):.6f} {r_status} ({abs(r_pct):.4f}%)")
        summary_lines.append("="*50 + "\n")

    summary_text = "\n".join(summary_lines)
    print(summary_text)

    summary_file = os.path.join(BENCHMARK_DIR, f"{dataset_name}_summary.txt")
    with open(summary_file, "w") as f:
        f.write(summary_text)
    print(f"[*] Summary report saved to {summary_file}")

# --- Mode 1: Remote Benchmark ---
def run_remote_suite(samples, escape, interval, runs, discard_summaries, bCandidates):
    results_file = os.path.join(BENCHMARK_DIR, "remote_results.csv")
    plot_file = os.path.join(BENCHMARK_DIR, "remote_comparison.pdf")
    results = []

    for category, data_list in datasets.items():
        print(f"\n=== REMOTE CATEGORY: {category.upper()} ===")
        for url, filename in data_list:
            dataset_name = filename.replace(".txt", "")
            path = download_and_prepare_dataset(url, filename)

            t1_list, r1_list = run_multiple_mosso(JAR_ORIGINAL, path, f"orig_{dataset_name}", 120, 3, interval, runs, discard_summaries)
            t2_list, r2_list = run_multiple_mosso(JAR_HYBRID, path, f"hyb_{dataset_name}", samples, escape, interval, runs, discard_summaries, bCandidates)

            if not t1_list or not t2_list:
                print("[!] Not enough successful runs to generate summary.")
                continue

            if runs > 1:
                # Pass RUNS_DIR into the plotter function so it knows where to save!
                plot_runs_variance(dataset_name, t1_list, t2_list, r1_list, r2_list, RUNS_DIR)

            t1_avg = sum(t1_list) / len(t1_list)
            r1_avg = sum(r1_list) / len(r1_list)
            t2_avg = sum(t2_list) / len(t2_list)
            r2_avg = sum(r2_list) / len(r2_list)

            print_benchmark_summary(dataset_name, t1_avg, t2_avg, r1_avg, r2_avg, runs)

            results.append({
                "Dataset": dataset_name,
                "Time_Original": t1_avg, "Time_Hybrid": t2_avg,
                "Ratio_Original": r1_avg, "Ratio_Hybrid": r2_avg
            })
            pd.DataFrame(results).to_csv(results_file, index=False)

    plot_results(results_file, plot_file)

# --- Mode 2: Local File Benchmark ---
def run_local_suite(file_path, samples, escape, interval, runs, discard_summaries, bCandidates):
    if not os.path.exists(file_path):
        print(f"[!] Error: Cannot find file '{file_path}'")
        return

    filename = os.path.basename(file_path)
    dataset_name = filename.replace(".txt", "")
    results_file = os.path.join(BENCHMARK_DIR, f"local_{filename}_results.csv")
    plot_file = os.path.join(BENCHMARK_DIR, f"local_{filename}_comparison.pdf")

    print(f"\n=== BENCHMARKING LOCAL FILE: {filename} ===")

    t1_list, r1_list = run_multiple_mosso(JAR_ORIGINAL, file_path, f"orig_{dataset_name}", 120, 3, interval, runs, discard_summaries)
    t2_list, r2_list = run_multiple_mosso(JAR_HYBRID, file_path, f"hyb_{dataset_name}", samples, escape, interval, runs, discard_summaries, bCandidates)

    if not t1_list or not t2_list:
        print("[!] Not enough successful runs to generate summary.")
        return

    if runs > 1:
        # Pass RUNS_DIR into the plotter function!
        plot_runs_variance(dataset_name, t1_list, t2_list, r1_list, r2_list, RUNS_DIR)

    t1_avg = sum(t1_list) / len(t1_list)
    r1_avg = sum(r1_list) / len(r1_list)
    t2_avg = sum(t2_list) / len(t2_list)
    r2_avg = sum(r2_list) / len(r2_list)

    print_benchmark_summary(dataset_name, t1_avg, t2_avg, r1_avg, r2_avg, runs)

    df = pd.DataFrame([{
        "Dataset": dataset_name,
        "Time_Original": t1_avg, "Time_Hybrid": t2_avg,
        "Ratio_Original": r1_avg, "Ratio_Hybrid": r2_avg
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
    parser.add_argument("--b", type=int, default=5,
                        help="Number of top candidates to consider (Default: 5)")
    parser.add_argument("--interval", type=int, default=1000,
                        help="How often the Java code prints progress (Default: 1000)")
    parser.add_argument("--runs", type=int, default=1,
                        help="Number of test iterations to average out inconsistencies (Default: 1)")
    parser.add_argument("--keep-summaries", action="store_true",
                        help="Save the generated summary graph files (Default is to discard them to save space).")

    args = parser.parse_args()

    if not args.skip_build:
        build_jars()

    setup_directories()

    discard = not args.keep_summaries

    if args.mode == "remote":
        run_remote_suite(args.samples, args.escape, args.interval, args.runs, discard, args.b)
    elif args.mode == "local":
        if not args.file:
            print("[!] Error: You must provide a --file argument when using --mode local.")
            return
        run_local_suite(args.file, args.samples, args.escape, args.interval, args.runs, discard, args.b)

if __name__ == "__main__":
    main()