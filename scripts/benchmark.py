import argparse
import subprocess
import os
import re
import shutil
import pandas as pd

from plotter import plot_results, plot_runs_variance
from utils import (
    setup_directories, build_jars, download_and_prepare_dataset, prepare_local_dataset,
    get_fastutil_path, BENCHMARK_DIR, RUNS_DIR, SUMMARIZED_DIR, JAR_ORIGINAL, JAR_HYBRID
)

# Default Benchmarking Datasets
datasets = {
    "small": [
        ("https://snap.stanford.edu/data/as-caida20071105.txt.gz", "as-caida20071105.txt"),
        ("https://snap.stanford.edu/data/email-Enron.txt.gz", "Email-Enron.txt"),
        ("https://snap.stanford.edu/data/loc-brightkite_edges.txt.gz", "Brightkite_edges.txt"),
        ("https://snap.stanford.edu/data/email-EuAll.txt.gz", "Email-EuAll.txt"),
        ("https://snap.stanford.edu/data/soc-Slashdot0902.txt.gz", "Slashdot0902.txt"),
        ("https://snap.stanford.edu/data/bigdata/communities/com-dblp.ungraph.txt.gz", "com-dblp.ungraph.txt")
    ],
    "large": [
        ("https://snap.stanford.edu/data/amazon0601.txt.gz", "amazon0601.txt"),
        ("https://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz", "com-youtube.ungraph.txt"),
        ("https://snap.stanford.edu/data/as-skitter.txt.gz", "as-skitter.txt"),
        ("https://snap.stanford.edu/data/bigdata/communities/com-lj.ungraph.txt.gz", "com-lj.ungraph.txt")
    ]
}

def run_multiple_mosso(jar_file, dataset_path, output_name, samples, escape, interval, runs, discard_summaries):
    times, ratios = [], []
    for i in range(runs):
        if runs > 1:
            print(f"\n    [Iteration {i+1}/{runs}]")
        t, r = run_mosso(jar_file, dataset_path, f"{output_name}_run{i+1}", samples, escape, interval, discard_summaries)
        if t is not None and r is not None:
            times.append(t)
            ratios.append(r)

    return times, ratios

def run_mosso(jar_file, dataset_path, output_name, samples, escape, interval, discard_summaries):
    classpath = f"{get_fastutil_path()}{os.pathsep}{jar_file}"
    out_file = os.path.join(RUNS_DIR, output_name)
    log_file = f"{out_file}.log"

    cmd = [
        "java",
        "-cp", classpath, "mosso.Run",
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

        t = float(time_m.group(1)) if time_m else None
        r = float(ratio_m.group(1)) if ratio_m else None

        return t, r
    except Exception as e:
        print(f"[!] Execution failed: {e}")
        return None, None

def print_benchmark_summary(dataset_name, t_orig, t_hyb, r_orig, r_hyb, runs):
    summary_lines = [f"\n" + "=" * 60, f" FINAL RESULTS: {dataset_name} (Average of {runs} runs)", "=" * 60]

    if None in (t_orig, t_hyb, r_orig, r_hyb):
        summary_lines.append(" [!] One or more runs failed. Cannot calculate differences.")
        summary_lines.append("="*60 + "\n")
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
        summary_lines.append(f"   -> Hybrid is {abs(r_diff):.6f} {r_status} ({abs(r_pct):.4f}%)\n")

        summary_lines.append("="*60 + "\n")

    summary_text = "\n".join(summary_lines)
    print(summary_text)

    summary_file = os.path.join(BENCHMARK_DIR, f"{dataset_name}_summary.txt")
    with open(summary_file, "w") as f:
        f.write(summary_text)
    print(f"[*] Summary report saved to {summary_file}")

def run_remote_suite(samples, escape, interval, runs, discard_summaries):
    results_file = os.path.join(BENCHMARK_DIR, "remote_results.csv")
    plot_file = os.path.join(BENCHMARK_DIR, "remote_comparison.pdf")
    results = []

    for category, data_list in datasets.items():
        print(f"\n=== BENCHMARK SUITE: {category.upper()} ===")
        for url, filename in data_list:
            dataset_name = filename.replace(".txt", "")
            path = download_and_prepare_dataset(url, filename)

            t1_list, r1_list = run_multiple_mosso(JAR_ORIGINAL, path, f"orig_{dataset_name}", 120, 3, interval, runs, discard_summaries)
            t2_list, r2_list = run_multiple_mosso(JAR_HYBRID, path, f"hyb_{dataset_name}", samples, escape, interval, runs, discard_summaries)

            if not t1_list or not t2_list:
                print("[!] Not enough successful runs to generate summary.")
                continue

            if runs > 1:
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

def run_local_suite(file_path, samples, escape, interval, runs, discard_summaries):
    if not os.path.exists(file_path):
        print(f"[!] Error: Cannot find file '{file_path}'")
        return

    # Auto-prepare the local file for MoSSo formatting
    prepared_path = prepare_local_dataset(file_path)

    filename = os.path.basename(file_path)
    dataset_name = filename.replace(".txt", "").replace(".csv", "")
    results_file = os.path.join(BENCHMARK_DIR, f"local_{dataset_name}_results.csv")
    plot_file = os.path.join(BENCHMARK_DIR, f"local_{dataset_name}_comparison.pdf")

    print(f"\n=== BENCHMARKING LOCAL FILE: {filename} ===")

    t1_list, r1_list = run_multiple_mosso(JAR_ORIGINAL, prepared_path, f"orig_{dataset_name}", 120, 3, interval, runs, discard_summaries)
    t2_list, r2_list = run_multiple_mosso(JAR_HYBRID, prepared_path, f"hyb_{dataset_name}", samples, escape, interval, runs, discard_summaries)

    if not t1_list or not t2_list:
        print("[!] Not enough successful runs to generate summary.")
        return

    if runs > 1:
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

    # Mode is removed. --file is now optional.
    parser.add_argument("--file", type=str,
                        help="Optional: Path to a specific local graph file. If omitted, runs the default dataset suite.")

    parser.add_argument("--skip-build", action="store_true",
                        help="Skip downloading and recompiling the Java JAR files.")
    parser.add_argument("--samples", type=int, default=120,
                        help="Number of random neighbors to sample (Default: 120)")
    parser.add_argument("--escape", type=int, default=3,
                        help="Escape probability parameter (Default: 3)")
    parser.add_argument("--interval", type=int, default=1000,
                        help="How often the Java code prints progress (Default: 1000)")
    parser.add_argument("--runs", type=int, default=1,
                        help="Number of test iterations to average out inconsistencies (Default: 1)")
    parser.add_argument("--keep-summaries", action="store_true",
                        help="Save the generated summary graph files (Default is to discard them to save space).")

    args = parser.parse_args()

    build_jars(args.skip_build)
    setup_directories()
    discard = not args.keep_summaries

    if args.file:
        run_local_suite(args.file, args.samples, args.escape, args.interval, args.runs, discard)
    else:
        run_remote_suite(args.samples, args.escape, args.interval, args.runs, discard)

if __name__ == "__main__":
    main()