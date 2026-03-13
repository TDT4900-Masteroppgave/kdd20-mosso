import os
import shutil
import subprocess
import re
import urllib.request
import gzip
import glob
import pandas as pd

from config import *
import logging
import sys
from datetime import datetime
from config import LOG_DIR, RUNS_DIR, SUMMARIZED_DIR

def get_fastutil_path():
    fastutil_files = glob.glob("fastutil-*.jar")
    return fastutil_files[0] if fastutil_files else "fastutil-missing.jar"


def setup_directories():
    for d in [DATASETS_DIR, OUTPUT_DIR, BENCHMARK_DIR, RUNS_DIR, SUMMARIZED_DIR, SWEEP_DIR, LOG_DIR, VERSIONS_DIR]:
        os.makedirs(d, exist_ok=True)


def build_jars(is_local, logger, algorithms):
    fastutil = get_fastutil_path()
    if not os.path.exists(fastutil):
        logger.error(f"[!] Error: {fastutil} missing. Download it to root first.")
        exit(1)

    if is_local:
        logger.info("[*] Compiling current Local code...")
        try:
            subprocess.run(["bash", "compile.sh"], cwd=".", check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            shutil.move(f"{PROJECT_NAME}-1.0.jar", f"{PROJECT_NAME}-Local.jar")
            logger.info(f"\t[OK] Successfully built {PROJECT_NAME}-Local.jar")
        except subprocess.CalledProcessError as e:
            logger.error(f"\t[!] Failed to build Local code. Compile Error: {e.stderr.strip()}")
            return
        except Exception as e:
            logger.error(f"\t[!] Unexpected error building Local code: {e}")
            return

    logger.info("[*] Compiling configured algorithms...")

    for algo_name, config in algorithms:
        if algo_name == "local":
            continue

        repo_url = str(config['repo'])
        branch = str(config['branch'])
        jar_name = f"{PROJECT_NAME}-{algo_name}.jar"
        target_dir = str(os.path.join(VERSIONS_DIR, algo_name))

        logger.debug(f"\t-> Building {algo_name} (Repo: {repo_url.split('/')[-1]} | Branch: {branch})...")

        try:
            if not os.path.exists(target_dir):
                subprocess.run(
                    ["git", "clone", "-q", "--branch", branch, "--single-branch", repo_url, target_dir],
                    check=True, stderr=subprocess.PIPE, text=True
                )
            else:
                subprocess.run(["git", "pull", "-q"], cwd=target_dir, check=True, stderr=subprocess.PIPE)

            shutil.copy(fastutil, os.path.join(target_dir, fastutil))
            subprocess.run(["bash", "compile.sh"], cwd=target_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            shutil.move(os.path.join(target_dir, f"{PROJECT_NAME}-1.0.jar"), jar_name)
            logger.info(f"\t[OK] Successfully built {jar_name}")

        except subprocess.CalledProcessError as e:
            logger.error(f"\t[!] Failed to build {algo_name}. Git/Compile Error: {e.stderr.strip()}")
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir) # Clean up broken clones
            return
        except Exception as e:
            logger.error(f"\t[!] Unexpected error building {algo_name}: {e}")
            return


def prepare_dataset(filepath, logger):
    filename = os.path.basename(filepath)
    prepared_path = os.path.join(DATASETS_DIR, f"prepared_{filename}")
    if os.path.exists(prepared_path):
        return prepared_path

    logger.debug(f"Cleaning {filename} (Undirected, No Self-Loops, No Multi-Edges)...")
    seen_edges = set()

    try:
        with open(filepath, 'r') as f_in, open(prepared_path, 'w') as f_out:
            for line in f_in:
                if line.startswith('#'): continue
                parts = line.strip().split()
                if len(parts) >= 2:
                    try:
                        u, v = int(parts[0]), int(parts[1])
                        if u == v: continue
                        edge = tuple(sorted((u, v)))
                        if edge in seen_edges: continue
                        seen_edges.add(edge)
                        f_out.write(f"{u}\t{v}\t1\n")
                    except ValueError:
                        continue
        return prepared_path

    except Exception as e:
        logger.error(f"[!] Failed to prepare local dataset {filename}: {e}")
        if os.path.exists(prepared_path):
            os.remove(prepared_path)
        return None


def download_and_prepare_dataset(url, filename, logger):
    gz_path = os.path.join(DATASETS_DIR, filename + ".gz")
    txt_path = os.path.join(DATASETS_DIR, filename)

    if not os.path.exists(txt_path):
        try:
            if not os.path.exists(gz_path):
                logger.info(f"[*] Downloading {filename}")
                urllib.request.urlretrieve(url, gz_path)

            logger.debug(f"Extracting and cleaning {filename}...")
            seen_edges = set()
            with gzip.open(gz_path, 'rt') as f_in, open(txt_path, 'w') as f_out:
                for line in f_in:
                    if line.startswith('#'): continue
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        try:
                            u, v = int(parts[0]), int(parts[1])
                            if u == v: continue
                            edge = tuple(sorted((u, v)))
                            if edge in seen_edges: continue
                            seen_edges.add(edge)
                            f_out.write(f"{u}\t{v}\t1\n")
                        except ValueError:
                            continue

            os.remove(gz_path)

        except Exception as e:
            logger.error(f"[!] Preparing dataset failed for {filename}: {e}")
            if os.path.exists(txt_path):
                os.remove(txt_path)
            return None

    return txt_path


def setup_logging(run_type):
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # The unique ID
    log_file = os.path.join(LOG_DIR, f"{run_type}_{timestamp}.log")

    logger = logging.getLogger(PROJECT_NAME)
    logger.setLevel(logging.DEBUG)
    logger.handlers = []

    # Console Handler: Clean, concise output
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(message)s'))

    # File Handler: Verbose output for debugging
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    logger.addHandler(ch)
    logger.addHandler(fh)

    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            logger.warning("[!] Execution interrupted by user (KeyboardInterrupt).")
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception

    return logger, timestamp

def format_dataframe_with_baseline(df, strategies, baseline_algo=None):
    """Helper function to calculate inline relative performance factors."""
    display_df = df.copy()

    for strat in strategies:
        time_col, ratio_col = f"Time_{strat}", f"Ratio_{strat}"
        formatted_times, formatted_ratios = [], []

        for _, row in df.iterrows():
            t_val, r_val = row.get(time_col), row.get(ratio_col)

            # If a baseline is provided and this column IS NOT the baseline
            if baseline_algo and baseline_algo in strategies and strat != baseline_algo:
                t_base = row.get(f"Time_{baseline_algo}")
                r_base = row.get(f"Ratio_{baseline_algo}")

                # Format Time (Speedup Factor: > 1.0x is faster, < 1.0x is slower)
                if pd.notna(t_val) and pd.notna(t_base) and t_val > 0:
                    speedup = t_base / t_val
                    formatted_times.append(f"{t_val:.3f}s ({speedup:.2f}x)")
                else:
                    formatted_times.append(f"{t_val:.3f}s" if pd.notna(t_val) else "N/A")

                # Format Ratio (Multiplier: < 1.0x is better compression)
                if pd.notna(r_val) and pd.notna(r_base) and r_base > 0:
                    ratio_mult = r_val / r_base
                    formatted_ratios.append(f"{r_val:.5f} ({ratio_mult:.2f}x)")
                else:
                    formatted_ratios.append(f"{r_val:.5f}" if pd.notna(r_val) else "N/A")

            # Standard formatting if no baseline comparison applies
            else:
                formatted_times.append(f"{t_val:.3f}s" if pd.notna(t_val) else "N/A")
                formatted_ratios.append(f"{r_val:.5f}" if pd.notna(r_val) else "N/A")

        display_df[time_col] = formatted_times
        display_df[ratio_col] = formatted_ratios

    return display_df

def get_datasets_to_run(args):
    """Deciding which datasets to process."""
    datasets_to_run = [("local", args.file)] if args.file else []
    if not args.file:
        if args.group == "all":
            for cat, data_list in DATASETS.items():
                for url, filename in data_list:
                    datasets_to_run.append((url, filename))
        else:
            for url, filename in DATASETS[args.group]:
                datasets_to_run.append((url, filename))
    return datasets_to_run

def run_algorithm(jar_file, dataset_path, output_name, discard_summaries, logger, parameters, template):
    classpath = f"{get_fastutil_path()}{os.pathsep}{jar_file}"
    out_file = os.path.join(RUNS_DIR, output_name)
    log_file = f"{out_file}.log"

    cmd = ["java", "-cp", classpath, JAVA_MAIN_CLASS, dataset_path, output_name, PROJECT_NAME]
    for param_key in template:
        cmd.append(str(parameters[param_key]))

    logger.debug(f"Running: {' '.join(cmd)}")
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        output_lines = []

        with open(log_file, 'w') as log_f:
            for line in process.stdout:
                log_f.write(line)
                output_lines.append(line)

        process.wait()

        if process.returncode != 0:
            logger.error(f"[!] Java error for {output_name}: Code {process.returncode}")
            full_error = "".join(output_lines)
            logger.debug(full_error)
            return None, None

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
        logger.error(f"Execution failed for {output_name}: {e}")
        return None, None

def run_multiple_algorithms(jar_file, dataset_path, output_name, runs, discard_summaries, logger, parameters, template):
    times, ratios = [], []
    for i in range(runs):
        logger.debug(f"Iter {i+1}/{runs} for {output_name}...")
        t, r = run_algorithm(jar_file, dataset_path, f"{output_name}_run{i+1}", discard_summaries, logger, parameters, template)
        if t is not None and r is not None:
            times.append(t)
            ratios.append(r)

    t_avg = sum(times) / len(times) if times else None
    r_avg = sum(ratios) / len(ratios) if ratios else None
    return t_avg, r_avg, times, ratios