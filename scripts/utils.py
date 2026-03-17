import os
import subprocess
import urllib.request
import gzip
import glob
import pandas as pd

from scripts.config import *
import logging
import sys
from datetime import datetime
from scripts.config import LOG_DIR, RUNS_DIR, SUMMARIZED_DIR

def get_fastutil_path():
    fastutil_files = glob.glob("fastutil-*.jar")
    return fastutil_files[0] if fastutil_files else "fastutil-missing.jar"


def setup_directories():
    for d in [DATASETS_DIR, OUTPUT_DIR, COMPARE_DIR, RUNS_DIR, SUMMARIZED_DIR, SWEEP_DIR, LOG_DIR, VERSIONS_DIR]:
        os.makedirs(d, exist_ok=True)

def retrieve_github_code(target_dir: str, algo_name: str, repo_url: str, branch: str, logger):
    try:
        if not os.path.exists(target_dir):
            logger.info(f"\t\t[*] Directory for {algo_name} did not exist")
            logger.info(f"\t\t[*] Cloning {repo_url}")

            subprocess.run(["git", "clone", "-q", "--branch", branch, "--single-branch", repo_url, target_dir],
                            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        else:
            logger.info(f"\t\t[*] Directory for {algo_name} already exist")
            logger.info(f"\t\t[*] Pulling {repo_url}")
            subprocess.run(["git", "pull", "-q"], cwd=target_dir,
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    except subprocess.CalledProcessError as e:
        raise e

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
            with gzip.open(gz_path, 'rt') as f_in, open(txt_path, 'w') as f_out:
                for line in f_in:
                    f_out.write(line)
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

    logger = logging.getLogger("MoSSo")
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

            if baseline_algo and baseline_algo in strategies and strat != baseline_algo:
                t_base = row.get(f"Time_{baseline_algo}")
                r_base = row.get(f"Ratio_{baseline_algo}")

                if pd.notna(t_val) and pd.notna(t_base) and t_val > 0:
                    speedup = t_base / t_val
                    formatted_times.append(f"{t_val:.3f}s ({speedup:.2f}x)")
                else:
                    formatted_times.append(f"{t_val:.3f}s" if pd.notna(t_val) else "N/A")

                if pd.notna(r_val) and pd.notna(r_base) and r_base > 0:
                    ratio_mult = r_val / r_base
                    formatted_ratios.append(f"{r_val:.5f} ({ratio_mult:.2f}x)")
                else:
                    formatted_ratios.append(f"{r_val:.5f}" if pd.notna(r_val) else "N/A")

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