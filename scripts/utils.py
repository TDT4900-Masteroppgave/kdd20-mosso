import os
import shutil
import subprocess
import urllib.request
import gzip
import glob
from config import *
import logging
import sys
from datetime import datetime
from config import LOG_DIR

def get_fastutil_path():
    fastutil_files = glob.glob("fastutil-*.jar")
    return fastutil_files[0] if fastutil_files else "fastutil-missing.jar"

def setup_directories():
    for d in [DATASETS_DIR, OUTPUT_DIR, BENCHMARK_DIR, EXTERNAL_DIR, RUNS_DIR, SUMMARIZED_DIR, SWEEP_DIR, LOG_DIR]:
        os.makedirs(d, exist_ok=True)

def build_jars(skip_build, logger):
    if skip_build:
        return

    fastutil = get_fastutil_path()
    if not os.path.exists(fastutil):
        logger.error(f"[!] Error: {fastutil} missing. Download it to root first.")
        exit(1)

    logger.debug("Compiling Original MoSSo...")
    if not os.path.exists(BASELINE_DIR):
        subprocess.run(["git", "clone", "-q", ORIGINAL_REPO_URL, BASELINE_DIR], check=True)

    shutil.copy(fastutil, os.path.join(BASELINE_DIR, fastutil))

    subprocess.run(["bash", "compile.sh"], cwd=BASELINE_DIR, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    shutil.move(os.path.join(BASELINE_DIR, "mosso-1.0.jar"), JAR_ORIGINAL)

    logger.debug("Compiling Hybrid MoSSo...")
    subprocess.run(["bash", "compile.sh"], check=True, stdout=subprocess.DEVNULL)
    shutil.move("mosso-1.0.jar", JAR_HYBRID)
    logger.info("[*] Java compilation successful.")

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
                logger.info(f"[*] Downloading {filename}...")
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

def setup_logging(run_type="benchmark"):
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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

    # Intercept uncaught exceptions and log them
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            logger.warning("\n[!] Execution interrupted by user (KeyboardInterrupt).")
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception
    return logger, log_file