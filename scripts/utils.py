import os
import shutil
import stat
import subprocess
import urllib.request
import gzip
import glob

# --- GLOBAL CONFIGURATION ---
ORIGINAL_REPO_URL = "https://github.com/jihoonko/kdd20-mosso"
DATASETS_DIR = "datasets"
OUTPUT_DIR = "output"
EXTERNAL_DIR = "external"
BASELINE_DIR = os.path.join(EXTERNAL_DIR, "kdd20-mosso")
BENCHMARK_DIR = os.path.join(OUTPUT_DIR, "benchmark")
RUNS_DIR = os.path.join(BENCHMARK_DIR, "runs")
SUMMARIZED_DIR = os.path.join(BENCHMARK_DIR, "summarized_graphs")
SWEEP_DIR = os.path.join(OUTPUT_DIR, "parameter_sweep")

JAR_ORIGINAL = "mosso-original.jar"
JAR_HYBRID = "mosso-hybrid.jar"

def get_fastutil_path():
    fastutil_files = glob.glob("fastutil-*.jar")
    return fastutil_files[0] if fastutil_files else "fastutil-missing.jar"

# --- DIRECTORY MANAGEMENT ---
def _on_rm_error(func, path, _):
    try:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except OSError:
        pass

def force_rmtree(path):
    if os.path.exists(path):
        shutil.rmtree(path, onerror=_on_rm_error)

def setup_directories():
    """Creates the required directory structure before running."""
    os.makedirs(DATASETS_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(BENCHMARK_DIR, exist_ok=True)
    os.makedirs(EXTERNAL_DIR, exist_ok=True)
    os.makedirs(RUNS_DIR, exist_ok=True)
    os.makedirs(SUMMARIZED_DIR, exist_ok=True)
    os.makedirs(SWEEP_DIR, exist_ok=True)

# --- JAVA COMPILATION ---
def build_original_jar():
    print(f"\n[*] Compiling Original MoSSo...")

    if not os.path.exists(BASELINE_DIR):
        print(f"[*] Baseline repository not found. Cloning into {BASELINE_DIR}...")
        try:
            subprocess.run(["git", "clone", "-q", ORIGINAL_REPO_URL, BASELINE_DIR], check=True)
        except subprocess.CalledProcessError as e:
            print(f"[!] Failed to clone baseline repository: {e}")
            exit(1)
    else:
        print(f"[*] Baseline repository already exists...")

    try:
        fastutil = get_fastutil_path()
        shutil.copy(fastutil, os.path.join(BASELINE_DIR, fastutil))
        subprocess.run(["bash", "compile.sh"], cwd=BASELINE_DIR, check=True, stdout=subprocess.DEVNULL)
        shutil.move(os.path.join(BASELINE_DIR, "mosso-1.0.jar"), JAR_ORIGINAL)
        print(f"[*] Success! Saved as {JAR_ORIGINAL}")
    except subprocess.CalledProcessError as e:
        print(f"[!] Original compilation failed: {e}")
        exit(1)

def build_local_hybrid_jar():
    print("\n[*] Compiling local Hybrid MoSSo code...")
    try:
        # Calls your compile.sh directly
        subprocess.run(["sh", "compile.sh"], check=True, stdout=subprocess.DEVNULL)
        shutil.move("mosso-1.0.jar", JAR_HYBRID)
        print(f"[*] Success! Saved as {JAR_HYBRID}")
    except subprocess.CalledProcessError as e:
        print(f"[!] Hybrid compilation failed: {e}")
        exit(1)

def build_jars(skip_build=False):
    if skip_build:
        return
    fastutil = get_fastutil_path()
    if not os.path.exists(fastutil):
        print(f"[!] Error: {fastutil} missing. Download it to root first.")
        exit(1)
    build_original_jar()
    build_local_hybrid_jar()

# --- DATASET MANAGEMENT ---
def download_and_prepare_dataset(url, filename):
    gz_path = os.path.join(DATASETS_DIR, filename + ".gz")
    txt_path = os.path.join(DATASETS_DIR, filename)
    if not os.path.exists(txt_path):
        if not os.path.exists(gz_path):
            print(f"[*] Downloading {filename}...")
            urllib.request.urlretrieve(url, gz_path)

        print(f"[*] Converting {filename} (Undirected, No Self-Loops, No Multi-Edges)...")

        seen_edges = set()

        with gzip.open(gz_path, 'rt') as f_in, open(txt_path, 'w') as f_out:
            for line in f_in:
                if line.startswith('#'): continue
                parts = line.strip().split()
                if len(parts) >= 2:
                    try:
                        u, v = int(parts[0]), int(parts[1])
                    except ValueError:
                        continue

                    # 1. Remove self-loops (as per MoSSo paper)
                    if u == v:
                        continue

                        # 2. Ignore direction & remove multiple edges (as per MoSSo paper)
                    # Sorting (u, v) treats (src, dst) and (dst, src) as the same edge
                    edge = tuple(sorted((u, v)))
                    if edge in seen_edges:
                        continue
                    seen_edges.add(edge)

                    # 3. Write using original IDs
                    f_out.write(f"{u}\t{v}\t1\n")

        print(f"[*] Ready: {len(seen_edges):,} unique undirected edges.")
        os.remove(gz_path)
    return txt_path

def prepare_local_dataset(filepath):
    filename = os.path.basename(filepath)
    prepared_path = os.path.join(DATASETS_DIR, f"prepared_{filename}")

    print(f"[*] Cleaning local file {filename} (Undirected, No Self-Loops, No Multi-Edges)...")

    seen_edges = set()

    with open(filepath, 'r') as f_in, open(prepared_path, 'w') as f_out:
        for line in f_in:
            if line.startswith('#'): continue
            parts = line.strip().split()
            if len(parts) >= 2:
                try:
                    u, v = int(parts[0]), int(parts[1])
                except ValueError:
                    continue

                if u == v:
                    continue

                edge = tuple(sorted((u, v)))
                if edge in seen_edges:
                    continue
                seen_edges.add(edge)

                # Write using original IDs
                f_out.write(f"{u}\t{v}\t1\n")

    print(f"[*] Ready: {len(seen_edges):,} unique undirected edges.")
    return prepared_path