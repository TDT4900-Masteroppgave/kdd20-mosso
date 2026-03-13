import os
import subprocess
import shutil
import re
import platform
from abc import ABC, abstractmethod

from config import VERSIONS_DIR, SUMMARIZED_DIR
from utils import get_fastutil_path, retrieve_github_code

class AlgorithmRunner(ABC):
    edge_format_string = "{u}\t{v}\n"

    """Autonomous execution and compilation strategy for algorithms."""
    def __init__(self, algo_name, config, logger, runs_dir):
        self.algo_name = algo_name
        self.config = config
        self.logger = logger
        self.runs_dir = runs_dir

        self.target_dir = self.config.get("target_dir", os.path.join(VERSIONS_DIR, algo_name))
        self.is_local = self.target_dir == "."

    @abstractmethod
    def get_binary_path(self):
        """Hook for subclasses to resolve their exact binary file name."""
        pass

    @abstractmethod
    def compile_logic(self):
        """Hook for subclasses to define specific build steps (CMake vs. Bash)."""
        pass

    def binary_exists(self):
        return os.path.exists(self.get_binary_path())

    def build(self):
        """Template method for building the algorithm."""
        try:
            if not self.is_local:
                repo_url = str(self.config['repo'])
                branch = str(self.config['branch'])
                self.logger.info(f"\t(Repo: {repo_url.split('/')[-1]} | Branch: {branch})")
                retrieve_github_code(self.target_dir, self.algo_name, repo_url, branch, self.logger)

            self.compile_logic()
            self.logger.info(f"\t\t[OK] Successfully built {os.path.basename(self.get_binary_path())}")

        except subprocess.CalledProcessError as e:
            self.logger.error(f"\t\t[!] Failed to build {self.algo_name}. Code {e.returncode}")
            if e.stdout: self.logger.debug(f"STDOUT:\n{e.stdout.strip()}")
            if e.stderr: self.logger.debug(f"STDERR:\n{e.stderr.strip()}")
            if not self.is_local and os.path.exists(self.target_dir):
                shutil.rmtree(self.target_dir) # Clean up broken clones
            raise RuntimeError(f"Compilation failed for {self.algo_name}.") from e

    @abstractmethod
    def build_command(self, dataset_path, graph_output_path, parameters, template):
        pass

    def post_process_run(self, graph_output_path, keep_summaries):
        """Hook for subclasses to handle quirky file saving behavior after execution."""
        pass

    @abstractmethod
    def get_time_ratio(self, output: str):
        pass


    def run_single(self, dataset_path, output_name, parameters, template, keep_summaries=False):
        """Executes a single run internally resolving the correct binary path."""
        log_file_path = os.path.join(self.runs_dir, f"{output_name}.log")

        graph_output_path = os.path.join(SUMMARIZED_DIR, output_name)

        cmd = self.build_command(dataset_path, graph_output_path, parameters, template)
        self.logger.debug(f"Running: {' '.join(cmd)}")

        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            output_lines = []

            with open(log_file_path, 'w') as log_f:
                for line in process.stdout:
                    log_f.write(line)
                    output_lines.append(line)

            process.wait()
            if process.returncode != 0:
                self.logger.error(f"[!] Crash in {output_name} (Code {process.returncode}). "
                                  f"See {log_file_path} for the Java/C++ stack trace.")
                return None, None

            self.post_process_run(graph_output_path, keep_summaries)

            output = "".join(output_lines)
            time_m, ratio_m = self.get_time_ratio(output)

            return time_m, ratio_m

        except Exception as e:
            self.logger.error(f"Execution failed for {output_name}: {e}")
            return None, None

    def format_dataset(self, original_dataset_path: str) -> str:
        """Universal dataset formatter using regex and template strings."""
        basename = os.path.basename(original_dataset_path)
        base_dir = os.path.dirname(original_dataset_path)

        save_dir = os.path.join(base_dir, self.__class__.__name__)
        os.makedirs(save_dir, exist_ok=True)
        formatted_path = os.path.join(save_dir, f"{self.__class__.__name__}_{basename}")

        # Cache check
        if os.path.exists(formatted_path):
            return formatted_path

        self.logger.debug(f"Extracting and cleaning {basename}...")
        self.logger.info(f"\t[*] Formatting dataset for {self.algo_name}: {basename}")

        # Compile the regex defined by the subclass
        pattern = re.compile(r"^\s*(\d+)\s+(\d+)")
        seen_edges = set()
        with open(original_dataset_path, 'r') as f_in, open(formatted_path, 'w') as f_out:
            for line in f_in:
                if line.startswith(('#', '%')): continue

                match = pattern.search(line)
                if match:
                    u, v = int(match.group(1)), int(match.group(2))
                    if u == v: continue # Remove self-loops

                    edge = tuple(sorted((u, v)))
                    if edge in seen_edges: continue
                    seen_edges.add(edge)

                    f_out.write(self.edge_format_string.format(u=u, v=v))

        return formatted_path

    def run_multiple(self, dataset_path, base_output_name, runs, parameters, template, keep_summaries=False):
        format_dataset_path = self.format_dataset(dataset_path)

        times, ratios = [], []
        for i in range(runs):
            self.logger.debug(f"Iter {i+1}/{runs} for {base_output_name}...")
            t, r = self.run_single(format_dataset_path, f"{base_output_name}_run{i+1}", parameters, template, keep_summaries)
            if t is not None and r is not None:
                times.append(t)
                ratios.append(r)

        return (sum(times)/len(times) if times else None), (sum(ratios)/len(ratios) if ratios else None), times, ratios


# --- CONCRETE IMPLEMENTATIONS ---

class MoSSoRunner(AlgorithmRunner):
    edge_format_string = "{u}\t{v}\t1\n"

    def __init__(self, algo_name, config, logger, runs_dir):
        super().__init__(algo_name, config, logger, runs_dir)
        self.fastutil_path = get_fastutil_path()

    def get_binary_path(self):
        default_name = f"mosso-{self.algo_name}.jar"
        binary_file = self.config.get('binary_file', default_name)
        return os.path.join(self.target_dir, binary_file)

    def get_time_ratio(self, output: str):
        time_m = re.search(r"Execution time:\s*([\d.]+)s", output, re.IGNORECASE)
        ratio_m = re.search(r"Expected Compression Ratio:\s*([\d.]+)", output, re.IGNORECASE)
        return float(time_m.group(1)) if time_m else None, float(ratio_m.group(1)) if ratio_m else None

    def compile_logic(self):
        if not self.is_local:
            shutil.copy(self.fastutil_path, os.path.join(self.target_dir, os.path.basename(self.fastutil_path)))
        subprocess.run(["bash", "compile.sh"], cwd=self.target_dir, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        shutil.move(os.path.join(self.target_dir, "mosso-1.0.jar"), self.get_binary_path())

    def build_command(self, dataset_path, graph_output_path, parameters, template):
        classpath = f"{self.fastutil_path}{os.pathsep}{self.get_binary_path()}"

        raw_filename = os.path.basename(graph_output_path)

        cmd = ["java", "-cp", classpath, "mosso.Run", dataset_path, raw_filename, "mosso"]
        for param_key in template:
            cmd.append(str(parameters.get(param_key, "")))
        return cmd

    def post_process_run(self, graph_output_path, keep_summaries):
        raw_filename = os.path.basename(graph_output_path)
        java_output_file = os.path.join("output", raw_filename)

        if not keep_summaries:
            os.remove(java_output_file)

        if os.path.exists(java_output_file):
            shutil.move(java_output_file, graph_output_path)


class MagsRunner(AlgorithmRunner):
    def get_binary_path(self):
        binary_file = self.config.get('binary_file', self.algo_name)

        if platform.system() == "Windows" and not binary_file.endswith(".exe"):
            binary_file += ".exe"
        return os.path.join(self.target_dir, binary_file)

    def get_time_ratio(self, output: str):
        read_match = re.search(r"read:\s*([\d.]+)\(s\)", output, re.IGNORECASE)
        merge_match = re.search(r"merge:\s*([\d.]+)\(s\)", output, re.IGNORECASE)
        encoding_match = re.search(r"encoding:\s*([\d.]+)\(s\)", output, re.IGNORECASE)

        ratio_match = re.search(r"relative size:\s*\d+/\d+\s*=\s*([\d.]+)", output, re.IGNORECASE)

        time_m = None
        if read_match and merge_match and encoding_match:
            time_m = float(read_match.group(1)) + float(merge_match.group(1)) + float(encoding_match.group(1))

        ratio_m = None
        if ratio_match:
            ratio_m = float(ratio_match.group(1))

        return time_m, ratio_m

    class MagsRunner(AlgorithmRunner):
        def get_binary_path(self):
            binary_file = self.config.get('binary_file', self.algo_name)
            # Windows requires .exe extension for compiled C++ binaries
            if platform.system() == "Windows" and not binary_file.endswith(".exe"):
                binary_file += ".exe"
            return os.path.join(self.target_dir, binary_file)

    def compile_logic(self):
        build_dir = os.path.join(self.target_dir, "build")
        os.makedirs(build_dir, exist_ok=True)
        env = os.environ.copy()

        # Inject Maximum Optimization Flags
        if platform.system() != "Windows":
            current_cxxflags = env.get("CXXFLAGS", "")
            env["CXXFLAGS"] = f"{current_cxxflags} -O3"

        # macOS Workaround for OpenMP via Homebrew LLVM
        if platform.system() == "Darwin":
            if os.path.exists("/opt/homebrew/opt/llvm/bin/clang++"):
                brew_prefix = "/opt/homebrew"
            elif os.path.exists("/usr/local/opt/llvm/bin/clang++"):
                brew_prefix = "/usr/local"
            else:
                brew_prefix = None

            if brew_prefix:
                # Add Clang to PATH so modern CMake finds it instantly
                llvm_bin = f"{brew_prefix}/opt/llvm/bin"
                env["PATH"] = f"{llvm_bin}:{env.get('PATH', '')}"
                env["CC"] = f"{llvm_bin}/clang"
                env["CXX"] = f"{llvm_bin}/clang++"

        # Hotfix: Correct upstream typo in MAGS C++
        pgsum_path = os.path.join(self.target_dir, "src", "pgsum.cpp")
        if os.path.exists(pgsum_path):
            with open(pgsum_path, "r", encoding="utf-8") as f:
                content = f.read()
            if "#pragma omp barier" in content:
                with open(pgsum_path, "w", encoding="utf-8") as f:
                    f.write(content.replace("#pragma omp barier", "#pragma omp barrier"))

        # --- THE NUCLEAR OPTION: Dynamically Generate a Modern CMakeLists.txt ---
        binary_file = self.config.get('binary_file', self.algo_name)

        # Dynamically map to run/run_mags.cpp or run/run_mags_dm.cpp
        run_file = f"run/run_{binary_file}.cpp"

        cmake_content = f"""
cmake_minimum_required(VERSION 3.10)
project({self.algo_name} LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

find_package(OpenMP REQUIRED)

add_executable({binary_file}
    src/util.cpp
    src/graph.cpp
    src/gsum.cpp
    src/pgsum.cpp
    {run_file}
)

target_include_directories({binary_file} PRIVATE src src/parallel_hashmap)
target_link_libraries({binary_file} PRIVATE OpenMP::OpenMP_CXX)

if(MSVC)
    target_compile_options({binary_file} PRIVATE /W0 /openmp:llvm /O2)
else()
    target_compile_options({binary_file} PRIVATE -w -03)
endif()
"""
        cmake_path = os.path.join(self.target_dir, "CMakeLists.txt")
        with open(cmake_path, "w", encoding="utf-8") as f:
            f.write(cmake_content.strip() + "\n")

        # 1. Platform-Agnostic CMake Configuration
        subprocess.run(["cmake", "..", "-DCMAKE_BUILD_TYPE=Release"], cwd=build_dir, env=env, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # 2. Platform-Agnostic Build (Replaces "make")
        subprocess.run(["cmake", "--build", ".", "--config", "Release"], cwd=build_dir, env=env, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # 3. Locate the binary (Windows MSVC puts it in a Release/ subfolder, Unix leaves it in build/)
        binary_name = os.path.basename(self.get_binary_path())
        compiled_binary_unix = os.path.join(build_dir, binary_name)
        compiled_binary_win = os.path.join(build_dir, "Release", binary_name)

        if os.path.exists(compiled_binary_win):
            shutil.move(compiled_binary_win, self.get_binary_path())
        elif os.path.exists(compiled_binary_unix):
            shutil.move(compiled_binary_unix, self.get_binary_path())
        else:
            raise FileNotFoundError(f"Expected compiled binary not found at {compiled_binary_unix} or {compiled_binary_win}")

    def build_command(self, dataset_path, graph_output_path, parameters, template):
        executable = os.path.abspath(self.get_binary_path())

        cmd = [executable, dataset_path]
        for param_key in template:
            cmd.append(str(parameters.get(param_key, "")))
        return cmd

def get_runner(algo_name, config, logger, runs_dir):
    """Instantiates the correct runner based on the config's language tag."""
    if config.get("lang", "java") == "cpp":
        return MagsRunner(algo_name, config, logger, runs_dir)
    elif config.get("lang", "java") == "java":
        return MoSSoRunner(algo_name, config, logger, runs_dir)

    return None