import os
from abc import ABC, abstractmethod
from datetime import datetime

from tabulate import tabulate

from scripts.config import PARAM_CONFIG, ALGORITHMS, DATASETS, BENCHMARK_DIR
from scripts.utils import setup_logging, setup_directories, get_datasets_to_run, download_and_prepare_dataset
from scripts.runners import get_runner
import argparse

class Benchmark(ABC):
    def __init__(self, benchmark_type):
        self.benchmark_type = benchmark_type
        self.results = []
        self.datasets_to_run = None

        self.args = None
        self._parse_arguments()

        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_dir = os.path.join(BENCHMARK_DIR, self.benchmark_type, self.get_session_name())
        self.runs_dir = os.path.join(self.session_dir, "runs")
        self.summaries_dir = os.path.join(self.session_dir, "summarized_graphs")

        os.makedirs(self.session_dir, exist_ok=True)
        os.makedirs(self.runs_dir, exist_ok=True)
        os.makedirs(self.summaries_dir, exist_ok=True)

        log_file = os.path.join(self.session_dir, "execution.log")
        self.logger = setup_logging(log_file)

    def get_session_name(self):
        """Hook to allow subclasses to name the folder (e.g., sweep_c_2026...)"""
        return f"run_{self.timestamp}"

    def _parse_arguments(self):
        """Builds the parser, collects custom args, and parses them."""
        parser = argparse.ArgumentParser()
        parser.add_argument("--file", type=str, help="Specific local graph file.")
        parser.add_argument("--runs", type=int, default=1)
        parser.add_argument("--group", choices=["all"] + list(DATASETS.keys()), default="all")
        parser.add_argument("--algorithm", nargs='+', help="Specific algorithms to run")
        parser.add_argument("--baseline", type=str, help="Algorithm for relative comparisons")

        for p_name, p_data in PARAM_CONFIG.items():
            parser.add_argument(f"--{p_name}", type=type(p_data["default"]), default=p_data["default"])

        self.add_custom_args(parser)

        args = parser.parse_args()

        if args.algorithm:
            self.active_algos = {k: v for k, v in ALGORITHMS.items() if k in args.algorithm}
        else:
            self.active_algos = {k: v for k, v in ALGORITHMS.items() if k != "local"}

        if args.baseline and args.baseline not in ALGORITHMS:
            print(f"[!] The specified baseline '{args.baseline}' is not in the active algorithms list.")
            exit(1)

        self.args = args

    @abstractmethod
    def add_custom_args(self, parser):
        """Subclasses use the passed 'parser' to add specific arguments here."""
        pass

    @abstractmethod
    def process(self, dataset_path: str):
        """Logic for running the benchmark."""
        pass

    @abstractmethod
    def finalize(self):
        """Logic for saving artifacts."""
        pass

    @abstractmethod
    def print_table(self):
        """Logic for formatting and printing the results table to the console."""
        pass

    def execute_runner(self, algo_name, algo_config, dataset_path, dataset_name, resolved_params):
        """A helper method to standardize runner execution across subclasses."""
        runner = get_runner(algo_name, algo_config, self.logger, self.runs_dir, self.summaries_dir)
        if not runner.binary_exists():
            self.logger.warning(f"[!] Binary not found for {algo_name}. Skipping.")
            return None, None, None, None

        template = algo_config.get('template', [])

        return runner.run_multiple(
            dataset_path=dataset_path,
            base_output_name=f"{algo_name}_{dataset_name}_{self.timestamp}",
            runs=self.args.runs,
            parameters=resolved_params,
            template=template
        )

    def setup(self):
        self.datasets_to_run = get_datasets_to_run(self.args)
        setup_directories()

        self.logger.info(f"[*] Session Directory Created: {self.session_dir}")
        self.logger.info("[*] Compiling configured algorithms...")

        for algo_name, config in self.active_algos.items():
            self.logger.info(f"\tBuilding {algo_name}")
            runner = get_runner(algo_name, config, self.logger, self.runs_dir, self.summaries_dir)
            runner.build()

    def get_algo_param_display(self, p_key, default_val):
        """Hook method. Allows subclasses to override parameter display formatting."""
        return default_val

    def print_parameters(self):
        general_args = [[k, v] for k, v in vars(self.args).items() if k not in PARAM_CONFIG]
        self.logger.info("\n[*] General Parameters:")
        self.logger.info(tabulate(general_args, headers=["Argument", "Value"], tablefmt="simple"))

        for algo_name, algo_config in self.active_algos.items():
            template = algo_config.get('template', [])
            params = algo_config.get('params', {})

            algo_data = []
            for p_key in template:
                if p_key in params:
                    algo_data.append([p_key, params[p_key], "FIXED"])
                else:
                    base_val = getattr(self.args, p_key, "N/A")
                    display_val = self.get_algo_param_display(p_key, base_val)
                    algo_data.append([p_key, display_val, "DYNAMIC"])

            if algo_data:
                self.logger.info(f"\n[*] Hyperparameters: {algo_name}")
                self.logger.info(tabulate(algo_data, headers=["Param", "Value", "State"], tablefmt="simple"))

        self.logger.info(f"[*] Datasets to Run ({len(self.datasets_to_run)}):")
        for url, filename in self.datasets_to_run:
            self.logger.info(f" "*4 + f"- {filename}")

    def run(self):
        """The main execution lifecycle."""
        self.logger.info("=" * 10 + f"{' STAGE 1: SETUP ':^30}" + "=" * 10)
        try:
            self.setup()
        except Exception as e:
            self.logger.error(f"[!] Setup aborted: {e}")
            return

        self.print_parameters()

        self.logger.info("=" * 10 + f"{' STAGE 2: PROCESSING ':^30}" + "=" * 10)
        for i, (url, filename) in enumerate(self.datasets_to_run, 1):
            try:
                dataset_name = filename.replace(".txt", "").replace(".csv", "")
                dataset_path = download_and_prepare_dataset(url, filename, self.logger)
                if not dataset_path:
                    raise RuntimeError(f"Failed to download dataset {filename}.")

                self.logger.info(f"[{i}/{len(self.datasets_to_run)}] Benchmarking [{dataset_name}] ({self.args.runs} runs) ...")
                self.process(dataset_path)
            except Exception as e:
                self.logger.error(f"[!] Processing aborted: {e}")
                continue

        if self.results and len(self.results) > 0:
            self.logger.info("=" * 10 + f"{' STAGE 3: RESULTS & ARTIFACTS ':^30}" + "=" * 10)
            try:
                self.print_table()
                self.finalize()
                self.logger.info(f"[*] Artifacts saved to: {self.session_dir}")
            except Exception as e:
                self.logger.error(f"[!] Error during table printing or plotting: {e}")
                # Ultimate fallback: Just dump the raw dictionaries so data isn't lost
                import json
                fallback_path = f"EMERGENCY_DUMP_{self.timestamp}.json"
                with open(fallback_path, "w") as f:
                    json.dump(self.results, f, indent=4)
                self.logger.warning(f"[*] Saved raw fallback data to {fallback_path}")
        else:
            self.logger.warning("[!] No results generated. Nothing to save.")