from abc import ABC, abstractmethod

from config import PARAM_CONFIG, ALGORITHMS, DATASETS, RUNS_DIR
from utils import setup_logging, setup_directories, get_datasets_to_run
import argparse

class Benchmark(ABC):
    def __init__(self, benchmark_type, save_dir):
        self.save_dir = save_dir
        self.benchmark_type = benchmark_type
        self.results = []
        self.datasets_to_run = None

        self.args = None
        self._parse_arguments()

        self.log_prefix = self.get_log_prefix()
        self.logger, self.timestamp = setup_logging(self.log_prefix)

    def _parse_arguments(self):
        """Builds the parser, collects custom args, and parses them."""
        parser = argparse.ArgumentParser()
        parser.add_argument("--file", type=str, help="Specific local graph file.")
        parser.add_argument("--runs", type=int, default=1)
        parser.add_argument("--group", choices=["all"] + list(DATASETS.keys()), default="all")
        parser.add_argument("--algos", nargs='+', help="Specific algorithms to run")
        parser.add_argument("--baseline", type=str, help="Algorithm for relative comparisons")

        for p_name, p_data in PARAM_CONFIG.items():
            parser.add_argument(f"--{p_name}", type=type(p_data["default"]), default=p_data["default"])

        self.add_custom_args(parser)

        args = parser.parse_args()

        if args.algos:
            self.active_algos = {k: v for k, v in ALGORITHMS.items() if k in args.algos}
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
    def get_log_prefix(self):
        """Returns the log prefix for the benchmark."""
        pass

    @abstractmethod
    def process(self):
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

    def setup(self):
        self.datasets_to_run = get_datasets_to_run(self.args)
        setup_directories()

        from runners import get_runner
        self.logger.info("[*] Compiling configured algorithms...")

        for algo_name, config in self.active_algos.items():
            self.logger.info(f"\tBuilding {algo_name}")
            runner = get_runner(algo_name, config, self.logger, RUNS_DIR)
            runner.build()

    def get_algo_param_display(self, p_key, default_val):
        """Hook method. Allows subclasses to override parameter display formatting."""
        return default_val

    def print_parameters(self):
        for arg_key, arg_val in vars(self.args).items():
            if arg_key not in PARAM_CONFIG:
                self.logger.info(" "*4 + f"- {arg_key}: {arg_val}")

        for algo_name, algo_config in self.active_algos.items():
            self.logger.info(f"[*] Hyperparameters for {algo_name}: ")
            template = algo_config.get('template', [])
            params = algo_config.get('params', {})

            if not template:
                self.logger.info(" "*4 + "(No template defined)")
                continue

            for p_key in template:
                if p_key in params:
                    val = params[p_key]
                    self.logger.info(" "*4 + f"- {p_key}: {val} (FIXED)")
                else:
                    base_val = getattr(self.args, p_key, "N/A")
                    display_val = self.get_algo_param_display(p_key, base_val)
                    self.logger.info(" "*4 + f"- {p_key}: {display_val}")

    def run(self):
        """The main execution lifecycle."""
        self.logger.info("=" * 10 + f"{' STAGE 1: SETUP & COMPILATION ':^30}" + "=" * 10)
        try:
            self.setup()

            self.logger.info("[*] Parameters:")
            self.print_parameters()

            self.logger.info(f"[*] Datasets to Run ({len(self.datasets_to_run)}):")
            for url, filename in self.datasets_to_run:
                self.logger.info(f" "*4 + f"- {filename}")
        except Exception as e:
            self.logger.error(f"[!] Setup aborted: {e}")
            return

        self.logger.info("=" * 10 + f"{' STAGE 2: PROCESSING ':^30}" + "=" * 10)
        try:
            self.process()
        except RuntimeError as e:
            self.logger.error(f"[!] Processing aborted: {e}")
            return

        if self.results and len(self.results[0]) > 1:
            self.logger.info("=" * 10 + f"{' STAGE 3: RESULTS & ARTIFACTS ':^30}" + "=" * 10)
            self.print_table()
            self.finalize()
            self.logger.info(f"[*] Artifacts saved to: {self.save_dir}")
        else:
            self.logger.warning("[!] No results generated.")