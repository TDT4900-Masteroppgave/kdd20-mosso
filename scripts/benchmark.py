from abc import ABC, abstractmethod

from config import PARAM_CONFIG, ALGORITHMS, DATASETS
from utils import setup_logging, setup_directories, build_jars, get_datasets_to_run
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
        args.local = False

        if args.algos:
            if "local" in args.algos:
                args.local = True
            for a in args.algos:
                if a not in ALGORITHMS.keys():
                    print(f"[!] Unknown algorithm: {a}. Available options: {list(ALGORITHMS.keys())}")
                    exit(1)
            for key in list(ALGORITHMS.keys()):
                if key not in args.algos:
                    ALGORITHMS.pop(key, None)
        else:
            ALGORITHMS.pop("local", None)

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
        """Logic for setting up the benchmark environment."""
        self.datasets_to_run = get_datasets_to_run(self.args)
        setup_directories()
        build_jars(self.args.local, self.logger)

    def get_algo_param_display(self, p_key, default_val):
        """Hook method. Allows subclasses to override parameter display formatting."""
        return default_val

    def run(self):
        """The main execution lifecycle."""
        self.logger.info("=" * 10 + f"{' STAGE 1: SETUP & COMPILATION ':^30}" + "=" * 10)
        self.setup()

        self.logger.info("[*] Global Execution Parameters:")
        for arg_key, arg_val in vars(self.args).items():
            if arg_key not in PARAM_CONFIG:
                self.logger.info(" "*4 + f"- {arg_key}: {arg_val}")

        for algo_name, algo_config in ALGORITHMS.items():
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

        self.logger.info("=" * 10 + f"{' STAGE 2: PROCESSING ':^30}" + "=" * 10)
        self.process()

        if self.results:
            self.logger.info("=" * 10 + f"{' STAGE 3: RESULTS & ARTIFACTS ':^30}" + "=" * 10)
            self.print_table()
            self.finalize()
            self.logger.info(f"[*] Artifacts saved to: {self.save_dir}")
        else:
            self.logger.warning("[!] No results generated.")