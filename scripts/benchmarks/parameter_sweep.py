import os
import pandas as pd
from tabulate import tabulate

from scripts.config import SWEEP_DIR, PARAM_CONFIG
from scripts.utils import download_and_prepare_dataset, prepare_dataset, format_dataframe_with_baseline, run_multiple
from scripts.plotter import plot_parameter_analysis
from scripts.benchmark import Benchmark

class ParameterSweepBenchmark(Benchmark):
    def __init__(self):
        super().__init__("sweep", SWEEP_DIR)
        config = PARAM_CONFIG[self.args.param]
        self.sweep_values = list(range(*self.args.range)) if self.args.range else (
            self.args.values if self.args.values else config["values"])

    def add_custom_args(self, parser):
        parser.add_argument("--param", choices=list(PARAM_CONFIG.keys()), required=True)
        parser.add_argument("--range", type=int, nargs=3)

    def get_log_prefix(self):
        return f"sweep_{self.args.param}"

    def get_algo_param_display(self, p_key, default_val):
        if self.args.param == p_key:
            range_str = self.args.range if self.args.range else self.args.values
            return f"SWEEPING {range_str}"
        return default_val

    def process(self):
        results = []
        for val in self.sweep_values:
            self.logger.info(f"--- Testing {self.args.param.upper()} = {val} ---")

            for i, (url, filename) in enumerate(self.datasets_to_run, 1):
                dataset_name = filename.replace(".txt", "").replace(".csv", "")
                path = prepare_dataset(filename, self.logger) if url == "local" else download_and_prepare_dataset(url,
                                                                                                             filename,
                                                                                                             self.logger)
                if not path: continue

                self.logger.info(f"[{i}/{len(self.datasets_to_run)}] Running {dataset_name} ({self.args.runs} runs) ...")
                current_result = {"Dataset": dataset_name, self.args.param: val}

                for algo_name, config in self.active_algos.items():
                    binary_file = config.get('binary_file')
                    if not os.path.exists(binary_file): continue

                    template = config.get('template', [])
                    params = config.get('params', {})

                    resolved_params = {
                        "interval": params.get('interval', self.args.interval)
                    }
                    for p_key in PARAM_CONFIG.keys():
                        current_fallback = val if self.args.param == p_key else getattr(self.args, p_key)
                        resolved_params[p_key] = params.get(p_key, current_fallback)

                    t, r, _, _ = run_multiple(
                        binary_file, path, f"{algo_name}_{dataset_name}_{self.args.param}{val}_{self.timestamp}",
                        self.args.runs, True, self.logger, resolved_params, template)

                    if t is not None:
                        current_result[f"Time_{algo_name}"], current_result[f"Ratio_{algo_name}"] = t, r
                        self.logger.info(f"\t=> {algo_name: <12} Time: {t:.3f}s | Ratio: {r:.5f}")

                results.append(current_result)
        self.results = results

    def print_table(self):
        df = pd.DataFrame(self.results)
        strategies = [col.replace("Time_", "") for col in df.columns if col.startswith("Time_")]

        self.logger.info(f"--- SWEEP LOG ({self.args.param.upper()}) ---")
        display_df = format_dataframe_with_baseline(df, strategies, self.args.baseline)
        table_str = tabulate(display_df, headers='keys', tablefmt='grid', showindex=False)
        for line in table_str.split('\n'):
            self.logger.info(line)

        self.logger.info(f"--- AVERAGES BY {self.args.param.upper()} ---")
        avg_df = df.groupby(self.args.param).mean(numeric_only=True).reset_index()
        display_avg = format_dataframe_with_baseline(avg_df, strategies, self.args.baseline)
        table_str = tabulate(display_avg, headers='keys', tablefmt='grid', showindex=False)
        for line in table_str.split('\n'):
            self.logger.info(line)

    def finalize(self):
        file_name = f"sweep_{self.args.param}_results_{self.timestamp}"
        master_csv = os.path.join(SWEEP_DIR, f"{file_name}.csv")
        pd.DataFrame(self.results).to_csv(master_csv, index=False)
        plot_parameter_analysis(master_csv, self.args.param, os.path.join(SWEEP_DIR, f"{file_name}.pdf"))


if __name__ == "__main__":
    ParameterSweepBenchmark().run()
