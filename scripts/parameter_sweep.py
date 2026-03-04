import os
import pandas as pd
from tabulate import tabulate

from config import SWEEP_DIR, PARAM_CONFIG
from utils import download_and_prepare_dataset, prepare_dataset, format_dataframe_with_baseline, run_multiple_mosso
from plotter import plot_parameter_analysis
from benchmark import Benchmark

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
        args = self.args
        logger = self.logger
        datasets_to_run = self.datasets_to_run
        timestamp = self.timestamp
        param = self.args.param

        results = []

        for val in self.sweep_values:
            logger.info(f"--- Testing {param.upper()} = {val} ---")

            for i, (url, filename) in enumerate(datasets_to_run, 1):
                dataset_name = filename.replace(".txt", "").replace(".csv", "")
                path = prepare_dataset(filename, logger) if url == "local" else download_and_prepare_dataset(url,
                                                                                                             filename,
                                                                                                             logger)
                if not path: continue

                logger.info(f"[{i}/{len(datasets_to_run)}] Running {dataset_name} ({args.runs} runs) ...")
                current_result = {"Dataset": dataset_name, param: val}

                for algo_name, algo_config in self.active_algos.items():
                    jar_file = f"mosso-{algo_name}.jar"
                    if not os.path.exists(jar_file): continue

                    template = algo_config.get('template', [])
                    params = algo_config.get('params', {})

                    resolved_params = {
                        "interval": params.get('interval', args.interval)
                    }
                    for p_key in PARAM_CONFIG.keys():
                        current_fallback = val if param == p_key else getattr(args, p_key)
                        resolved_params[p_key] = params.get(p_key, current_fallback)

                    t, r, _, _ = run_multiple_mosso(
                        jar_file, path, f"{algo_name}_{dataset_name}_{param}{val}_{timestamp}",
                        args.runs, True, logger, resolved_params, template)

                    if t is not None:
                        current_result[f"Time_{algo_name}"], current_result[f"Ratio_{algo_name}"] = t, r
                        logger.info(f"\t=> {algo_name: <12} Time: {t:.3f}s | Ratio: {r:.5f}")

                results.append(current_result)

            if results:
                master_csv = os.path.join(SWEEP_DIR, f"sweep_{param}_results_{timestamp}.csv")
                pd.DataFrame(results).to_csv(master_csv, index=False)
                plot_parameter_analysis(master_csv, param,
                                        os.path.join(SWEEP_DIR, f"sweep_{param}_plot_{timestamp}.pdf"))

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
        pass


if __name__ == "__main__":
    ParameterSweepBenchmark().run()
