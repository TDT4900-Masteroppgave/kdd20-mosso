import os
import pandas as pd
from tabulate import tabulate

from config import BENCHMARK_DIR, RUNS_DIR, PARAM_CONFIG
from utils import download_and_prepare_dataset, prepare_dataset, format_dataframe_with_baseline, run_multiple_mosso
from plotter import plot_results, plot_runs_variance
from benchmark import Benchmark

class CompareBenchmark(Benchmark):
    def __init__(self):
        super().__init__("compare", BENCHMARK_DIR)

    def add_custom_args(self, parser):
        parser.add_argument("--keep-summaries", action="store_true")

    def get_log_prefix(self):
        return "compare"

    def process(self):
        args = self.args
        logger = self.logger
        datasets_to_run = self.datasets_to_run
        timestamp = self.timestamp

        results = []

        for i, (url, filename) in enumerate(datasets_to_run, 1):
            dataset_name = filename.replace(".txt", "").replace(".csv", "")
            path = prepare_dataset(filename, logger) if url == "local" else download_and_prepare_dataset(url, filename,
                                                                                                         logger)
            if not path: continue

            logger.info(f"[{i}/{len(datasets_to_run)}] Benchmarking [{dataset_name}] ({args.runs} runs) ...")
            current_result = {"Dataset": dataset_name}
            all_times_dict, all_ratios_dict = {}, {}

            for algo_name, algo_config in self.active_algos.items():
                jar_file = f"mosso-{algo_name}.jar"
                if not os.path.exists(jar_file):
                    continue

                template = algo_config.get('template')
                params = algo_config.get('params', {})

                resolved_params = {"interval": params.get('interval', args.interval)}
                for p_key in PARAM_CONFIG.keys():
                    resolved_params[p_key] = params.get(p_key, getattr(args, p_key))

                t_avg, r_avg, t_list, r_list = run_multiple_mosso(
                    jar_file, path, f"{algo_name}_{dataset_name}_{timestamp}",
                    args.runs, not args.keep_summaries, logger, resolved_params, template
                )

                if t_avg is not None:
                    current_result[f"Time_{algo_name}"], current_result[f"Ratio_{algo_name}"] = t_avg, r_avg
                    logger.info(f"\t=> {algo_name: <12} Time: {t_avg:.3f}s | Ratio: {r_avg:.5f}")
                    if args.runs > 1:
                        all_times_dict[algo_name], all_ratios_dict[algo_name] = t_list, r_list

            results.append(current_result)
            if args.runs > 1:
                plot_runs_variance(f"{dataset_name}_{timestamp}", all_times_dict, all_ratios_dict, RUNS_DIR)

        self.results = results

    def print_table(self):
        df = pd.DataFrame(self.results)
        strategies = [col.replace("Time_", "") for col in df.columns if col.startswith("Time_")]

        avg_row = df.mean(numeric_only=True).to_dict()
        avg_row['Dataset'] = 'AVERAGE'
        df = pd.concat([df, pd.DataFrame([avg_row])], ignore_index=True)

        display_df = format_dataframe_with_baseline(df, strategies, self.args.baseline)
        table_str = tabulate(display_df, headers='keys', tablefmt='grid', showindex=False)
        for line in table_str.split('\n'):
            self.logger.info(line)

    def finalize(self):
        csv_file = os.path.join(self.save_dir, f"results_{self.timestamp}.csv")
        pd.DataFrame(self.results).to_csv(csv_file, index=False)
        plot_results(csv_file, os.path.join(self.save_dir, f"plot_{self.timestamp}.pdf"), self.logger)


if __name__ == "__main__":
    CompareBenchmark().run()
