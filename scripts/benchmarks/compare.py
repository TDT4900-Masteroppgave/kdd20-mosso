import os
import pandas as pd
from tabulate import tabulate

from scripts.config import COMPARE_DIR, RUNS_DIR, PARAM_CONFIG
from scripts.utils import format_dataframe_with_baseline
from scripts.plotter import plot_results, plot_runs_variance
from scripts.benchmark import Benchmark


class CompareBenchmark(Benchmark):
    def __init__(self):
        super().__init__("compare", COMPARE_DIR)

    def add_custom_args(self, parser):
        parser.add_argument("--keep-summaries", action="store_true")

    def get_log_prefix(self):
        return "compare"

    def process(self, dataset_path: str):
        dataset_name = os.path.basename(dataset_path)
        current_result = {"Dataset": dataset_name}
        all_times_dict, all_ratios_dict = {}, {}

        for algo_name, algo_config in self.active_algos.items():
            resolved_params = {}
            params = algo_config.get('params', {})
            for p_key in PARAM_CONFIG.keys():
                resolved_params[p_key] = params.get(p_key, getattr(self.args, p_key))

            t_avg, r_avg, t_list, r_list = self.execute_runner(
                algo_name=algo_name,
                algo_config=algo_config,
                dataset_path=dataset_path,
                dataset_name=dataset_name,
                resolved_params=resolved_params
            )

            if t_avg is not None:
                current_result[f"Time_{algo_name}"] = t_avg
                current_result[f"Ratio_{algo_name}"] = r_avg
                self.logger.info(f"\t=> {algo_name: <12} Time: {t_avg:.3f}s | Ratio: {r_avg:.5f}")
                if self.args.runs > 1:
                    all_times_dict[algo_name], all_ratios_dict[algo_name] = t_list, r_list

        self.results.append(current_result)
        if self.args.runs > 1:
            plot_runs_variance(f"{dataset_name}_{self.timestamp}", all_times_dict, all_ratios_dict, RUNS_DIR)

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
