import os
import pandas as pd
from tabulate import tabulate
from scipy.stats import qmc

from config import PARAM_CONFIG, OUTPUT_DIR, PROJECT_NAME
from plotter import plot_pareto_front, get_pareto_front_2d
from utils import prepare_dataset, download_and_prepare_dataset, run_multiple_algorithms
from benchmark import Benchmark

LHS_DIR = os.path.join(OUTPUT_DIR, "lhs_optimization")

class LHSBenchmark(Benchmark):
    def __init__(self):
        super().__init__("lhs", LHS_DIR)
        os.makedirs(LHS_DIR, exist_ok=True)

    def add_custom_args(self, parser):
        # Instead of --param and --range, we just need the number of samples!
        parser.add_argument("--samples", type=int, default=30, help="Number of LHS configurations to test")

    def get_log_prefix(self):
        return "lhs_opt"

    def get_algo_param_display(self, p_key, default_val):
        bounds = PARAM_CONFIG[p_key].get('bounds')
        return f"BOUNDS {bounds}" if bounds else "FIXED"

    def generate_lhs_samples(self, algo_template):
        """Generates scaled LHS configurations for the active template."""
        sampler = qmc.LatinHypercube(d=len(algo_template))
        sample = sampler.random(n=self.args.samples)

        # Scale the [0, 1] samples to your integer bounds from config.py
        configs = []
        for row in sample:
            cfg = {}
            for i, p_key in enumerate(algo_template):
                l_bound, u_bound = PARAM_CONFIG[p_key]["bounds"]
                # Scale and round to integer
                val = int(l_bound + row[i] * (u_bound - l_bound))
                cfg[p_key] = val
            configs.append(cfg)
        return configs

    def process(self):
        results = []
        for i, (url, filename) in enumerate(self.datasets_to_run, 1):
            dataset_name = filename.replace(".txt", "").replace(".csv", "")
            path = prepare_dataset(filename, self.logger) if url == "local" else download_and_prepare_dataset(url, filename, self.logger)
            if not path: continue

            for algo_name, algo_config in self.active_algos.items():
                jar_file = f"{PROJECT_NAME}-{algo_name}.jar"
                if not os.path.exists(jar_file): continue

                template = algo_config.get('template', [])
                lhs_configs = self.generate_lhs_samples(template)

                for run_idx, params in enumerate(lhs_configs, 1):
                    self.logger.info(f"[{dataset_name} | {algo_name}] Testing Config {run_idx}/{self.args.samples}: {params}")

                    t, r, _, _ = run_multiple_algorithms(
                        jar_file, path, f"{algo_name}_{dataset_name}_lhs{run_idx}_{self.timestamp}",
                        self.args.runs, True, self.logger, params, template)

                    if t is not None:
                        res = {"Dataset": dataset_name, "Algorithm": algo_name, "Time": t, "Ratio": r}
                        res.update(params)
                        results.append(res)

        # Store the results to the instance variable
        self.results = results

    def print_table(self):
        df = pd.DataFrame(self.results)

        pareto_df = get_pareto_front_2d(df, 'Time', 'Ratio')

        pareto_df = pareto_df.sort_values(by="Time", ascending=True)

        self.logger.info("\n--- PARETO OPTIMAL CONFIGURATIONS (LHS) ---")
        table_str = tabulate(pareto_df, headers='keys', tablefmt='grid', showindex=False)
        for line in table_str.split('\n'):
            self.logger.info(line)

    def finalize(self):
        master_csv = os.path.join(self.save_dir, f"lhs_results_{self.timestamp}.csv")
        pd.DataFrame(self.results).to_csv(master_csv, index=False)
        plot_pareto_front(master_csv, os.path.join(self.save_dir, f"lhs_plot_{self.timestamp}.pdf"))

if __name__ == "__main__":
    LHSBenchmark().run()