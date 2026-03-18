import os
import pandas as pd
from tabulate import tabulate
from scipy.stats import qmc

from scripts.config import PARAM_CONFIG, OUTPUT_DIR
from scripts.plotter import plot_pareto_front, get_pareto_front_2d
from scripts.benchmark import Benchmark

LHS_DIR = os.path.join(OUTPUT_DIR, "lhs_optimization")

class LHSBenchmark(Benchmark):
    def __init__(self):
        super().__init__("lhs")

    def add_custom_args(self, parser):
        parser.add_argument("--samples", type=int, default=30, help="Number of LHS configurations to test")

    def get_algo_param_display(self, p_key, default_val):
        bounds = PARAM_CONFIG.get(p_key, {}).get('bounds')
        return f"BOUNDS {bounds}" if bounds else "FIXED"

    def generate_lhs_samples(self, algo_template):
        """Generates scaled LHS configurations for the active template."""
        if not algo_template:
            return []

        sampler = qmc.LatinHypercube(d=len(algo_template))
        sample = sampler.random(n=self.args.samples)

        configs = []
        for row in sample:
            cfg = {}
            for i, p_key in enumerate(algo_template):
                bounds = PARAM_CONFIG.get(p_key, {}).get("bounds")
                if bounds:
                    l_bound, u_bound = bounds
                    # Scale and round to integer
                    val = int(l_bound + row[i] * (u_bound - l_bound))
                    cfg[p_key] = val
                else:
                    cfg[p_key] = getattr(self.args, p_key)
            configs.append(cfg)
        return configs

    def process(self, dataset_path: str):
        dataset_name = os.path.basename(dataset_path)

        for algo_name, algo_config in self.active_algos.items():
            template = algo_config.get('template', [])
            base_params = algo_config.get('params', {})

            lhs_configs = self.generate_lhs_samples(template) if template else [{}]

            total_runs = len(lhs_configs)

            for run_idx, lhs_params in enumerate(lhs_configs, 1):
                resolved_params = {}
                for p_key in PARAM_CONFIG.keys():
                    resolved_params[p_key] = getattr(self.args, p_key)
                resolved_params.update(base_params)
                resolved_params.update(lhs_params)

                if template:
                    self.logger.info(f"\t[{algo_name}] Testing Config {run_idx}/{total_runs}: {lhs_params}")
                else:
                    self.logger.info(f"\t[{algo_name}] Testing Default Config (No template defined).")

                unique_run_name = f"{dataset_name}_lhs{run_idx}"

                t, r, _, _ = self.execute_runner(
                    algo_name=algo_name,
                    algo_config=algo_config,
                    dataset_path=dataset_path,
                    dataset_name=unique_run_name,
                    resolved_params=resolved_params
                )

                if t is not None:
                    res = {
                        "Dataset": dataset_name,
                        "Algorithm": algo_name,
                        "Time": t,
                        "Ratio": r
                    }
                    for p_key in template:
                        res[p_key] = resolved_params[p_key]

                    self.results.append(res)

    def print_table(self):
        df = pd.DataFrame(self.results)
        pareto_df = get_pareto_front_2d(df, 'Time', 'Ratio')
        pareto_df = pareto_df.sort_values(by="Time", ascending=True)

        self.logger.info("\n--- PARETO OPTIMAL CONFIGURATIONS (LHS) ---")
        table_str = tabulate(pareto_df, headers='keys', tablefmt='grid', showindex=False)
        for line in table_str.split('\n'):
            self.logger.info(line)

    def finalize(self):
        csv_file = os.path.join(self.session_dir, f"lhs_results_{self.timestamp}.csv")
        pd.DataFrame(self.results).to_csv(csv_file, index=False)
        plot_pareto_front(csv_file, os.path.join(self.session_dir, f"lhs_plot_{self.timestamp}.pdf"))

if __name__ == "__main__":
    LHSBenchmark().run()