import os
import pandas as pd
from tabulate import tabulate
from skopt import Optimizer
from skopt.space import Integer

from scripts.config import PARAM_CONFIG
from scripts.plotter import plot_pareto_front, get_pareto_front_2d
from scripts.benchmark import Benchmark

class BayesianOptimizationBenchmark(Benchmark):
    def __init__(self):
        super().__init__("bayesian")
        self.algo_optimizers = {}
        self.parameter_map = {}

    def add_custom_args(self, parser):
        parser.add_argument("--iterations", type=int, default=30, help="Total number of points to sample")
        parser.add_argument("--random-starts", type=int, default=10, help="Initial random explorations before optimizing")

    def _setup_optimizer(self, algo_name, template):
        """Initializes the Scikit-Optimize search space for a specific algorithm."""
        dimensions = []
        self.parameter_map[algo_name] = []

        for p_key in template:
            bounds = PARAM_CONFIG.get(p_key, {}).get("bounds")
            if bounds:
                dimensions.append(Integer(bounds[0], bounds[1], name=p_key))
                self.parameter_map[algo_name].append(p_key)

        return Optimizer(
            dimensions=dimensions,
            n_initial_points=self.args.random_starts,
            initial_point_generator="lhs",
            acq_func="EI", # Expected Improvement
            random_state=42
        )

    def process(self, dataset_path: str):
        dataset_name = os.path.basename(dataset_path)

        for algo_name, algo_config in self.active_algos.items():
            template = algo_config.get('template', [])
            base_params = algo_config.get('params', {})

            if not template:
                self.logger.warning(f"[{algo_name}] No template parameters defined. Skipping Bayesian search.")
                continue

            # Initialize optimizer for this algo if not already done
            if algo_name not in self.algo_optimizers:
                self.algo_optimizers[algo_name] = self._setup_optimizer(algo_name, template)

            opt = self.algo_optimizers[algo_name]
            param_keys = self.parameter_map[algo_name]

            for i in range(1, self.args.iterations + 1):
                suggested_values = opt.ask()
                current_lhs_params = dict(zip(param_keys, suggested_values))

                resolved_params = {p: getattr(self.args, p) for p in PARAM_CONFIG.keys()}
                resolved_params.update(base_params)
                resolved_params.update(current_lhs_params)

                self.logger.info(f"\t[{algo_name}] Iteration {i}/{self.args.iterations}: {current_lhs_params}")

                unique_run_name = f"{dataset_name}_bayesian{i}"
                t, r, _, _ = self.execute_runner(
                    algo_name=algo_name,
                    algo_config=algo_config,
                    dataset_path=dataset_path,
                    dataset_name=unique_run_name,
                    resolved_params=resolved_params
                )

                if t is not None:
                    # Note: Bayesian Opt usually minimizes ONE value.
                    # This minimizes a weighted score of Time and Ratio.
                    # Adjust the weight based on what you value more (Speed vs Compression).
                    score = (t * 0.5) + (r * 100 * 0.5)
                    opt.tell(suggested_values, score)

                    res = {
                        "Dataset": dataset_name,
                        "Algorithm": algo_name,
                        "Time": t,
                        "Ratio": r,
                        "Optimization_Score": score
                    }
                    res.update(current_lhs_params)
                    self.results.append(res)

    def _get_averaged_dataframe(self):
        if not self.results: return pd.DataFrame()
        df = pd.DataFrame(self.results)
        group_cols = [col for col in df.columns if col not in ['Dataset', 'Time', 'Ratio', 'Optimization_Score']]
        avg_df = df.groupby(group_cols).mean(numeric_only=True).reset_index()
        avg_df['Dataset'] = 'AVERAGE_ACROSS_DATASETS'
        return avg_df

    def print_table(self):
        avg_df = self._get_averaged_dataframe()
        if avg_df.empty: return
        pareto_df = get_pareto_front_2d(avg_df, 'Time', 'Ratio').sort_values(by="Time")
        self.logger.info("\n--- BAYESIAN OPTIMIZATION: RECOMMENDED DEFAULTS ---")
        self.logger.info(tabulate(pareto_df, headers='keys', tablefmt='grid', showindex=False))

    def finalize(self):
        if not self.results: return
        raw_df = pd.DataFrame(self.results)
        avg_df = self._get_averaged_dataframe()

        # Save Tables
        table_output = "--- BAYESIAN SEARCH RESULTS ---\n"
        table_output += tabulate(avg_df.sort_values("Optimization_Score"), headers='keys', tablefmt='grid')
        with open(os.path.join(self.session_dir, "table_results.txt"), "w") as f:
            f.write(table_output)

        # Save CSVs and Plots
        pd.concat([raw_df, avg_df], ignore_index=True).to_csv(
            os.path.join(self.session_dir, "bayesian_combined.csv"), index=False
        )
        plot_pareto_front(
            os.path.join(self.session_dir, "bayesian_combined.csv"),
            os.path.join(self.session_dir, "bayesian_optimization_plot.pdf")
        )

if __name__ == "__main__":
    BayesianOptimizationBenchmark().run()