import os
import pandas as pd
from tabulate import tabulate
import optuna
import warnings

from scripts.config import PARAM_CONFIG
from scripts.plotter import get_pareto_front_2d
from scripts.benchmark import Benchmark

optuna.logging.set_verbosity(optuna.logging.WARNING)
warnings.filterwarnings("ignore", category=FutureWarning)


class BayesianOptimizationBenchmark(Benchmark):
    def __init__(self):
        # We keep the internal name "bayesian" so you don't have to change your run.sh script!
        super().__init__("bayesian")

    def add_custom_args(self, parser):
        parser.add_argument("--iterations", type=int, default=30, help="Total number of points to sample")
        parser.add_argument("--n-startup", type=int, default=10, help="Initial random explorations before AI kicks in")
        parser.add_argument("--jobs", type=int, default=1, help="Number of parallel threads to run (-1 uses all CPUs)")

    def process(self, dataset_path: str, dataset_name: str):
        for algo_name, algo_config in self.active_algos.items():
            template = algo_config.get('template', [])
            if not template:
                self.logger.info(f"\n[*] Skipping [{algo_name}]: No hyperparameters to optimize.")
                continue

            def objective(trial):
                resolved_params = {}

                # Dynamically build the search space from your config.py
                for p_key in template:
                    bounds = PARAM_CONFIG.get(p_key, {}).get("bounds")
                    if bounds:
                        resolved_params[p_key] = trial.suggest_int(p_key, bounds[0], bounds[1])
                    else:
                        resolved_params[p_key] = PARAM_CONFIG.get(p_key, {}).get("default")

                # Execute the algorithm
                avg_time, avg_ratio, _, _ = self.execute_runner(
                    algo_name, algo_config, dataset_path, dataset_name, resolved_params
                )

                # Pruning: If it crashes or times out, tell Optuna to instantly discard this branch
                if avg_time is None or avg_ratio is None:
                    raise optuna.exceptions.TrialPruned()

                # Optimization Metric: Minimize (Time * Ratio)
                score = avg_time * avg_ratio

                # Save the result so our tables and plotters still work
                result_entry = {
                    'Dataset': dataset_name,
                    'Algorithm': algo_name,
                    'Time': avg_time,
                    'Ratio': avg_ratio,
                    'Optimization_Score': score
                }
                result_entry.update(resolved_params)
                self.results.append(result_entry)

                return score

            # Create an SQLite database in your session folder.
            db_path = os.path.join(self.session_dir, "optuna_study.db")
            study_name = f"{algo_name}_{dataset_name}"

            # Use the Tree-structured Parzen Estimator (TPE) algorithm
            sampler = optuna.samplers.TPESampler(n_startup_trials=self.args.n_startup)

            study = optuna.create_study(
                study_name=study_name,
                storage=f"sqlite:///{db_path}",
                direction="minimize",
                sampler=sampler,
                load_if_exists=True
            )

            self.logger.info(f"\n[*] Starting Optuna Search for [{algo_name}] on [{dataset_name}] ({self.args.iterations} trials)")

            study.optimize(objective, n_trials=self.args.iterations, n_jobs=self.args.jobs, show_progress_bar=True)

            best = study.best_trial
            self.logger.info(f"[*] Best configuration found: {best.params} (Score: {best.value:.4f})")

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
        pareto_df = get_pareto_front_2d(avg_df, 'Time', 'Ratio').sort_values(by=["Ratio", "Time"])
        self.logger.info("\n--- OPTUNA OPTIMIZATION: RECOMMENDED DEFAULTS (Sorted by Ratio) ---")
        self.logger.info(tabulate(pareto_df, headers='keys', tablefmt='grid', showindex=False))

    def finalize(self):
        if not self.results: return
        raw_df = pd.DataFrame(self.results)
        avg_df = self._get_averaged_dataframe()

        # Save Tables
        table_output = "--- OPTUNA SEARCH RESULTS (Priority: Ratio > Time) ---\n"
        sorted_avg_df = avg_df.sort_values(by=["Ratio", "Time"])
        table_output += tabulate(sorted_avg_df, headers='keys', tablefmt='grid')

        with open(os.path.join(self.session_dir, "table_results.txt"), "w") as f:
            f.write(table_output)

        # Save CSVs
        pd.concat([raw_df, avg_df], ignore_index=True).to_csv(
            os.path.join(self.session_dir, "optuna_combined_results.csv"), index=False
        )

if __name__ == "__main__":
    BayesianOptimizationBenchmark().run()