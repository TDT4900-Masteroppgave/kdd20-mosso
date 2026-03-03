import argparse
import pandas as pd
from config import *
from utils import setup_logging, setup_directories, build_jars, download_and_prepare_dataset, prepare_dataset
from run_mosso import run_multiple_mosso
from plotter import plot_results, plot_runs_variance

def print_summary_table(results, logger):
    if not results: return
    df = pd.DataFrame(results)

    avg_row = df.mean(numeric_only=True).to_dict()
    avg_row['Dataset'] = 'AVERAGE'
    df = pd.concat([df, pd.DataFrame([avg_row])], ignore_index=True)

    strategies = [col.replace("Time_", "") for col in df.columns if col.startswith("Time_")]

    header_cols = [f"{'Dataset':<18}"]
    for strat in strategies:
        header_cols.append(f"{strat[:10]+' Time':<12}")
        header_cols.append(f"{strat[:10]+' Ratio':<12}")

    header = "| " + " | ".join(header_cols) + " |"
    sep = "-" * len(header)

    logger.info(f"{sep}")
    logger.info(f"| {'FINAL BENCHMARK SUMMARY':^{len(header)-4}} |")
    logger.info(f"{sep}")
    logger.info(header)
    logger.info(sep)

    for _, row in df.iterrows():
        dataset = str(row['Dataset'])[:18]
        row_str = f"| {dataset:<18} |"

        for strat in strategies:
            t_val = row.get(f"Time_{strat}", float('nan'))
            r_val = row.get(f"Ratio_{strat}", float('nan'))

            t_str = f"{t_val:.3f}s" if pd.notna(t_val) else "N/A"
            r_str = f"{r_val:.5f}" if pd.notna(r_val) else "N/A"

            row_str += f" {t_str:<12} | {r_str:<12} |"

        if dataset == 'AVERAGE': logger.info(sep)
        logger.info(row_str)

    logger.info(f"{sep}")

def run_suite(args, file_path, logger, timestamp):
    results = []
    datasets_to_run = [("local", file_path)] if file_path else []

    if not file_path:
        if args.group == "all":
            for cat, data_list in DATASETS.items():
                for url, filename in data_list:
                    datasets_to_run.append((url, filename))
        else:
            for url, filename in DATASETS[args.group]:
                datasets_to_run.append((url, filename))

    total_datasets = len(datasets_to_run)

    logger.info("="*60)
    logger.info(f"{'STAGE 2: BENCHMARK PROCESSING':^60}")
    logger.info("="*60)

    for i, (url, filename) in enumerate(datasets_to_run, 1):
        dataset_name = filename.replace(".txt", "").replace(".csv", "")
        path = prepare_dataset(filename, logger) if url == "local" else download_and_prepare_dataset(url, filename, logger)

        if not path:
            continue

        logger.info(f"[{i}/{total_datasets}] Benchmarking [{dataset_name}] ({args.runs} runs) ...")

        current_result = {"Dataset": dataset_name}
        all_times_dict = {}
        all_ratios_dict = {}

        # The Dynamic Execution Loop
        for algo_name, algo_config in ALGORITHMS.items():
                jar_file = f"mosso-{algo_name}.jar"
                if not os.path.exists(jar_file):
                    logger.warning(f"\t[!] Skipping {algo_name} because {jar_file} is missing.")
                    continue

                template = algo_config.get('template')

                params = algo_config.get('params', {})
                resolved_params = {
                    "samples": params.get('samples', args.samples),
                    "escape": params.get('escape', args.escape),
                    "b": params.get('b', args.b),
                    "interval": params.get('interval', args.interval)
                }
                logger.debug(f"Running {algo_name} with mapped params: {resolved_params}...")

                t_avg, r_avg, t_list, r_list = run_multiple_mosso(
                    jar_file, path, f"{algo_name}_{dataset_name}_{timestamp}",
                    args.runs, not args.keep_summaries, logger, resolved_params, template
                )

                if t_avg is not None:
                    current_result[f"Time_{algo_name}"] = t_avg
                    current_result[f"Ratio_{algo_name}"] = r_avg
                    logger.info(f"\t=> {algo_name: <12} Time: {t_avg:.3f}s | Ratio: {r_avg:.5f}")

                    if args.runs > 1:
                        all_times_dict[algo_name] = t_list
                        all_ratios_dict[algo_name] = r_list

        results.append(current_result)

        if args.runs > 1:
            plot_runs_variance(f"{dataset_name}_{timestamp}", all_times_dict, all_ratios_dict, RUNS_DIR, logger)

    logger.info("="*60)
    logger.info(f"{'STAGE 3: RESULTS & ARTIFACTS':^60}")
    logger.info("="*60)

    print_summary_table(results, logger)

    if results:
        csv_file = os.path.join(BENCHMARK_DIR, f"results_{timestamp}.csv")
        pd.DataFrame(results).to_csv(csv_file, index=False)
        plot_file = os.path.join(BENCHMARK_DIR, f"comparison_{timestamp}.pdf")
        plot_results(csv_file, plot_file, logger)
        logger.info(f"[*] Artifacts successfully saved to: {BENCHMARK_DIR}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, help="Specific local graph file.")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--samples", type=int, default=120)
    parser.add_argument("--escape", type=int, default=3)
    parser.add_argument("--b", type=int, default=5)
    parser.add_argument("--interval", type=int, default=1000)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--keep-summaries", action="store_true")
    parser.add_argument("--group", choices=["all"] + list(DATASETS.keys()), default="all")

    args = parser.parse_args()
    logger, log_file, timestamp = setup_logging("benchmark")

    logger.info("="*60)
    logger.info(f"{'STAGE 1: SETUP & COMPILATION':^60}")
    logger.info("="*60)

    setup_directories()
    build_jars(args.skip_build, logger)

    run_suite(args, args.file, logger, timestamp)

if __name__ == "__main__":
    main()