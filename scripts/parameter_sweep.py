import argparse
import pandas as pd
from config import *
from utils import setup_logging, setup_directories, build_jars, download_and_prepare_dataset, prepare_dataset
from run_mosso import run_multiple_mosso
from plotter import plot_parameter_analysis

def print_sweep_summary_table(results, param_name, logger):
    if not results: return
    df = pd.DataFrame(results)

    # 1. Dynamically find all strategies
    strategies = [col.replace("Time_", "") for col in df.columns if col.startswith("Time_")]

    # 2. Build dynamic header
    header_cols = [f"{'Dataset':<18}", f"{param_name.upper():<9}"]
    for strat in strategies:
        header_cols.append(f"{strat[:10]+' Time':<12}")
        header_cols.append(f"{strat[:10]+' Ratio':<12}")

    header = "| " + " | ".join(header_cols) + " |"
    sep = "-" * len(header)

    # 3. Print Header
    logger.info(f"{sep}")
    logger.info(f"| {f'FINAL SWEEP SUMMARY: {param_name.upper()}':^{len(header)-4}} |")
    logger.info(f"{sep}")
    logger.info(header)
    logger.info(sep)

    df_sorted = df.sort_values(by=['Dataset', param_name])
    current_dataset = None

    # 4. Print Detail Rows
    for _, row in df_sorted.iterrows():
        dataset = str(row['Dataset'])[:18]
        if current_dataset and dataset != current_dataset:
            logger.info(sep)
        current_dataset = dataset

        p_val = str(row[param_name])
        row_str = f"| {dataset:<18} | {p_val:<9} |"

        for strat in strategies:
            t_val = row.get(f"Time_{strat}", float('nan'))
            r_val = row.get(f"Ratio_{strat}", float('nan'))
            t_str = f"{t_val:.3f}s" if pd.notna(t_val) else "N/A"
            r_str = f"{r_val:.5f}" if pd.notna(r_val) else "N/A"
            row_str += f" {t_str:<12} | {r_str:<12} |"

        logger.info(row_str)

    # 5. Print Averages
    logger.info(sep)
    logger.info(f"| {'AVERAGES BY PARAMETER VALUE':^{len(header)-4}} |")
    logger.info(sep)

    avg_df = df.groupby(param_name).mean(numeric_only=True).reset_index()
    for _, row in avg_df.iterrows():
        p_val_num = row[param_name]
        p_val = str(int(p_val_num)) if p_val_num.is_integer() else f"{p_val_num:.2f}"
        row_str = f"| {'ALL (Avg)':<18} | {p_val:<9} |"

        for strat in strategies:
            t_val = row.get(f"Time_{strat}", float('nan'))
            r_val = row.get(f"Ratio_{strat}", float('nan'))
            t_str = f"{t_val:.3f}s" if pd.notna(t_val) else "N/A"
            r_str = f"{r_val:.5f}" if pd.notna(r_val) else "N/A"
            row_str += f" {t_str:<12} | {r_str:<12} |"

        logger.info(row_str)

    logger.info(f"{sep}")

def run_sweep(args, logger, timestamp):
    param = args.param
    config = SWEEP_CONFIG[param]
    all_results = []

    if args.file:
        datasets_to_run = [("local", args.file)]
    else:
        if args.group == "all":
            datasets_to_run = [(url, filename) for data_list in DATASETS.values() for url, filename in data_list]
        else:
            datasets_to_run = [(url, filename) for url, filename in DATASETS[args.group]]

    total_datasets = len(datasets_to_run)
    sweep_values = list(range(*args.range)) if args.range else (args.values if args.values else config["values"])

    logger.info("="*60)
    logger.info(f"{'STAGE 2: ABLATION SWEEP PROCESSING':^60}")
    logger.info("="*60)

    for val in sweep_values:
        logger.info(f"--- Testing {param.upper()} = {val} ---")
        samples = val if param == "samples" else args.samples
        escape = val if param == "escape" else args.escape
        b_cand = val if param == "b" else args.b

        for i, (url, filename) in enumerate(datasets_to_run, 1):
            dataset_name = filename.replace(".txt", "").replace(".csv", "")
            path = prepare_dataset(filename, logger) if url == "local" else download_and_prepare_dataset(url, filename, logger)
            if not path: continue

            logger.info(f"[{i}/{total_datasets}] Running {dataset_name} ({args.runs} runs) ...")

            current_result = {"Dataset": dataset_name, param: val}

            # The Dynamic Sweep Loop
            for algo_name, algo_config in ALGORITHMS.items():
                jar_file = f"mosso-{algo_name}.jar"
                if not os.path.exists(jar_file): continue
                logger.debug(f"Running {algo_name}...")

                if algo_config.get('is_baseline', False):
                    t, r, _, _ = run_multiple_mosso(jar_file, path, f"{algo_name}_{dataset_name}_{param}{val}_{timestamp}", 120, 3, args.interval, args.runs, True, logger)
                else:
                    t, r, _, _ = run_multiple_mosso(jar_file, path, f"{algo_name}_{dataset_name}_{param}{val}_{timestamp}", samples, escape, args.interval, args.runs, True, logger, b_cand)

                if t is not None:
                    current_result[f"Time_{algo_name}"] = t
                    current_result[f"Ratio_{algo_name}"] = r
                    logger.info(f"\t=> {algo_name: <12} Time: {t:.3f}s | Ratio: {r:.5f}")

            all_results.append(current_result)

        # Incremental saving handles arbitrary columns flawlessly
        if all_results:
            current_df = pd.DataFrame(all_results)
            master_csv = os.path.join(SWEEP_DIR, f"sweep_{param}_results_{timestamp}.csv")
            current_df.to_csv(master_csv, index=False)
            plot_output = os.path.join(SWEEP_DIR, f"sweep_{param}_plot_{timestamp}.pdf")
            plot_parameter_analysis(master_csv, param, plot_output, logger)

    logger.info("="*60)
    logger.info(f"{'STAGE 3: RESULTS & ARTIFACTS':^60}")
    logger.info("="*60)

    if all_results:
        print_sweep_summary_table(all_results, param, logger)
        logger.info(f"[*] Sweep complete! Artifacts saved incrementally to: {SWEEP_DIR}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--param", choices=list(SWEEP_CONFIG.keys()), required=True)
    parser.add_argument("--file", type=str)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--range", type=int, nargs=3)
    parser.add_argument("--values", type=int, nargs='+')
    parser.add_argument("--samples", type=int, default=SWEEP_CONFIG["samples"]["default"])
    parser.add_argument("--escape", type=int, default=SWEEP_CONFIG["escape"]["default"])
    parser.add_argument("--b", type=int, default=SWEEP_CONFIG["b"]["default"])
    parser.add_argument("--interval", type=int, default=1000)
    parser.add_argument("--group", choices=["all"] + list(DATASETS.keys()), default="all")

    args = parser.parse_args()
    logger, log_file, timestamp = setup_logging(f"sweep_{args.param}")

    logger.info("="*60)
    logger.info(f"{'STAGE 1: SETUP & COMPILATION':^60}")
    logger.info("="*60)

    setup_directories()
    build_jars(args.skip_build, logger)
    run_sweep(args, logger, timestamp)

if __name__ == "__main__":
    main()