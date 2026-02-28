import argparse
import pandas as pd
from config import *
from logger import setup_logging
from utils import setup_directories, build_jars, download_and_prepare_dataset, prepare_dataset
from run_mosso import run_multiple_mosso
from plotter import plot_parameter_analysis

SWEEP_CONFIG = {
    "samples": {"values": [i for i in range(10, 240, 10)], "default": 120},
    "escape": {"values": [i for i in range(1, 9, 2)], "default": 3},
    "b": {"values": [1, 3, 5, 7, 10], "default": 5}
}

def print_sweep_summary_table(results, param_name, logger):
    if not results: return
    df = pd.DataFrame(results)

    # Calculate percentage differences
    df['Time_Diff_%'] = ((df['Time_Original'] - df['Time_Hybrid']) / df['Time_Original']) * 100
    df['Ratio_Diff_%'] = ((df['Ratio_Hybrid'] - df['Ratio_Original']) / df['Ratio_Original']) * 100

    header = f"| {'Dataset':<18} | {param_name.upper():<9} | {'Orig Time(s)':<12} | {'Hyb Time(s)':<12} | {'Time Diff':<10} | {'Orig Ratio':<10} | {'Hyb Ratio':<10} | {'Ratio Diff':<10} |"
    sep = "-" * len(header)

    logger.info(f"\n{sep}")
    logger.info(f"| {f'FINAL SWEEP SUMMARY: {param_name.upper()}':^{len(header)-4}} |")
    logger.info(f"{sep}")
    logger.info(header)
    logger.info(sep)

    # Sort by dataset, then parameter value for easy reading
    df_sorted = df.sort_values(by=['Dataset', param_name])

    current_dataset = None
    for _, row in df_sorted.iterrows():
        dataset = row['Dataset'][:18]
        if current_dataset and dataset != current_dataset:
            logger.info(sep) # Add a visual divider between different datasets
        current_dataset = dataset

        p_val = str(row[param_name])
        t_o, t_h = f"{row['Time_Original']:.3f}", f"{row['Time_Hybrid']:.3f}"
        t_d = f"{row['Time_Diff_%']:+.2f}%"
        r_o, r_h = f"{row['Ratio_Original']:.5f}", f"{row['Ratio_Hybrid']:.5f}"
        r_d = f"{row['Ratio_Diff_%']:+.4f}%"

        logger.info(f"| {dataset:<18} | {p_val:<9} | {t_o:<12} | {t_h:<12} | {t_d:<10} | {r_o:<10} | {r_h:<10} | {r_d:<10} |")

    # --- AVERAGES SECTION ---
    logger.info(sep)
    logger.info(f"| {'AVERAGES BY PARAMETER VALUE':^{len(header)-4}} |")
    logger.info(sep)

    avg_df = df.groupby(param_name).mean(numeric_only=True).reset_index()
    for _, row in avg_df.iterrows():
        p_val_num = row[param_name]
        p_val = str(int(p_val_num)) if p_val_num.is_integer() else f"{p_val_num:.2f}"

        t_o, t_h = f"{row['Time_Original']:.3f}", f"{row['Time_Hybrid']:.3f}"
        t_d = f"{row['Time_Diff_%']:+.2f}%"
        r_o, r_h = f"{row['Ratio_Original']:.5f}", f"{row['Ratio_Hybrid']:.5f}"
        r_d = f"{row['Ratio_Diff_%']:+.4f}%"

        logger.info(f"| {'ALL (Avg)':<18} | {p_val:<9} | {t_o:<12} | {t_h:<12} | {t_d:<10} | {r_o:<10} | {r_h:<10} | {r_d:<10} |")

    logger.info(f"{sep}\n")

def run_sweep(args, logger):
    param = args.param
    config = SWEEP_CONFIG[param]
    all_results = []

    datasets_to_run = [("local", args.file)] if args.file else [
        (url, filename) for data_list in DATASETS.values() for url, filename in data_list
    ]

    total_datasets = len(datasets_to_run)

    logger.info(f"[*] Starting Sweep for: {param.upper()} over {len(config['values'])} values.")

    for val in config["values"]:
        logger.info(f"\n{'='*60}")
        logger.info(f"--- Testing {param.upper()} = {val} ---")
        logger.info(f"{'='*60}")

        # Determine current parameters
        samples = val if param == "samples" else SWEEP_CONFIG["samples"]["default"]
        escape = val if param == "escape" else SWEEP_CONFIG["escape"]["default"]
        b_cand = val if param == "b" else SWEEP_CONFIG["b"]["default"]

        for i, (url, filename) in enumerate(datasets_to_run, 1):
            dataset_name = filename.replace(".txt", "").replace(".csv", "")

            if url == "local":
                path = prepare_dataset(filename, logger)
            else:
                path = download_and_prepare_dataset(url, filename, logger)

            # Safety check
            if not path:
                logger.warning(f"\n[{i}/{total_datasets}] [!] Skipping {dataset_name} because preparation failed.")
                continue

            logger.info(f"\n[{i}/{total_datasets}] Running {dataset_name} ({args.runs} runs) ...")

            logger.debug("   Running Original...")
            t1, r1, _, _ = run_multiple_mosso(JAR_ORIGINAL, path, f"orig_{dataset_name}_{param}{val}", 120, 3, 1000, args.runs, True, logger)

            logger.debug("   Running Hybrid...")
            t2, r2, _, _ = run_multiple_mosso(JAR_HYBRID, path, f"hyb_{dataset_name}_{param}{val}", samples, escape, 1000, args.runs, True, logger, b_cand)

            if None in (t1, t2):
                logger.warning(f"   [!] Skipped {dataset_name} due to execution failure.")
                continue

            # Real-time feedback
            t_diff = ((t1 - t2) / t1) * 100
            r_diff = ((r2 - r1) / r1) * 100
            logger.info(f"   => Original: {t1:.3f}s / {r1:.5f} | Hybrid: {t2:.3f}s / {r2:.5f}")
            logger.info(f"   => Diff: Time {t_diff:+.2f}% | Ratio {r_diff:+.4f}%")

            all_results.append({
                "Dataset": dataset_name, param: val,
                "Time_Original": t1, "Time_Hybrid": t2,
                "Ratio_Original": r1, "Ratio_Hybrid": r2
            })

    if all_results:
        # Print the beautiful summary table right before finishing!
        print_sweep_summary_table(all_results, param, logger)

        final_df = pd.DataFrame(all_results)
        master_csv = os.path.join(SWEEP_DIR, f"sweep_{param}_results.csv")
        final_df.to_csv(master_csv, index=False)

        plot_output = os.path.join(SWEEP_DIR, f"sweep_{param}_plot.pdf")
        plot_parameter_analysis(master_csv, param, plot_output, logger)
        logger.info(f"[*] Sweep complete! Saved to {master_csv} & {plot_output}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--param", choices=list(SWEEP_CONFIG.keys()), required=True)
    parser.add_argument("--file", type=str)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--skip-build", action="store_true")

    args = parser.parse_args()
    logger, log_file = setup_logging(f"sweep_{args.param}")

    setup_directories()
    build_jars(args.skip_build, logger)
    run_sweep(args, logger)

if __name__ == "__main__":
    main()