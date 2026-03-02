import argparse
import pandas as pd
from config import *
from utils import setup_logging, setup_directories, build_jars, download_and_prepare_dataset, prepare_dataset
from run_mosso import run_multiple_mosso
from plotter import plot_parameter_analysis

def print_sweep_summary_table(results, param_name, logger):
    if not results: return
    df = pd.DataFrame(results)

    df['Time_Diff_%'] = ((df['Time_Original'] - df['Time_Hybrid']) / df['Time_Original']) * 100
    df['Ratio_Diff_%'] = ((df['Ratio_Hybrid'] - df['Ratio_Original']) / df['Ratio_Original']) * 100

    header = f"| {'Dataset':<18} | {param_name.upper():<9} | {'Orig Time(s)':<12} | {'Hyb Time(s)':<12} | {'Time Diff':<10} | {'Orig Ratio':<10} | {'Hyb Ratio':<10} | {'Ratio Diff':<10} |"
    sep = "-" * len(header)

    logger.info(f"\n{sep}")
    logger.info(f"| {f'FINAL SWEEP SUMMARY: {param_name.upper()}':^{len(header)-4}} |")
    logger.info(f"{sep}")
    logger.info(header)
    logger.info(sep)

    df_sorted = df.sort_values(by=['Dataset', param_name])

    current_dataset = None
    for _, row in df_sorted.iterrows():
        dataset = row['Dataset'][:18]
        if current_dataset and dataset != current_dataset:
            logger.info(sep)
        current_dataset = dataset

        p_val = str(row[param_name])
        t_o, t_h = f"{row['Time_Original']:.3f}", f"{row['Time_Hybrid']:.3f}"
        t_d = f"{row['Time_Diff_%']:+.2f}%"
        r_o, r_h = f"{row['Ratio_Original']:.5f}", f"{row['Ratio_Hybrid']:.5f}"
        r_d = f"{row['Ratio_Diff_%']:+.4f}%"

        logger.info(f"| {dataset:<18} | {p_val:<9} | {t_o:<12} | {t_h:<12} | {t_d:<10} | {r_o:<10} | {r_h:<10} | {r_d:<10} |")

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

    if args.range:
        start, stop, step = args.range
        sweep_values = list(range(start, stop + 1, step))
    elif args.values:
        sweep_values = args.values
    else:
        sweep_values = config["values"]

    # ==========================================
    # STAGE 2: PROCESSING
    # ==========================================
    logger.info("\n" + "="*60)
    logger.info(f"{'STAGE 2: PARAMETER SWEEP PROCESSING':^60}")
    logger.info("="*60)
    logger.info(f"[*] Starting Sweep for: {param.upper()} over {len(sweep_values)} values.")

    for val in sweep_values:
        logger.info(f"\n--- Testing {param.upper()} = {val} ---")

        samples = val if param == "samples" else args.samples
        escape = val if param == "escape" else args.escape
        b_cand = val if param == "b" else args.b

        for i, (url, filename) in enumerate(datasets_to_run, 1):
            dataset_name = filename.replace(".txt", "").replace(".csv", "")

            if url == "local":
                path = prepare_dataset(filename, logger)
            else:
                path = download_and_prepare_dataset(url, filename, logger)

            if not path:
                logger.warning(f"\n[{i}/{total_datasets}] [!] Skipping {dataset_name} because preparation failed.")
                continue

            logger.info(f"\n[{i}/{total_datasets}] Running {dataset_name} ({args.runs} runs) ...")

            logger.debug("   Running Original...")
            t1, r1, _, _ = run_multiple_mosso(JAR_ORIGINAL, path, f"orig_{dataset_name}_{param}{val}_{timestamp}", 120, 3, args.interval, args.runs, True, logger)

            logger.debug("   Running Hybrid...")
            t2, r2, _, _ = run_multiple_mosso(JAR_HYBRID, path, f"hyb_{dataset_name}_{param}{val}_{timestamp}", samples, escape, args.interval, args.runs, True, logger, b_cand)

            if None in (t1, t2):
                logger.warning(f"   [!] Skipped {dataset_name} due to execution failure.")
                continue

            t_diff = ((t1 - t2) / t1) * 100
            r_diff = ((r2 - r1) / r1) * 100
            logger.info(f"   => Original: {t1:.3f}s / {r1:.5f} | Hybrid: {t2:.3f}s / {r2:.5f}")
            logger.info(f"   => Diff: Time {t_diff:+.2f}% | Ratio {r_diff:+.4f}%")

            all_results.append({
                "Dataset": dataset_name, param: val,
                "Time_Original": t1, "Time_Hybrid": t2,
                "Ratio_Original": r1, "Ratio_Hybrid": r2
            })

    # ==========================================
    # STAGE 3: RESULTS
    # ==========================================
    logger.info("\n" + "="*60)
    logger.info(f"{'STAGE 3: RESULTS & ARTIFACTS':^60}")
    logger.info("="*60)

    if all_results:
        print_sweep_summary_table(all_results, param, logger)

        final_df = pd.DataFrame(all_results)
        master_csv = os.path.join(SWEEP_DIR, f"sweep_{param}_results_{timestamp}.csv")
        final_df.to_csv(master_csv, index=False)

        plot_output = os.path.join(SWEEP_DIR, f"sweep_{param}_plot_{timestamp}.pdf")
        plot_parameter_analysis(master_csv, param, plot_output, logger)

        logger.info(f"[*] Sweep complete! Artifacts saved to: {SWEEP_DIR}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--param", choices=list(SWEEP_CONFIG.keys()), required=True)
    parser.add_argument("--file", type=str)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--skip-build", action="store_true")

    parser.add_argument("--range", type=int, nargs=3, metavar=('START', 'STOP', 'STEP'),
                        help="Generate a range of values to test (e.g., --range 10 100 10)")
    parser.add_argument("--values", type=int, nargs='+', help="Specific values to test (e.g., --values 10 50 100)")

    parser.add_argument("--samples", type=int, default=SWEEP_CONFIG["samples"]["default"])
    parser.add_argument("--escape", type=int, default=SWEEP_CONFIG["escape"]["default"])
    parser.add_argument("--b", type=int, default=SWEEP_CONFIG["b"]["default"])
    parser.add_argument("--interval", type=int, default=1000)
    parser.add_argument("--group", choices=["all"] + list(DATASETS.keys()), default="all",
                        help="Which dataset group to run from config.py")

    args = parser.parse_args()

    logger, log_file, timestamp = setup_logging(f"sweep_{args.param}")

    # ==========================================
    # STAGE 1: SETUP
    # ==========================================
    logger.info("\n" + "="*60)
    logger.info(f"{'STAGE 1: SETUP & COMPILATION':^60}")
    logger.info("="*60)
    logger.info(f"[*] Log initialized: {log_file}")

    setup_directories()
    build_jars(args.skip_build, logger)

    run_sweep(args, logger, timestamp)

if __name__ == "__main__":
    main()