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

    df['Time_Diff_%'] = ((df['Time_Original'] - df['Time_Hybrid']) / df['Time_Original']) * 100
    df['Ratio_Diff_%'] = ((df['Ratio_Hybrid'] - df['Ratio_Original']) / df['Ratio_Original']) * 100

    header = f"| {'Dataset':<18} | {'Orig Time(s)':<12} | {'Hyb Time(s)':<12} | {'Time Diff':<10} | {'Orig Ratio':<10} | {'Hyb Ratio':<10} | {'Ratio Diff':<10} |"
    sep = "-" * len(header)

    logger.info(f"{sep}")
    logger.info(f"| {'FINAL BENCHMARK SUMMARY':^96} |")
    logger.info(f"{sep}")
    logger.info(header)
    logger.info(sep)

    for _, row in df.iterrows():
        dataset = row['Dataset'][:18]
        t_o, t_h = f"{row['Time_Original']:.3f}", f"{row['Time_Hybrid']:.3f}"
        t_d = f"{row['Time_Diff_%']:+.2f}%"
        r_o, r_h = f"{row['Ratio_Original']:.5f}", f"{row['Ratio_Hybrid']:.5f}"
        r_d = f"{row['Ratio_Diff_%']:+.4f}%"

        if dataset == 'AVERAGE': logger.info(sep)
        logger.info(f"| {dataset:<18} | {t_o:<12} | {t_h:<12} | {t_d:<10} | {r_o:<10} | {r_h:<10} | {r_d:<10} |")

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

    # ==========================================
    # STAGE 2: PROCESSING
    # ==========================================
    logger.info("="*60)
    logger.info(f"{'STAGE 2: BENCHMARK PROCESSING':^60}")
    logger.info("="*60)

    for i, (url, filename) in enumerate(datasets_to_run, 1):
        dataset_name = filename.replace(".txt", "").replace(".csv", "")

        if url == "local":
            path = prepare_dataset(filename, logger)
        else:
            path = download_and_prepare_dataset(url, filename, logger)

        if not path:
            logger.warning(f"[{i}/{total_datasets}] [!] Skipping {dataset_name} because preparation failed.")
            continue

        logger.info(f"[{i}/{total_datasets}] Benchmarking [{dataset_name}] ({args.runs} runs) ...")

        logger.debug("   Running Original...")
        t1_avg, r1_avg, t1_list, r1_list = run_multiple_mosso(
            JAR_ORIGINAL, path, f"orig_{dataset_name}_{timestamp}", 120, 3, args.interval, args.runs, not args.keep_summaries, logger)

        logger.debug("   Running Hybrid...")
        t2_avg, r2_avg, t2_list, r2_list = run_multiple_mosso(
            JAR_HYBRID, path, f"hyb_{dataset_name}_{timestamp}", args.samples, args.escape, args.interval, args.runs, not args.keep_summaries, logger, args.b)

        if None in (t1_avg, t2_avg):
            logger.warning(f"   [!] Skipped {dataset_name} due to execution failure.")
            continue

        t_diff = ((t1_avg - t2_avg) / t1_avg) * 100
        r_diff = ((r2_avg - r1_avg) / r1_avg) * 100
        logger.info(f"   => Original: {t1_avg:.3f}s / {r1_avg:.5f} | Hybrid: {t2_avg:.3f}s / {r2_avg:.5f}")
        logger.info(f"   => Diff: Time {t_diff:+.2f}% | Ratio {r_diff:+.4f}%")

        results.append({
            "Dataset": dataset_name,
            "Time_Original": t1_avg, "Time_Hybrid": t2_avg,
            "Ratio_Original": r1_avg, "Ratio_Hybrid": r2_avg
        })

        if args.runs > 1:
            plot_runs_variance(f"{dataset_name}_{timestamp}", t1_list, t2_list, r1_list, r2_list, RUNS_DIR, logger)

    # ==========================================
    # STAGE 3: RESULTS
    # ==========================================
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
    parser.add_argument("--group", choices=["all"] + list(DATASETS.keys()), default="all",
                        help="Which dataset group to run from config.py")

    args = parser.parse_args()

    logger, log_file, timestamp = setup_logging("benchmark")

    # ==========================================
    # STAGE 1: SETUP
    # ==========================================
    logger.info("="*60)
    logger.info(f"{'STAGE 1: SETUP & COMPILATION':^60}")
    logger.info("="*60)
    logger.info(f"[*] Log initialized: {log_file}")

    setup_directories()
    build_jars(args.skip_build, logger)

    run_suite(args, args.file, logger, timestamp)

if __name__ == "__main__":
    main()