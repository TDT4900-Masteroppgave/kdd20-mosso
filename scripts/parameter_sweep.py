import os
import pandas as pd
from config import ALGORITHMS, SWEEP_DIR, SWEEP_CONFIG
from utils import setup_logging, setup_directories, build_jars, download_and_prepare_dataset, prepare_dataset, \
    parse_and_filter_args, get_datasets_to_run, print_sweep_table
from run_mosso import run_multiple_mosso
from plotter import plot_parameter_analysis


def run_sweep(args, datasets_to_run, sweep_values, param, logger, timestamp):
    results = []

    for val in sweep_values:
        logger.info(f"--- Testing {param.upper()} = {val} ---")
        samples = val if param == "samples" else args.samples
        escape = val if param == "escape" else args.escape
        b = val if param == "b" else args.b

        for i, (url, filename) in enumerate(datasets_to_run, 1):
            dataset_name = filename.replace(".txt", "").replace(".csv", "")
            path = prepare_dataset(filename, logger) if url == "local" else download_and_prepare_dataset(url, filename,
                                                                                                         logger)
            if not path: continue

            logger.info(f"[{i}/{len(datasets_to_run)}] Running {dataset_name} ({args.runs} runs) ...")
            current_result = {"Dataset": dataset_name, param: val}

            for algo_name, algo_config in ALGORITHMS.items():
                jar_file = f"mosso-{algo_name}.jar"
                if not os.path.exists(jar_file): continue

                template = algo_config.get('template', [])
                params = algo_config.get('params', {})
                resolved_params = {
                    "samples": params.get('samples', samples),
                    "escape": params.get('escape', escape),
                    "b": params.get('b', b),
                    "interval": params.get('interval', args.interval)
                }

                t, r, _, _ = run_multiple_mosso(
                    jar_file, path, f"{algo_name}_{dataset_name}_{param}{val}_{timestamp}",
                    args.runs, True, logger, resolved_params, template)

                if t is not None:
                    current_result[f"Time_{algo_name}"], current_result[f"Ratio_{algo_name}"] = t, r
                    logger.info(f"\t=> {algo_name: <12} Time: {t:.3f}s | Ratio: {r:.5f}")

            results.append(current_result)

        if results:
            master_csv = os.path.join(SWEEP_DIR, f"sweep_{param}_results_{timestamp}.csv")
            pd.DataFrame(results).to_csv(master_csv, index=False)
            plot_parameter_analysis(master_csv, param, os.path.join(SWEEP_DIR, f"sweep_{param}_plot_{timestamp}.pdf"))

    return results


def main():
    args = parse_and_filter_args(script_type="sweep")
    logger, timestamp = setup_logging(f"sweep_{args.param}")

    logger.info("=" * 10 + f"{' STAGE 1: SETUP & COMPILATION ':^10}" + "=" * 10)
    setup_directories()
    build_jars(args.skip_build, args.local, logger)

    logger.info("=" * 10 + f"{' STAGE 2: SWEEP PROCESSING ':^10}" + "=" * 10)
    datasets_to_run = get_datasets_to_run(args)
    param, config = args.param, SWEEP_CONFIG[args.param]
    sweep_values = list(range(*args.range)) if args.range else (args.values if args.values else config["values"])

    results = run_sweep(args, datasets_to_run, sweep_values, param, logger, timestamp)
    if results:
        print_sweep_table(results, logger, title=f"SWEEP SUMMARY: {param.upper()}", sweep_param=param, baseline_algo=args.baseline)
        logger.info(f"[*] Artifacts saved to: {SWEEP_DIR}")

if __name__ == "__main__":
    main()