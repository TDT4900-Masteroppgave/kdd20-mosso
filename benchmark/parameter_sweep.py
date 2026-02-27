"""
Parameter Analysis Tool for Hybrid MoSSo.
Iterates through a range of values for a specific parameter to map out the Pareto frontier.
"""

import os
import subprocess
import pandas as pd
import argparse
from plotter import plot_parameter_analysis

SWEEP_CONFIG = {
    "samples": {
        "values": [i for i in range(10, 240, 10)],
        "default": 120,
        "arg_flag": "--samples"
    },
    "escape": {
        "values": [i for i in range(1, 9, 2)],
        "default": 3,
        "arg_flag": "--escape"
    }
}

def setup_directories():
    """Guarantees the output directory exists right before saving."""
    sweep_output_dir = os.path.join("output", "parameter_sweep")
    os.makedirs(sweep_output_dir, exist_ok=True)
    return sweep_output_dir

def run_parameter_sweep(param_to_sweep, mode, runs, file_path=None):
    if param_to_sweep not in SWEEP_CONFIG:
        print(f"[!] Invalid parameter. Available: {list(SWEEP_CONFIG.keys())}")
        return

    sweep_values = SWEEP_CONFIG[param_to_sweep]["values"]

    print(f"[*] Initiating Parameter Sweep for: {param_to_sweep.upper()}")
    print(f"[*] Testing values: {sweep_values}")

    # Pre-build the JARs once to save time
    print("\n[*] Initializing build...")
    subprocess.run(["python3", "benchmark/benchmark.py", "--mode", "remote", "--runs", "1", "--keep-summaries"], check=True)

    all_results = []

    for val in sweep_values:
        print(f"\n{'='*60}")
        print(f"[*] SWEEPING: {param_to_sweep} = {val}")
        print(f"{'='*60}")

        # Construct the base command
        cmd = [
            "python3", "benchmark/benchmark.py",
            "--mode", mode,
            "--runs", str(runs),
            "--skip-build" # Skip building since we did it above
        ]

        # Add the local file if required
        if mode == "local" and file_path:
            cmd.extend(["--file", file_path])

        # Inject the parameter being swept, and use defaults for the rest
        for p_name, p_config in SWEEP_CONFIG.items():
            current_val = val if p_name == param_to_sweep else p_config["default"]
            cmd.extend([p_config["arg_flag"], str(current_val)])

        # Run the benchmark
        subprocess.run(cmd)

        # Harvest the output from benchmark.py (which defaults to output/benchmark/)
        result_file = "output/benchmark/remote_results.csv" if mode == "remote" else f"output/benchmark/local_{os.path.basename(file_path)}_results.csv"

        if os.path.exists(result_file):
            df = pd.read_csv(result_file)
            df[param_to_sweep] = val # Tag the dataframe with the current sweep parameter
            all_results.append(df)
        else:
            print(f"[!] Warning: No results generated for {param_to_sweep} = {val}")

    # Aggregate and Plot in the new sweep_output_dir
    if all_results:
        # Guarantee directory exists RIGHT BEFORE saving
        sweep_output_dir = setup_directories()

        final_df = pd.concat(all_results, ignore_index=True)
        master_csv = os.path.join(sweep_output_dir, f"sweep_{param_to_sweep}_results.csv")

        # Save the CSV
        final_df.to_csv(master_csv, index=False)
        print(f"\n[*] Sweep complete! Master CSV saved to {master_csv}")

        # Generate the Plot
        plot_output = os.path.join(sweep_output_dir, f"sweep_{param_to_sweep}_plot.pdf")
        plot_parameter_analysis(master_csv, param_to_sweep, plot_output)

def main():
    parser = argparse.ArgumentParser(description="Parameter Sweep Tool for MoSSo")
    parser.add_argument("--param", choices=list(SWEEP_CONFIG.keys()), required=True,
                        help="The parameter you want to analyze.")
    parser.add_argument("--mode", choices=["remote", "local"], default="remote",
                        help="Dataset mode (remote runs all 3 datasets).")
    parser.add_argument("--file", type=str,
                        help="Required if mode is local.")
    parser.add_argument("--runs", type=int, default=1,
                        help="Number of iterations per parameter value.")

    args = parser.parse_args()

    if args.mode == "local" and not args.file:
        print("[!] Error: You must provide --file when using --mode local.")
        return

    run_parameter_sweep(args.param, args.mode, args.runs, args.file)

if __name__ == "__main__":
    main()