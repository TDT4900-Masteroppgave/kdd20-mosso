import os
import subprocess
import sys
import pandas as pd
import argparse
from plotter import plot_parameter_analysis
from utils import setup_directories

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

def run_parameter_sweep(param_to_sweep, runs, file_path=None):
    if param_to_sweep not in SWEEP_CONFIG:
        print(f"[!] Invalid parameter. Available: {list(SWEEP_CONFIG.keys())}")
        return

    print(f"[*] Initiating Parameter Sweep for: {param_to_sweep.upper()}")
    setup_directories()

    print("\n[*] Initializing build...")
    # Run once to build JARs, without passing a file argument
    subprocess.run([sys.executable, "scripts/benchmark.py", "--runs", "1", "--keep-summaries"], check=True)
    all_results = []

    sweep_values = SWEEP_CONFIG[param_to_sweep]["values"]

    for val in sweep_values:
        print(f"\n{'='*60}")
        print(f"[*] SWEEPING: {param_to_sweep} = {val}")
        print(f"{'='*60}")

        cmd = [
            sys.executable, "scripts/benchmark.py",
            "--runs", str(runs),
            "--skip-build"
        ]

        if file_path:
            cmd.extend(["--file", file_path])

        for p_name, p_config in SWEEP_CONFIG.items():
            current_val = val if p_name == param_to_sweep else p_config["default"]
            cmd.extend([p_config["arg_flag"], str(current_val)])

        subprocess.run(cmd)

        # Determine which results file benchmark.py generated
        result_file = "output/benchmark/remote_results.csv"
        if file_path:
            dataset_name = os.path.basename(file_path).replace(".txt", "").replace(".csv", "")
            result_file = f"output/benchmark/local_{dataset_name}_results.csv"

        if os.path.exists(result_file):
            df = pd.read_csv(result_file)
            df[param_to_sweep] = val
            all_results.append(df)
        else:
            print(f"[!] Warning: No results generated for {param_to_sweep} = {val}")

    if all_results:
        sweep_output_dir = os.path.join("output", "parameter_sweep") # Explicitly define for saving

        final_df = pd.concat(all_results, ignore_index=True)
        master_csv = os.path.join(sweep_output_dir, f"sweep_{param_to_sweep}_results.csv")

        final_df.to_csv(master_csv, index=False)
        print(f"\n[*] Sweep complete! Master CSV saved to {master_csv}")

        plot_output = os.path.join(sweep_output_dir, f"sweep_{param_to_sweep}_plot.pdf")
        plot_parameter_analysis(master_csv, param_to_sweep, plot_output)

def main():
    parser = argparse.ArgumentParser(description="Parameter Sweep Tool for MoSSo")
    parser.add_argument("--param", choices=list(SWEEP_CONFIG.keys()), required=True,
                        help="The parameter you want to analyze.")

    parser.add_argument("--file", type=str,
                        help="Optional: Path to a specific local graph file. If omitted, sweeps the default dataset suite.")

    parser.add_argument("--runs", type=int, default=1,
                        help="Number of iterations per parameter value.")

    args = parser.parse_args()

    run_parameter_sweep(args.param, args.runs, args.file)

if __name__ == "__main__":
    main()