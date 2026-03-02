import os
import pandas as pd
import matplotlib.pyplot as plt

def plot_results(csv_file, plot_file, logger):
    df = pd.read_csv(csv_file).dropna(subset=["Time_Original", "Time_Hybrid"])
    if df.empty: return

    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    df.plot(x="Dataset", y=["Ratio_Original", "Ratio_Hybrid"], kind="bar", ax=axes[0], color=["#e74c3c", "#2ecc71"])
    axes[0].set_title("Average Compression Ratio (Lower is Better)")
    axes[0].tick_params(axis='x', rotation=45 if len(df) > 1 else 0)

    df.plot(x="Dataset", y=["Time_Original", "Time_Hybrid"], kind="bar", ax=axes[1], color=["#e74c3c", "#2ecc71"])
    axes[1].set_title("Average Execution Time (Seconds)")
    axes[1].tick_params(axis='x', rotation=45 if len(df) > 1 else 0)

    plt.tight_layout()
    plt.savefig(plot_file)
    logger.debug(f"Saved bar plot to {plot_file}")
    plt.close()

def plot_runs_variance(dataset_name, orig_times, hyb_times, orig_ratios, hyb_ratios, runs_dir, logger):
    if not orig_times or not hyb_times: return
    runs_x = list(range(1, len(orig_times) + 1))
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))

    axes[0].plot(runs_x, orig_ratios, marker='o', color='#e74c3c', label='Original')
    axes[0].plot(runs_x, hyb_ratios, marker='s', color='#2ecc71', label='Hybrid')
    axes[0].set_title(f"Compression Ratio Variance ({len(orig_times)} Runs)")
    axes[0].legend()

    axes[1].plot(runs_x, orig_times, marker='o', color='#e74c3c', label='Original')
    axes[1].plot(runs_x, hyb_times, marker='s', color='#2ecc71', label='Hybrid')
    axes[1].set_title(f"Execution Time Variance ({len(orig_times)} Runs)")
    axes[1].legend()

    plt.tight_layout()
    plt.savefig(os.path.join(runs_dir, f"{dataset_name}_variance_plot.pdf"))
    plt.close()

def plot_parameter_analysis(csv_file, param_name, plot_file, logger):
    df = pd.read_csv(csv_file)
    if df.empty: return

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    avg_df = df.groupby(param_name).mean(numeric_only=True).reset_index()

    # --- Plot 1: Average Compression Ratio ---
    axes[0].plot(avg_df[param_name], avg_df['Ratio_Hybrid'], marker='o', color='#3498db', linewidth=2.5, markersize=8, label='Hybrid MoSSo (Average)')
    axes[0].axhline(y=avg_df['Ratio_Original'].mean(), linestyle='--', color='#e74c3c', linewidth=2, label='Original MoSSo (Average)')

    axes[0].set_title(f"Average Compression Ratio vs {param_name.upper()}", fontsize=14, fontweight='bold')
    axes[0].set_xlabel(f"Parameter: {param_name.upper()}", fontsize=12)
    axes[0].set_ylabel("Compression Ratio (Lower is Better)", fontsize=12)
    axes[0].set_xticks(avg_df[param_name])
    axes[0].tick_params(axis='x', rotation=45)
    axes[0].grid(True, linestyle=':', alpha=0.7)
    axes[0].legend(fontsize=11)

    # --- Plot 2: Average Execution Time ---
    axes[1].plot(avg_df[param_name], avg_df['Time_Hybrid'], marker='s', color='#2ecc71', linewidth=2.5, markersize=8, label='Hybrid MoSSo (Average)')
    axes[1].axhline(y=avg_df['Time_Original'].mean(), linestyle='--', color='#e74c3c', linewidth=2, label='Original MoSSo (Average)')

    axes[1].set_title(f"Average Execution Time vs {param_name.upper()}", fontsize=14, fontweight='bold')
    axes[1].set_xlabel(f"Parameter: {param_name.upper()}", fontsize=12)
    axes[1].set_ylabel("Execution Time in Seconds (Lower is Better)", fontsize=12)
    axes[1].set_xticks(avg_df[param_name])
    axes[1].tick_params(axis='x', rotation=45)
    axes[1].grid(True, linestyle=':', alpha=0.7)
    axes[1].legend(fontsize=11)

    plt.tight_layout()
    plt.savefig(plot_file)
    plt.close()