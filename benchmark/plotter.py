import os
import pandas as pd
import matplotlib.pyplot as plt

def plot_results(csv_file, plot_file):
    df = pd.read_csv(csv_file)
    if df.empty: return

    df = df.dropna(subset=["Time_Original", "Time_Hybrid"])

    if df.empty:
        print("[!] No successful runs to plot.")
        return

    fig, axes = plt.subplots(1, 2, figsize=(15, 6))

    # --- Plot 1: Average Compression Ratio ---
    df.plot(x="Dataset", y=["Ratio_Original", "Ratio_Hybrid"], kind="bar", ax=axes[0], color=["#e74c3c", "#2ecc71"])
    axes[0].set_title("Average Compression Ratio (Lower is Better)")
    axes[0].set_ylabel("Average Ratio")
    axes[0].tick_params(axis='x', rotation=45 if len(df) > 1 else 0)

    # --- Plot 2: Average Execution Time ---
    df.plot(x="Dataset", y=["Time_Original", "Time_Hybrid"], kind="bar", ax=axes[1], color=["#e74c3c", "#2ecc71"])
    axes[1].set_title("Average Execution Time (Seconds)")
    axes[1].set_ylabel("Average Seconds")
    axes[1].tick_params(axis='x', rotation=45 if len(df) > 1 else 0)

    plt.tight_layout()
    plt.savefig(plot_file)
    print(f"[*] Plot saved to {plot_file}")
    plt.close() # Close to free up memory

def plot_runs_variance(dataset_name, orig_times, hyb_times, orig_ratios, hyb_ratios, runs_dir):
    if not orig_times or not hyb_times:
        return

    runs_x = list(range(1, len(orig_times) + 1))
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))

    # Ratio Plot
    axes[0].plot(runs_x, orig_ratios, marker='o', linestyle='-', color='#e74c3c', label='Original')
    axes[0].plot(runs_x, hyb_ratios, marker='s', linestyle='-', color='#2ecc71', label='Hybrid')
    axes[0].set_title(f"Compression Ratio Variance ({len(orig_times)} Runs)")
    axes[0].set_xlabel("Run Number")
    axes[0].set_ylabel("Compression Ratio (Lower is Better)")
    axes[0].set_xticks(runs_x)
    axes[0].legend()
    axes[0].grid(True, linestyle='--', alpha=0.7)

    # Time Plot
    axes[1].plot(runs_x, orig_times, marker='o', linestyle='-', color='#e74c3c', label='Original')
    axes[1].plot(runs_x, hyb_times, marker='s', linestyle='-', color='#2ecc71', label='Hybrid')
    axes[1].set_title(f"Execution Time Variance ({len(orig_times)} Runs)")
    axes[1].set_xlabel("Run Number")
    axes[1].set_ylabel("Time (Seconds)")
    axes[1].set_xticks(runs_x)
    axes[1].legend()
    axes[1].grid(True, linestyle='--', alpha=0.7)

    plt.tight_layout()
    plot_file = os.path.join(runs_dir, f"{dataset_name}_variance_plot.pdf")
    plt.savefig(plot_file)
    print(f"[*] Variance line-plot saved to {plot_file}")
    plt.close()

def plot_parameter_analysis(csv_file, param_name, plot_file):
    """
    Generates a parameter sensitivity plot mimicking Figure 6 of the MoSSo paper.
    """
    df = pd.read_csv(csv_file)
    if df.empty:
        print("[!] No data to plot.")
        return

    # Create 1 row, 2 columns for Ratio and Time (like the paper)
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    datasets = df['Dataset'].unique()
    markers = ['o', 's', '^', 'D', 'v']
    colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6']

    # Subplot 1: Compression Ratio
    for i, dataset in enumerate(datasets):
        subset = df[df['Dataset'] == dataset].sort_values(by=param_name)
        axes[0].plot(subset[param_name], subset['Ratio_Hybrid'],
                     marker=markers[i % len(markers)], color=colors[i % len(colors)],
                     linestyle='-', linewidth=2, markersize=8, label=f'{dataset} (Hybrid)')

        # Plot Original Baseline as a dashed line for reference
        orig_avg = subset['Ratio_Original'].mean()
        axes[0].axhline(y=orig_avg, linestyle='--', color=colors[i % len(colors)], alpha=0.5)

    axes[0].set_title(f"Effects of {param_name.capitalize()}", fontsize=14)
    axes[0].set_xlabel(param_name.capitalize(), fontsize=12)
    axes[0].set_ylabel("Compression Ratio", fontsize=12)
    axes[0].grid(True, linestyle=':', alpha=0.6)
    axes[0].legend(fontsize=10)

    # Subplot 2: Execution Time
    for i, dataset in enumerate(datasets):
        subset = df[df['Dataset'] == dataset].sort_values(by=param_name)
        axes[1].plot(subset[param_name], subset['Time_Hybrid'],
                     marker=markers[i % len(markers)], color=colors[i % len(colors)],
                     linestyle='-', linewidth=2, markersize=8, label=f'{dataset} (Hybrid)')

        orig_time = subset['Time_Original'].mean()
        axes[1].axhline(y=orig_time, linestyle='--', color=colors[i % len(colors)], alpha=0.5)

    axes[1].set_title(f"Scalability of {param_name.capitalize()}", fontsize=14)
    axes[1].set_xlabel(param_name.capitalize(), fontsize=12)
    axes[1].set_ylabel("Execution Time (sec)", fontsize=12)
    axes[1].grid(True, linestyle=':', alpha=0.6)
    axes[1].legend(fontsize=10)

    plt.tight_layout()
    plt.savefig(plot_file)
    print(f"[*] Parameter analysis plot saved to {plot_file}")
    plt.close()