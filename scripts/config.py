import os

OUTPUT_DIR = "output"
DATASETS_DIR = "datasets"
BENCHMARK_DIR = os.path.join(OUTPUT_DIR, "benchmarks")
VERSIONS_DIR = os.path.join(BENCHMARK_DIR, "versions")

BASE_REPO_URL = "https://github.com/TDT4900-Masteroppgave/mosso-mags-dm.git"

PARAM_CONFIG = {
    "c": {"description": "sample number", "default": 120, "bounds": (10, 240)},
    "e": {"description": "escape", "default": 3, "bounds": (1, 9)},
    "interval": {"description": "interval", "default": 1000},
    "b": {"description": "top candidates", "default": 5, "bounds": (1, 10)},
    "h": {"description": "hashes", "default": 4, "bounds": (4, 40)}
}

ALGORITHMS = {
    "local": {
        "target_dir": ".",
        "type": "mosso",
        "template": ["e", "c", "interval"]
    },
    "kdd20-mosso": {
        "repo": "https://github.com/jihoonko/kdd20-mosso.git",
        "branch": "master",
        "params" : {"c": 120, "e": 3},
        "type": "mosso",
        "template": ["e", "c", "interval"],
        "binary_file": "kdd20-mosso.jar"
    },
    "strat_1": {
        "repo": BASE_REPO_URL,
        "branch": "feature/merging_strategy_1",
        "type": "mosso",
        "template": ["e", "c", "interval"],
        "binary_file": "mosso-strat_1.jar",
    },
    "strat_2": {
        "repo": BASE_REPO_URL,
        "branch": "feature/merging_strategy_2",
        "type": "mosso",
        "template": ["e", "c", "interval", "h"],
        "binary_file": "mosso-strat_2.jar",
    },
    "strat_1_2": {
        "repo": BASE_REPO_URL,
        "branch": "feature/merging_strategy_1_2",
        "type": "mosso",
        "template": ["e", "c", "interval", "b"],
        "binary_file": "mosso-strat_2.jar",
    },
    "mags": {
        "repo": "https://github.com/nedchu/mags-release",
        "branch": "main",
        "type": "mags",
        "template": [],
        "binary_file": "mags",
    },
    "mags-dm": {
        "repo": "https://github.com/nedchu/mags-release",
        "branch": "main",
        "type": "mags",
        "template": [],
        "binary_file": "mags_dm",
    },
}

DATASETS = {
    "one": [
        ("https://snap.stanford.edu/data/as-caida20071105.txt.gz", "as-caida20071105.txt")
    ],
    "small": [
        ("https://snap.stanford.edu/data/as-caida20071105.txt.gz", "as-caida20071105.txt"),
        ("https://snap.stanford.edu/data/email-Enron.txt.gz", "Email-Enron.txt"),
        ("https://snap.stanford.edu/data/loc-brightkite_edges.txt.gz", "Brightkite_edges.txt"),
        ("https://snap.stanford.edu/data/email-EuAll.txt.gz", "Email-EuAll.txt"),
        ("https://snap.stanford.edu/data/soc-Slashdot0902.txt.gz", "Slashdot0902.txt"),
        ("https://snap.stanford.edu/data/bigdata/communities/com-dblp.ungraph.txt.gz", "com-dblp.ungraph.txt")
    ],
    "large": [
        ("https://snap.stanford.edu/data/amazon0601.txt.gz", "amazon0601.txt"),
        ("https://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz", "com-youtube.ungraph.txt"),
        ("https://snap.stanford.edu/data/as-skitter.txt.gz", "as-skitter.txt"),
        ("https://snap.stanford.edu/data/bigdata/communities/com-lj.ungraph.txt.gz", "com-lj.ungraph.txt")
    ]
}