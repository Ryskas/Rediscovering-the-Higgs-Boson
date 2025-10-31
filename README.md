# Rediscovering the Higgs Boson

This repository contains notebook for analyzing ATLAS Open Data to rediscover the Higgs boson in the diphoton channel ($H \rightarrow \gamma\gamma$) and a script for downloading files with data.

## Contents

- `HyyAnalysis_with_extensions.ipynb`: Main analysis notebook with baseline and extended selections, plotting, and fit results.
- `download_atlas_files.py`: Script to download ATLAS files to a local cache directory using atlasopenmagic. The `atlas_cache/` is the output directory for downloaded ATLAS data files.
- `requirements_download.txt`: Python requirements for the download script.

## How to Run

1. Download ATLAS data files:
	```
	pip install -r requirements_download.txt
	python download_atlas_files.py
	```
    Downloads about 10 GB of files, so it takes few minutes.
2. Open and run the analysis notebook:
	- Open `HyyAnalysis_with_extensions.ipynb`
	- Run all cells to reproduce the analysis

## Results

- Cutflow tables showing event counts after each selection step
- Di-photon invariant mass spectrum with Higgs peak
- Signal and background fit results (Gaussian + polynomial)
- Plots and tables demonstrating the effect of baseline and extended cuts
