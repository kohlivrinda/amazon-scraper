#!/bin/bash

conda init

eval "$(conda shell.bash hook)"

conda activate scraping_env
cd projects/green-beauty/app/src
python3 scraper.py
conda deactivate