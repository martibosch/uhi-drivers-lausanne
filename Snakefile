from os import path

PROJECT_NAME = "uhi-drivers-lausanne"
CODE_DIR = "uhi_drivers_lausanne"

NOTEBOOKS_DIR = "notebooks"
NOTEBOOKS_OUTPUT_DIR = path.join(NOTEBOOKS_DIR, "output")

DATA_DIR = "data"
DATA_RAW_DIR = path.join(DATA_DIR, "raw")
DATA_INTERIM_DIR = path.join(DATA_DIR, "interim")
DATA_PROCESSED_DIR = path.join(DATA_DIR, "processed")


# 0. conda/mamba environment -----------------------------------------------------------
rule create_environment:
    shell:
        "mamba env create -f environment.yml"


rule register_ipykernel:
    shell:
        "python -m ipykernel install --user --name {PROJECT_NAME} --display-name"
        " 'Python ({PROJECT_NAME})'"


# 1. download data ---------------------------------------------------------------------
NETATMO_DIR_NAME = "netatmo-lausanne-aug-21"


rule download_netatmo_data:
    output:
        directory(path.join(DATA_RAW_DIR, NETATMO_DIR_NAME)),
    shell:
        # download using awscli
        # ensure that credentials are active
        "aws s3 cp --recursive s3://ceat-data/swiss-urban-es/netatmo-lausanne-aug-21"
        " {output}"


# 2. preprocess netatmo data -----------------------------------------------------------
NETATMO_PREPROCESS_IPYNB_BASENAME = "netatmo-preprocessing.ipynb"


rule preprocess_netatmo_data:
    input:
        data=ancient(rules.download_netatmo_data.output),
        notebook=path.join(NOTEBOOKS_DIR, NETATMO_PREPROCESS_IPYNB_BASENAME),
    output:
        ts_df=path.join(DATA_INTERIM_DIR, NETATMO_DIR_NAME, "ts-df.csv"),
        station_gser=path.join(DATA_INTERIM_DIR, NETATMO_DIR_NAME, "station-gser.gpkg"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, NETATMO_PREPROCESS_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook} -p data_dir {input.data}"
        " -p dst_ts_df_filepath {output.ts_df}"
        " -p dst_station_gser_filepath {output.station_gser}"
