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


# 1. meteo data preprocessing ----------------------------------------------------------
# 1.0. agglomeration extent ------------------------------------------------------------
rule download_agglom_extent:
    output:
        temp(f"{DATA_RAW_DIR}/agglom-extent.zip"),
    shell:
        "wget https://zenodo.org/record/4311544/files/agglom-extent.zip?download=1 -O "
        "{output}"


rule agglom_extent_shp:
    input:
        rules.download_agglom_extent.output,
    output:
        temp(directory(f"{DATA_RAW_DIR}/agglom-extent")),
    shell:
        "unzip {input} -d {output}"


rule agglom_extent:
    input:
        rules.agglom_extent_shp.output,
    output:
        f"{DATA_RAW_DIR}/agglom-extent.gpkg",
    shell:
        "ogr2ogr -f GPKG {output} {input}"


# 1.1. official weather stations -------------------------------------------------------

OFFICIAL_STATIONS_DIR_NAME = "official"
OFFICIAL_STATIONS_PREPROCESS_IPYNB_BASENAME = "official-stations-preprocessing.ipynb"


rule official_stations_data:
    input:
        agglom_extent=rules.agglom_extent.output,
        notebook=path.join(NOTEBOOKS_DIR, OFFICIAL_STATIONS_PREPROCESS_IPYNB_BASENAME),
    output:
        ts_df=path.join(DATA_INTERIM_DIR, OFFICIAL_STATIONS_DIR_NAME, "ts-df.csv"),
        station_gser=path.join(
            DATA_INTERIM_DIR, OFFICIAL_STATIONS_DIR_NAME, "station-gser.gpkg"
        ),
        notebook=path.join(
            NOTEBOOKS_OUTPUT_DIR, OFFICIAL_STATIONS_PREPROCESS_IPYNB_BASENAME
        ),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p agglom_extent_filepath {input.agglom_extent}"
        " -p dst_ts_df_filepath {output.ts_df}"
        " -p dst_station_gser_filepath {output.station_gser}"


# 1.2. citizen weather stations (Netatmo) ----------------------------------------------
# 1.2.1. download raw netatmo data -----------------------------------------------------
NETATMO_DIR_NAME = "netatmo"


rule download_netatmo_data:
    output:
        directory(path.join(DATA_RAW_DIR, NETATMO_DIR_NAME)),
    shell:
        # download using awscli
        # ensure that credentials are active
        "aws s3 cp --recursive s3://ceat-data/swiss-urban-es/netatmo-lausanne-aug-21"
        " {output}"


# 1.2.2. preprocess netatmo data -------------------------------------------------------
NETATMO_PREPROCESS_IPYNB_BASENAME = "netatmo-preprocessing.ipynb"


rule netatmo_data:
    input:
        netatmo_data=ancient(rules.download_netatmo_data.output),
        agglom_extent=rules.agglom_extent.output,
        notebook=path.join(NOTEBOOKS_DIR, NETATMO_PREPROCESS_IPYNB_BASENAME),
    output:
        ts_df=path.join(DATA_INTERIM_DIR, NETATMO_DIR_NAME, "ts-df.csv"),
        station_gser=path.join(DATA_INTERIM_DIR, NETATMO_DIR_NAME, "station-gser.gpkg"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, NETATMO_PREPROCESS_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p data_dir {input.netatmo_data}"
        " -p agglom_extent_filepath {input.agglom_extent}"
        " -p dst_ts_df_filepath {output.ts_df}"
        " -p dst_station_gser_filepath {output.station_gser}"


# 1.2.3. quality control netatmo data --------------------------------------------------

NETATMO_QC_IPYNB_BASENAME = "netatmo-quality-control.ipynb"


rule netatmo_qc:
    input:
        netatmo_ts_df=rules.netatmo_data.output.ts_df,
        official_ts_df=rules.official_stations_data.output.ts_df,
        notebook=path.join(NOTEBOOKS_DIR, NETATMO_QC_IPYNB_BASENAME),
    output:
        ts_df=path.join(DATA_INTERIM_DIR, NETATMO_DIR_NAME, "ts-df-qc.csv"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, NETATMO_QC_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p netatmo_ts_df_filepath {input.netatmo_ts_df}"
        " -p official_ts_df_filepath {input.official_ts_df}"
        " -p dst_filepath {output.ts_df}"


# 2. computing features ----------------------------------------------------------------
BUFFER_DISTS_YML = path.join(DATA_RAW_DIR, "buffer-dists.yml")

# 2.1. building features ---------------------------------------------------------------
BUILDING_FEATURES_IPYNB_BASENAME = "building-features.ipynb"


rule building_features:
    input:
        agglom_extent=rules.agglom_extent.output,
        stations_gdf=rules.stations_gdf.output.stations_gdf,
        notebook=path.join(NOTEBOOKS_DIR, BUILDING_FEATURES_IPYNB_BASENAME),
    output:
        building_features=path.join(DATA_INTERIM_DIR, "bldg-features.csv"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, BUILDING_FEATURES_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p agglom_extent_filepath {input.agglom_extent}"
        " -p stations_gdf_filepath {input.stations_gdf}"
        " -f {BUFFER_DISTS_YML}"
        " -p dst_filepath {output.building_features}"


# 2.2. tree canopy features ------------------------------------------------------------
TREE_CANOPY_IPYNB_BASENAME = "tree-canopy.ipynb"
TREE_FEATURES_IPYNB_BASENAME = "tree-features.ipynb"


rule tree_canopy:
    input:
        stations_gdf=rules.stations_gdf.output.stations_gdf,
        agglom_extent=rules.agglom_extent.output,
        notebook=path.join(NOTEBOOKS_DIR, TREE_CANOPY_IPYNB_BASENAME),
    output:
        tree_canopy=path.join(DATA_INTERIM_DIR, "tree-canopy.nc"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, TREE_CANOPY_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p stations_gdf_filepath {input.stations_gdf}"
        " -p agglom_extent_filepath {input.agglom_extent}"
        " -f {BUFFER_DISTS_YML}"
        " -p dst_filepath {output.tree_canopy}"


rule tree_features:
    input:
        stations_gdf=rules.stations_gdf.output.stations_gdf,
        tree_canopy=rules.tree_canopy.output.tree_canopy,
        notebook=path.join(NOTEBOOKS_DIR, TREE_FEATURES_IPYNB_BASENAME),
    output:
        tree_features=path.join(DATA_INTERIM_DIR, "tree-features.csv"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, TREE_FEATURES_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p stations_gdf_filepath {input.stations_gdf}"
        " -p tree_canopy_filepath {input.tree_canopy}"
        " -f {BUFFER_DISTS_YML}"
        " -p dst_filepath {output.tree_features}"


# 2.3. elevation features --------------------------------------------------------------
ELEVATION_FEATURES_IPYNB_BASENAME = "elev-features.ipynb"


rule elevation_features:
    input:
        stations_gdf=rules.stations_gdf.output.stations_gdf,
        notebook=path.join(NOTEBOOKS_DIR, ELEVATION_FEATURES_IPYNB_BASENAME),
    output:
        elev_features=path.join(DATA_INTERIM_DIR, "elev-features.csv"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, ELEVATION_FEATURES_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p stations_gdf_filepath {input.stations_gdf}"
        " -p dst_filepath {output.elev_features}"


# 2.4. lake features -------------------------------------------------------------------
LAKE_FEATURES_IPYNB_BASENAME = "lake-features.ipynb"


rule lake_features:
    input:
        stations_gdf=rules.stations_gdf.output.stations_gdf,
        notebook=path.join(NOTEBOOKS_DIR, LAKE_FEATURES_IPYNB_BASENAME),
    output:
        lake_features=path.join(DATA_INTERIM_DIR, "lake-features.csv"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, LAKE_FEATURES_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p stations_gdf_filepath {input.stations_gdf}"
        " -p dst_filepath {output.lake_features}"
