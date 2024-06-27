from os import path

PROJECT_NAME = "uhi-drivers-lausanne"
CODE_DIR = "uhi_drivers_lausanne"

NOTEBOOKS_DIR = "notebooks"
NOTEBOOKS_OUTPUT_DIR = path.join(NOTEBOOKS_DIR, "output")

DATA_DIR = "data"
DATA_RAW_DIR = path.join(DATA_DIR, "raw")
DATA_INTERIM_DIR = path.join(DATA_DIR, "interim")
DATA_PROCESSED_DIR = path.join(DATA_DIR, "processed")

REPORTS_DIR = "reports"
FIGURES_DIR = path.join(REPORTS_DIR, "figures")

YEAR = 2023


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

OFFICIAL_DATA_IPYNB_BASENAME = "official-data.ipynb"


rule official_data:
    input:
        agglom_extent=rules.agglom_extent.output,
        notebook=path.join(NOTEBOOKS_DIR, OFFICIAL_DATA_IPYNB_BASENAME),
    output:
        ts_df=path.join(DATA_INTERIM_DIR, "official-ts-df.csv"),
        stations_gdf=path.join(DATA_INTERIM_DIR, "official-stations.gpkg"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, OFFICIAL_DATA_IPYNB_BASENAME),
    params:
        year=YEAR,
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p agglom_extent_filepath {input.agglom_extent}"
        " -p year {params.year}"
        " -p dst_ts_df_filepath {output.ts_df}"
        " -p dst_stations_gdf_filepath {output.stations_gdf}"


# 1.2. citizen weather stations (Netatmo) ----------------------------------------------
# 1.2.2. download CWS data -------------------------------------------------------------
CWS_DOWNLOAD_DATA_IPYNB_BASENAME = "cws-download-data.ipynb"


rule cws_download_data:
    input:
        agglom_extent=rules.agglom_extent.output,
        official_ts_df=rules.official_data.output.ts_df,
        notebook=path.join(NOTEBOOKS_DIR, CWS_DOWNLOAD_DATA_IPYNB_BASENAME),
    output:
        ts_df=path.join(DATA_RAW_DIR, "cws-ts-df.csv"),
        stations_gdf=path.join(DATA_RAW_DIR, "cws-stations.gpkg"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, CWS_DOWNLOAD_DATA_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p agglom_extent_filepath {input.agglom_extent}"
        " -p official_ts_df_filepath {input.official_ts_df}"
        " -p dst_ts_df_filepath {output.ts_df}"
        " -p dst_stations_gdf_filepath {output.stations_gdf}"


# 1.2.2. quality control CWS data ------------------------------------------------------
CWS_QC_IPYNB_BASENAME = "cws-qc.ipynb"


rule cws_qc:
    input:
        cws_ts_df=rules.cws_download_data.output.ts_df,
        official_ts_df=rules.official_data.output.ts_df,
        cws_stations_gdf=rules.cws_download_data.output.stations_gdf,
        notebook=path.join(NOTEBOOKS_DIR, CWS_QC_IPYNB_BASENAME),
    output:
        ts_df=path.join(DATA_INTERIM_DIR, "cws-qc-ts-df.csv"),
        stations_gdf=path.join(DATA_INTERIM_DIR, "cws-qc-stations.gpkg"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, CWS_QC_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p cws_ts_df_filepath {input.cws_ts_df}"
        " -p official_ts_df_filepath {input.official_ts_df}"
        " -p cws_stations_gdf_filepath {input.cws_stations_gdf}"
        " -p dst_ts_df_filepath {output.ts_df}"
        " -p dst_stations_gdf_filepath {output.stations_gdf}"


# 1.3 merge official and CWS data ------------------------------------------------------
MERGE_OFFICIAL_CWS_DATA_IPYNB_BASENAME = "merge-official-cws-data.ipynb"


rule merge_official_cws_data:
    input:
        official_ts_df=rules.official_data.output.ts_df,
        cws_ts_df=rules.cws_qc.output.ts_df,
        official_stations_gdf=rules.official_data.output.stations_gdf,
        cws_stations_gdf=rules.cws_qc.output.stations_gdf,
        notebook=path.join(NOTEBOOKS_DIR, MERGE_OFFICIAL_CWS_DATA_IPYNB_BASENAME),
    output:
        ts_df=path.join(DATA_PROCESSED_DIR, "ts-df.csv"),
        stations_gdf=path.join(DATA_PROCESSED_DIR, "stations.gpkg"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, MERGE_OFFICIAL_CWS_DATA_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p official_ts_df_filepath {input.official_ts_df}"
        " -p cws_ts_df_filepath {input.cws_ts_df}"
        " -p official_stations_gdf_filepath {input.official_stations_gdf}"
        " -p cws_stations_gdf_filepath {input.cws_stations_gdf}"
        " -p dst_ts_df_filepath {output.ts_df}"
        " -p dst_stations_gdf_filepath {output.stations_gdf}"


# 2. exploratory data analysis ---------------------------------------------------------
UHI_EDA_IPYNB_BASENAME = "uhi-eda.ipynb"


rule uhi_eda:
    input:
        ts_df=rules.merge_official_cws_data.output.ts_df,
        stations_gdf=rules.merge_official_cws_data.output.stations_gdf,
        notebook=path.join(NOTEBOOKS_DIR, UHI_EDA_IPYNB_BASENAME),
    output:
        fig_t_mean_map=path.join(FIGURES_DIR, "t-mean-map.png"),
        fig_uhi_ts=path.join(FIGURES_DIR, "uhi-ts.pdf"),
        fig_uhi_hourly=path.join(FIGURES_DIR, "uhi-hourly.pdf"),
        fig_uhi_mean_hist=path.join(FIGURES_DIR, "uhi-mean-hist.pdf"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, UHI_EDA_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"


# 3. compute features -------------------------------------------------------------------
BUFFER_DISTS_YML = path.join(DATA_RAW_DIR, "buffer-dists.yml")

# 3.1. building features ---------------------------------------------------------------
BUILDING_FEATURES_IPYNB_BASENAME = "building-features.ipynb"


rule building_features:
    input:
        agglom_extent=rules.agglom_extent.output,
        stations_gdf=rules.merge_official_cws_data.output.stations_gdf,
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


# 3.2. tree canopy features ------------------------------------------------------------
TREE_CANOPY_IPYNB_BASENAME = "tree-canopy.ipynb"
TREE_FEATURES_IPYNB_BASENAME = "tree-features.ipynb"


rule tree_canopy:
    input:
        stations_gdf=rules.merge_official_cws_data.output.stations_gdf,
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
        stations_gdf=rules.merge_official_cws_data.output.stations_gdf,
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


# 3.3. elevation features --------------------------------------------------------------
ELEVATION_FEATURES_IPYNB_BASENAME = "elev-features.ipynb"


rule elevation_features:
    input:
        stations_gdf=rules.merge_official_cws_data.output.stations_gdf,
        notebook=path.join(NOTEBOOKS_DIR, ELEVATION_FEATURES_IPYNB_BASENAME),
    output:
        elev_features=path.join(DATA_INTERIM_DIR, "elev-features.csv"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, ELEVATION_FEATURES_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p stations_gdf_filepath {input.stations_gdf}"
        " -p dst_filepath {output.elev_features}"


# 3.4. lake features -------------------------------------------------------------------
LAKE_FEATURES_IPYNB_BASENAME = "lake-features.ipynb"


rule lake_features:
    input:
        stations_gdf=rules.merge_official_cws_data.output.stations_gdf,
        notebook=path.join(NOTEBOOKS_DIR, LAKE_FEATURES_IPYNB_BASENAME),
    output:
        lake_features=path.join(DATA_INTERIM_DIR, "lake-features.csv"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, LAKE_FEATURES_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p stations_gdf_filepath {input.stations_gdf}"
        " -p dst_filepath {output.lake_features}"


# 3.5. merge features -------------------------------------------------------------------
MERGE_FEATURES_IPYNB_BASENAME = "merge-features.ipynb"


rule station_features:
    input:
        building_features=rules.building_features.output.building_features,
        tree_features=rules.tree_features.output.tree_features,
        elev_features=rules.elevation_features.output.elev_features,
        lake_features=rules.lake_features.output.lake_features,
        notebook=path.join(NOTEBOOKS_DIR, MERGE_FEATURES_IPYNB_BASENAME),
    output:
        station_features=path.join(DATA_PROCESSED_DIR, "station-features.csv"),
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, MERGE_FEATURES_IPYNB_BASENAME),
    shell:
        "papermill {input.notebook} {output.notebook}"
        " -p bldg_features_filepath {input.building_features}"
        " -p tree_features_filepath {input.tree_features}"
        " -p elev_features_filepath {input.elev_features}"
        " -p lake_features_filepath {input.lake_features}"
        " -p dst_filepath {output.station_features}"
