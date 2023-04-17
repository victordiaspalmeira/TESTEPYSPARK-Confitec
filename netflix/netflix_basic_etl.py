import logging
import os
import time

from dotenv import dotenv_values

from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date, when

logging.basicConfig(level=logging.INFO)
config = dotenv_values()

columns_and_translations = {
    'Title': 'Título',
    'Genre': 'Gênero',
    'GenreLabels': 'GêneroLabels',
    'Premiere': 'Estreia',
    'Seasons': 'Temporadas',
    'SeasonsParsed': 'TemporadasParsed',
    'EpisodesParsed': 'EpisódiosParsed',
    'Length': 'Duração',
    'MinLength': 'DuraçãoMínima',
    'MaxLength': 'DuraçãoMáxima',
    'Status': 'Estado',
    'Active': 'Ativo',
    'Table': 'Tabela',
    'Language': 'Idioma',
    'dt_inclusao': 'dt_inclusao'
}


def run_netflix_etc(spark):
    # Load data and filter duplicates
    df = spark.read.parquet(
        config['RAW_DATA_PATH']
    ).distinct()

    # Check if all columns are valid and expected.
    gdf = SparkDFDataset(df)
    check_result = gdf.expect_table_columns_to_match_set(
        column_set=list(columns_and_translations.keys())
    )['success']

    assert check_result, 'Mismatch in expected columns.'

    # Translate 'TBA' values in 'Seasons' field to 'a ser anunciado'
    df = df.withColumn(
        'Seasons',        when(
            df['Seasons'] == 'TBA',
            'a ser anunciado'
        ).otherwise(df['Seasons'])
    )

    # Transform fields 'Premiere' and 'dt_inclusao' from string to datetime
    df = df.withColumn(
        'Premiere',
        to_date(df['Premiere'], 'dd-MMM-yy')
    ).withColumn(
        'dt_inclusao',
        to_date(df['dt_inclusao'])
    )

    # Add new field 'Data de Alteração' with current timestamp
    df = df.withColumn('Data de Alteração', current_timestamp())

    # Sort (desc) by 'Active' and 'Genre'
    df = df.orderBy(['Active', 'Genre'], ascending=False)

    # Translate all english field names to portuguese
    for eng_col, ptbr_col in columns_and_translations.items():
        df = df.withColumnRenamed(eng_col, ptbr_col)

    # Select specified field and save to .csv
    df_final = df.select(
        'Título',
        'Gênero',
        'Temporadas',
        'Estreia',
        'Idioma',
        'Ativo',
        'Estado',
        'dt_inclusao',
        'Data de Alteração'
    )

    # Save final dataframe to AWS S3.
    df_final.coalesce(1).write.mode(
        'overwrite'
    ).options(
        header='True',
        separator=';'
    ).csv(
        config['AWS_S3_BUCKET']+'/'+config['OUTPUT_DIRECTORY']
    )


if __name__ == "__main__":
    # Set env AWS credentials
    os.environ["AWS_ACCESS_KEY_ID"] = config['AWS_ACCESS_KEY_ID']
    os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS_SECRET_ACCESS_KEY']
    os.environ["AWS_REGION"] = config['AWS_REGION']

    # Initialize SparkSession
    logging.info('Starting Spark Session...')
    spark = SparkSession.builder \
        .master(config['SPARK_MASTER_URL']) \
        .appName(config['SPARK_APP_NAME']) \
        .config("spark.cores.max", config['SPARK_CORE_MAX']) \
        .config("spark.executor.memory", config['SPARK_EXECUTOR_MEMORY']) \
        .config("spark.executor.instances", config['SPARK_NUM_EXECUTORS']) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    logging.info('Spark Session OK!')
    logging.info("Running Netflix ETL...")
    run_netflix_etc(spark)

    # Monitor ETL executor status
    try:
        status_tracker = spark.sparkContext.statusTracker()
        while status_tracker.getActiveJobsIds():
            for job_id in status_tracker.getActiveJobsIds():
                job_info = status_tracker.getJobInfo(job_id)
                if job_info.status == 'FAILED':
                    raise Exception(
                        f"""
                            Job {job_info.job_info.jobId} has failed!
                        """
                    )
            time.sleep(3)
        logging.info("Netflix ETL has been finished successfully!")
    except Exception as e:
        logging.critical(str(e))
        logging.critical('Please check execution logs.')
    finally:
        spark.stop()
