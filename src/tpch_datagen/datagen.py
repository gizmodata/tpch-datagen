import click
import duckdb
import math
import multiprocessing
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from codetiming import Timer

from . import __version__ as tpch_datagen_version
from .config import DATA_DIR, WORK_DIR, DEFAULT_NUM_CHUNKS, DEFAULT_NUM_PROCESSES
from .logger import logger
from .utils import get_printable_number

# Constants
TIMER_TEXT = "{name}: Elapsed time: {:.4f} seconds"
TPCH_SMALL_TABLE_LIST = [
    "region",
    "nation"
]
TPCH_LARGE_TABLE_LIST = [
    "customer",
    "lineitem",
    "orders",
    "part",
    "partsupp",
    "supplier"
]
TPCH_TABLE_LIST = TPCH_SMALL_TABLE_LIST + TPCH_LARGE_TABLE_LIST


def execute_query(conn: duckdb.DuckDBPyConnection,
                  query: str
                  ):
    logger.info(msg=f"Executing SQL: '{query}'")
    conn.execute(query=query)


def error_callback(exception):
    """Error callback to handle job failure"""
    logger.error(msg=f"Error encountered: {exception}")


def generate_chunk(scale_factor: float,
                   data_directory: str,
                   work_directory: str,
                   chunk_number: int,
                   num_chunks: int,
                   duckdb_threads: int,
                   per_thread_output: bool,
                   compression_method: str,
                   file_size_bytes: str,
                   table_list: list
                   ):
    logger.info(msg=f"generate_chunk called with args: {locals()}")

    with Timer(name=f"Generate chunk: {chunk_number} of: {num_chunks}",
               text=TIMER_TEXT,
               initial_text=True,
               logger=logger.info
               ):
        try:
            with TemporaryDirectory(dir=work_directory) as local_database_dir:
                # Get a DuckDB database connection
                conn = duckdb.connect(database=f"{local_database_dir}/tpch.db")

                # Set the number of threads
                conn.execute(f"SET threads={duckdb_threads};")

                # Load the TPCH extension needed to generate the data...
                conn.load_extension(extension="tpch")

                # Generate the data
                execute_query(conn=conn,
                              query=f"CALL dbgen(sf={scale_factor}, children={num_chunks}, step={chunk_number})"
                              )

                # Export the data to parquet
                for table_name in table_list:
                    execute_query(conn=conn,
                                  query=f"""
                                     COPY (FROM {table_name})
                                     TO '{data_directory}/{table_name}/'
                                     (OVERWRITE_OR_IGNORE true,
                                      FILENAME_PATTERN '{table_name}_{chunk_number}_',
                                      FORMAT parquet,
                                      COMPRESSION '{compression_method}',
                                      PER_THREAD_OUTPUT {str(per_thread_output).lower()},
                                      FILE_SIZE_BYTES '{file_size_bytes}'
                                     )"""
                                  )
        except Exception as e:
            logger.error(msg=f"Error generating chunk: {str(e)}")
            raise e


def datagen(version: bool,
            scale_factor: int,
            data_directory: str,
            work_directory: str,
            overwrite: bool,
            num_chunks: int,
            num_processes: int,
            duckdb_threads: int,
            per_thread_output: bool,
            compression_method: str,
            file_size_bytes: str):
    if version:
        logger.info(msg=f"TPC-H DataGen - Version: {tpch_datagen_version}")
        return

    logger.info(msg=f"click_datagen called with args: {locals()}")

    with Timer(name=f"Run TPC-H Datagen to generate TPC-H data at scale factor: {scale_factor}",
               text=TIMER_TEXT,
               initial_text=True,
               logger=logger.info
               ):
        if not scale_factor:
            raise RuntimeError("You must specify a scale factor to generate data.")

        # Export the data
        target_directory = Path(f"{data_directory}/tpch/sf={get_printable_number(num=scale_factor)}")

        if target_directory.exists():
            if overwrite:
                logger.warning(msg=f"Directory: {target_directory.as_posix()} exists, removing...")
                shutil.rmtree(path=target_directory.as_posix())
            else:
                raise RuntimeError(f"Directory: {target_directory.as_posix()} exists, aborting.")

        target_directory.mkdir(parents=True, exist_ok=True)

        # Process nation and region tables first - b/c they are small
        generate_chunk(scale_factor=0.01,
                       data_directory=target_directory,
                       work_directory=work_directory,
                       chunk_number=0,
                       num_chunks=1,
                       duckdb_threads=duckdb_threads,
                       per_thread_output=False,
                       compression_method=compression_method,
                       file_size_bytes=file_size_bytes,
                       table_list=TPCH_SMALL_TABLE_LIST
                       )

        # Process the chunks
        with multiprocessing.Pool(processes=num_processes) as pool:
            results = []
            for i in range(num_chunks):
                result = pool.apply_async(generate_chunk,
                                          kwds=dict(scale_factor=scale_factor,
                                                    data_directory=target_directory,
                                                    work_directory=work_directory,
                                                    chunk_number=i,
                                                    num_chunks=num_chunks,
                                                    duckdb_threads=duckdb_threads,
                                                    per_thread_output=per_thread_output,
                                                    compression_method=compression_method,
                                                    file_size_bytes=file_size_bytes,
                                                    table_list=TPCH_LARGE_TABLE_LIST
                                                    ),
                                          error_callback=error_callback
                                          )
                results.append(result)

            # Collect results (this will block until all jobs finish or one fails)
            try:
                for result in results:
                    result.get()

            except Exception as e:
                logger.error(msg=f"Job failure encountered: {str(e)}")
                pool.terminate()
                raise e

            else:
                logger.info(msg="All jobs completed successfully.")


@click.command()
@click.option(
    "--version/--no-version",
    type=bool,
    default=False,
    show_default=False,
    required=True,
    help="Prints the TPC-H Datagen package version and exits."
)
@click.option(
    "--scale-factor",
    type=int,
    required=False,
    help="The TPC-H Scale Factor to use for data generation.",
)
@click.option(
    "--data-directory",
    type=str,
    default=DATA_DIR.as_posix(),
    show_default=True,
    required=True,
    help="The target output data directory to put the files into"
)
@click.option(
    "--work-directory",
    type=str,
    default=WORK_DIR.as_posix(),
    show_default=True,
    required=True,
    help="The work directory to use for data generation."
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Can we overwrite the target directory if it already exists..."
)
@click.option(
    "--num-chunks",
    type=int,
    default=DEFAULT_NUM_CHUNKS,
    show_default=True,
    required=True,
    help="The number of chunks that will be generated - more chunks equals smaller memory requirements, but more files generated."
)
@click.option(
    "--num-processes",
    type=int,
    default=DEFAULT_NUM_PROCESSES,
    show_default=True,
    required=True,
    help="The maximum number of processes for the multi-processing pool to use for data generation."
)
@click.option(
    "--duckdb-threads",
    type=int,
    default=math.ceil(os.cpu_count() / DEFAULT_NUM_PROCESSES),
    show_default=True,
    required=True,
    help="The number of DuckDB threads to use for data generation (within each job process)."
)
@click.option(
    "--per-thread-output/--no-per-thread-output",
    type=bool,
    default=True,
    show_default=True,
    required=True,
    help="Controls whether to write the output to a single file or multiple files (for each process)."
)
@click.option(
    "--compression-method",
    type=click.Choice(["none", "snappy", "gzip", "zstd"]),
    default="zstd",
    show_default=True,
    required=True,
    help="The compression method to use for the parquet files generated."
)
@click.option(
    "--file-size-bytes",
    type=str,
    default="100m",
    show_default=True,
    required=True,
    help="The target file size for the parquet files generated."
)
def click_datagen(version: bool,
                  scale_factor: int,
                  data_directory: str,
                  work_directory: str,
                  overwrite: bool,
                  num_chunks: int,
                  num_processes: int,
                  duckdb_threads: int,
                  per_thread_output: bool,
                  compression_method: str,
                  file_size_bytes: str
                  ):
    datagen(**locals())


if __name__ == "__main__":
    click_datagen()
