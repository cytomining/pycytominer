import os
import subprocess
import sys


def run_check_errors(cmd):
    """Run a system command, and exit if an error occurred, otherwise continue"""
    if type(cmd)==str:
        cmd = cmd.split()
    output = subprocess.run(cmd,capture_output=True,text=True)
    if output.stderr != '':
        print_cmd = ' '.join(map(str,cmd))
        sys.exit(f"The error {output.stderr} was generated when running {print_cmd}. Exiting.") 
    return

def collate(
    batch,
    config, 
    plate, 
    base_directory='../..', 
    column=None, 
    munge=False,
    pipeline='analysis',
    remote=None,
    temp='/tmp',
    overwrite=False
    ):
    """Collate the CellProfiler-created CSVs into a single SQLite file by calling cytominer-database

    Parameters
    ----------
    batch : str
        Batch name to process
    config : str
        config file to pass to cytominer-database
    plate : str
        Plate name to process
    base_directory : str, default "../.."
        Base directory where the CSV files will be located
    column : str, optional, default None
        An existing column to be explicitly copied to a Metadata_Plate column if Metadata_Plate was not set
    munge : bool, default False
        Whether munge should be passed to cytominer-database, if True will break a single object CSV down by objects
    pipeline : str, default 'analysis'
        A string used in path creation
    remote : str, optional, default None
        A remote AWS directory, if set CSV files will be synced down from at the beginning and to which SQLite files will be synced up at the end of the run
    tmp: str, default '/tmp'
        The temporary directory to be used by cytominer-databases for output
    overwrite: bool, optional, default False
        Whether or not to overwrite an sqlite that exists in the temporary directory if it already exists
    """

    #Set up directories (these need to be abspaths to keep from confusing makedirs later)
    input_dir = os.path.abspath(os.path.join(base_directory, 'analysis', batch, plate, pipeline))
    backend_dir = os.path.abspath(os.path.join(base_directory, 'backend', batch, plate))
    cache_backend_dir = os.path.abspath(os.path.join(temp, 'backend', batch, plate))

    backend_file = os.path.join(backend_dir, plate+'.sqlite')
    cache_backend_file = os.path.join(cache_backend_dir, plate+'.sqlite')

    if os.path.exists(cache_backend_file): 
        if not overwrite:
            print(f"An SQLite file for {plate} already exists at {cache_backend_file} and overwrite is set to False. Terminating.")
            sys.exit(0)
        else:
            os.remove(cache_backend_file)

    for eachdir in [input_dir, backend_dir, cache_backend_dir]:
        if not os.path.exists(eachdir):
            os.makedirs(eachdir, exist_ok=True)

    if remote:
        
        remote_input_dir = os.path.join(remote, 'analysis', batch, plate, pipeline)
        remote_backend_file = os.path.join(remote, 'backend', batch, plate, plate+'.sqlite')

        sync_cmd = 'aws s3 sync --exclude "*" --include "*/Cells.csv" --include "*/Nuclei.csv" --include "*/Cytoplasm.csv" --include "*/Image.csv" ' + remote_input_dir + ' ' + input_dir

        print(f"Downloading CSVs from {remote_input_dir} to {input_dir}")
        run_check_errors(sync_cmd)

    ingest_cmd = ['cytominer-database', 'ingest', input_dir, 'sqlite:///'+cache_backend_file, '-c', config]
    if not munge:
        #munge is by default True in cytominer-database
        ingest_cmd.append('--no-munge')

    print(f"Ingesting {input_dir}")
    run_check_errors(ingest_cmd)
    
    if column:
        print(f"Adding a Metadata_Plate column based on column {column}")
        alter_cmd = ['sqlite3', cache_backend_file, "'ALTER TABLE Image ADD COLUMN Metadata_Plate TEXT;'"]
        run_check_errors(alter_cmd)
        update_cmd = ['sqlite3', cache_backend_file,"'UPDATE image SET Metadata_Plate ="+ column + ";'" ]
        run_check_errors(update_cmd)

    print(f"Indexing database {cache_backend_file}")
    index_cmd_1 = ['sqlite3', cache_backend_file, "'CREATE INDEX IF NOT EXISTS table_image_idx ON Image(TableNumber, ImageNumber);'"]
    run_check_errors(index_cmd_1)
    index_cmd_2 = ['sqlite3', cache_backend_file, "'CREATE INDEX IF NOT EXISTS table_image_object_cells_idx ON Cells(TableNumber, ImageNumber, ObjectNumber);'"]
    run_check_errors(index_cmd_2)
    index_cmd_3 = ['sqlite3', cache_backend_file, "'CREATE INDEX IF NOT EXISTS table_image_object_cytoplasm_idx ON Cytoplasm(TableNumber, ImageNumber, ObjectNumber);'"]
    run_check_errors(index_cmd_3)
    index_cmd_4 = ['sqlite3', cache_backend_file, "'CREATE INDEX IF NOT EXISTS table_image_object_nuclei_idx ON Nuclei(TableNumber, ImageNumber, ObjectNumber);'"]
    run_check_errors(index_cmd_4)
    index_cmd_5 = ['sqlite3', cache_backend_file, "'CREATE INDEX IF NOT EXISTS plate_well_image_idx ON Image(Metadata_Plate, Metadata_Well);'"]
    run_check_errors(index_cmd_5)

    if remote:

        print(f"Uploading {cache_backend_file} to {remote_backend_file}")
        cp_cmd = ['aws', 's3', 'cp', cache_backend_file, remote_backend_file]
        run_check_errors(cp_cmd)

        print(f"Removing temporary files from {input_dir} and {cache_backend_dir}")
        import shutil
        shutil.rmtree(input_dir)
        shutil.rmtree(cache_backend_dir)

    else:
        print(f"Renaming {cache_backend_file} to {backend_file}")
        os.rename(cache_backend_file,backend_file)

if __name__ =='__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Collate CSVs')
    parser.add_argument('batch', help='Batch name to process')
    parser.add_argument('config', help='config file to pass to cytominer-database')
    parser.add_argument('plate', help='Plate name to process')
    parser.add_argument('--base','--base-directory', dest='base_directory',default='../..',help='Base directory where the CSV files will be located')
    parser.add_argument('--column', default=None,help='An existing column to be explicitly copied to a Metadata_Plate column if Metadata_Plate was not set')
    parser.add_argument('--munge', action='store_true', default=False,help='Whether munge should be passed to cytominer-database, if True will break a single object CSV down by objects')
    parser.add_argument('--pipeline', default='analysis',help='A string used in path creation')
    parser.add_argument('--remote', default=None,help='A remote AWS directory, if set CSV files will be synced down from at the beginning and to which SQLite files will be synced up at the end of the run')
    parser.add_argument('--temp', default='/tmp',help='The temporary directory to be used by cytominer-databases for output')
    parser.add_argument('--overwrite', action='store_true', default=False,help='Whether or not to overwrite an sqlite that exists in the temporary directory if it already exists')
    
    args = parser.parse_args()
    
    collate(args.batch, args.config, args.plate, base_directory=args.base_directory, column=args.column, munge=args.munge, pipeline=args.pipeline, remote=args.remote, temp=args.temp, overwrite=args.overwrite)