if __name__ == "__main__":
    import argparse
    from pycytominer.cyto_utils.collate import collate

    parser = argparse.ArgumentParser(description="Collate CSVs")
    parser.add_argument("batch", help="Batch name to process")
    parser.add_argument("config", help="Config file to pass to cytominer-database")
    parser.add_argument("plate", help="Plate name to process")
    parser.add_argument(
        "--base",
        "--base-directory",
        dest="base_directory",
        default="../..",
        help="Base directory where the CSV files will be located",
    )
    parser.add_argument(
        "--column",
        default=None,
        help="An existing column to be explicitly copied to a Metadata_Plate column if Metadata_Plate was not set",
    )
    parser.add_argument(
        "--munge",
        action="store_true",
        default=False,
        help="Whether munge should be passed to cytominer-database, if True will break a single object CSV down by objects",
    )
    parser.add_argument(
        "--pipeline", default="analysis", help="A string used in path creation"
    )
    parser.add_argument(
        "--remote",
        default=None,
        help="A remote AWS directory, if set CSV files will be synced down from at the beginning and to which SQLite files will be synced up at the end of the run",
    )
    parser.add_argument(
        "--aggregate-only",
        dest="aggregate_only",
        action="store_true",
        default=False,
        help="Whether to perform only the aggregation of existant SQLite files and bypass previous collation steps",
    )
    parser.add_argument(
        "--temp",
        default="/tmp",
        help="The temporary directory to be used by cytominer-databases for output",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Whether or not to overwrite an sqlite that exists in the temporary directory if it already exists",
    )
    parser.add_argument(
        "--dont-add-image-features",
        dest="add_image_features",
        action="store_false",
        default=True,
        help="Whether or not to add the image features to the profiles",
    )
    parser.add_argument(
        "--image-feature-categories",
        dest="image_feature_categories",
        type=lambda s: [item for item in s.split(",")],
        default="Granularity,Texture,ImageQuality,Count,Threshold",
        help="Which image feature categories should be added if adding image features to the aggregated profiles. Multiple values can be passed in if comma separated with no spaces between them",
    )
    parser.add_argument(
        "--dont-print",
        dest="printtoscreen",
        action="store_false",
        default=True,
        help="Whether to print status updates",
    )

    args = parser.parse_args()

    collate(
        args.batch,
        args.config,
        args.plate,
        base_directory=args.base_directory,
        column=args.column,
        munge=args.munge,
        pipeline=args.pipeline,
        remote=args.remote,
        aggregate_only=args.aggregate_only,
        temp=args.temp,
        overwrite=args.overwrite,
        add_image_features=args.add_image_features,
        image_feature_categories=args.image_feature_categories,
        printtoscreen=args.printtoscreen,
    )
