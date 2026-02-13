"""Command Line Interface (CLI) for Pycytominer operations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

import fire

from pycytominer import aggregate, annotate, consensus, feature_select, normalize
from pycytominer.cyto_utils.load import load_profiles


class PycytominerCLIError(RuntimeError):
    """Raised when CLI wrapper assumptions are violated."""


def _split_csv_arg(value: str | Sequence[str]) -> list[str]:
    """Split a comma-delimited string or sequence into a list.

    Args:
        value: Comma-delimited string or sequence of strings.

    Returns:
        List of strings with whitespace trimmed and empty values removed.
    """
    items = value.split(",") if isinstance(value, str) else list(value)

    return [item.strip() for item in items if item and str(item).strip()]


def _parse_list_or_str(value: str | None) -> str | list[str] | None:
    """Convert a comma-delimited string into a list when needed.

    Args:
        value: Optional string input.

    Returns:
        A list of strings if the input contains commas, otherwise the original string.
    """
    if value is None:
        return None
    if "," in value:
        return _split_csv_arg(value)
    return value


class PycytominerCLI:
    """Command Line Interface for Pycytominer operations."""

    def aggregate(
        self,
        profiles: str,
        output_file: str,
        strata: str | Sequence[str] = "Metadata_Plate,Metadata_Well",
        features: str | Sequence[str] = "infer",
        operation: str = "median",
        output_type: Literal["csv", "parquet", "anndata_h5ad", "anndata_zarr"]
        | None = "csv",
        compute_object_count: bool = False,
        object_feature: str = "Metadata_ObjectNumber",
        subset_data_file: str | None = None,
        compression_options: str | dict[str, Any] | None = None,
        float_format: str | None = None,
    ) -> str:
        """Aggregate profiles from a file and write the results to disk.

        Args:
            profiles: Path to the input profiles file.
            output_file: Path to the output file to write.
            strata: Metadata columns to aggregate by.
            features: Feature list or "infer" to infer CellProfiler features.
            operation: Aggregation operation ("median" or "mean").
            output_type: Output type to write.
            compute_object_count: Whether to compute object counts.
            object_feature: Column used for object counting.
            subset_data_file: Optional path to a subset dataframe for filtering.
            compression_options: Compression options for writing output.
            float_format: Decimal precision for output formatting.

        Returns:
            The output file path.
        """
        profiles_df = load_profiles(profiles)
        strata_list = _split_csv_arg(strata)
        if isinstance(features, str) and features == "infer":
            features_value: str | list[str] = "infer"
        else:
            features_value = _split_csv_arg(features)
        subset_df = load_profiles(subset_data_file) if subset_data_file else None

        result = aggregate(
            population_df=profiles_df,
            strata=strata_list,
            features=features_value,
            operation=operation,
            output_file=output_file,
            output_type=output_type,
            compute_object_count=compute_object_count,
            object_feature=object_feature,
            subset_data_df=subset_df,
            compression_options=compression_options,
            float_format=float_format,
        )
        if isinstance(result, str):
            return result
        raise PycytominerCLIError(
            "aggregate() returned a DataFrame when a file path was expected."
        )

    def annotate(
        self,
        profiles: str,
        platemap: str,
        output_file: str,
        join_on: str | Sequence[str] = "Metadata_well_position,Metadata_Well",
        output_type: Literal["csv", "parquet", "anndata_h5ad", "anndata_zarr"]
        | None = "csv",
        add_metadata_id_to_platemap: bool = True,
        format_broad_cmap: bool = False,
        clean_cellprofiler: bool = True,
        external_metadata: str | None = None,
        external_join_left: str | None = None,
        external_join_right: str | None = None,
        compression_options: str | dict[str, str] | None = None,
        float_format: str | None = None,
    ) -> str:
        """Annotate profiles using a platemap file and write output.

        Args:
            profiles: Path to the input profiles file.
            platemap: Path to the platemap file.
            output_file: Path to the output file to write.
            join_on: Join keys (platemap, profiles).
            output_type: Output type to write.
            add_metadata_id_to_platemap: Whether to prefix platemap columns.
            format_broad_cmap: Whether to format Broad CMAP metadata.
            clean_cellprofiler: Whether to clean CellProfiler feature names.
            external_metadata: Optional external metadata file path.
            external_join_left: Join column in profiles metadata.
            external_join_right: Join column in external metadata.
            compression_options: Compression options for writing output.
            float_format: Decimal precision for output formatting.

        Returns:
            The output file path.
        """
        join_on_values = _split_csv_arg(join_on)
        if len(join_on_values) != 2:
            raise ValueError("join_on must contain exactly two values.")

        result = annotate(
            profiles=profiles,
            platemap=platemap,
            join_on=join_on_values,
            output_file=output_file,
            output_type=output_type,
            add_metadata_id_to_platemap=add_metadata_id_to_platemap,
            format_broad_cmap=format_broad_cmap,
            clean_cellprofiler=clean_cellprofiler,
            external_metadata=external_metadata,
            external_join_left=external_join_left,
            external_join_right=external_join_right,
            compression_options=compression_options,
            float_format=float_format,
        )
        if isinstance(result, str):
            return result
        raise PycytominerCLIError(
            "annotate() returned a DataFrame when a file path was expected."
        )

    def normalize(
        self,
        profiles: str,
        output_file: str,
        features: str | Sequence[str] = "infer",
        image_features: bool = False,
        meta_features: str | Sequence[str] = "infer",
        samples: str = "all",
        method: str = "standardize",
        output_type: Literal["csv", "parquet", "anndata_h5ad", "anndata_zarr"]
        | None = "csv",
        compression_options: str | dict[str, Any] | None = None,
        float_format: str | None = None,
        mad_robustize_epsilon: float | None = 1e-18,
        spherize_center: bool = True,
        spherize_method: str = "ZCA-cor",
        spherize_epsilon: float = 1e-6,
    ) -> str:
        """Normalize profiles from a file and write the results to disk.

        Args:
            profiles: Path to the input profiles file.
            output_file: Path to the output file to write.
            features: Feature list or "infer" to infer CellProfiler features.
            image_features: Whether profiles include image features.
            meta_features: Metadata list or "infer" for metadata inference.
            samples: Query string to choose normalization samples.
            method: Normalization method.
            output_type: Output type to write.
            compression_options: Compression options for writing output.
            float_format: Decimal precision for output formatting.
            mad_robustize_epsilon: Robust MAD epsilon parameter.
            spherize_center: Whether to center data before sphering.
            spherize_method: Spherize method to use.
            spherize_epsilon: Spherize epsilon parameter.

        Returns:
            The output file path.
        """
        if isinstance(features, str) and features == "infer":
            features_value: str | list[str] = "infer"
        else:
            features_value = _split_csv_arg(features)
        if isinstance(meta_features, str) and meta_features == "infer":
            meta_features_value: str | list[str] = "infer"
        else:
            meta_features_value = _split_csv_arg(meta_features)

        result = normalize(
            profiles=profiles,
            features=features_value,
            image_features=image_features,
            meta_features=meta_features_value,
            samples=samples,
            method=method,
            output_file=output_file,
            output_type=output_type,
            compression_options=compression_options,
            float_format=float_format,
            mad_robustize_epsilon=mad_robustize_epsilon,
            spherize_center=spherize_center,
            spherize_method=spherize_method,
            spherize_epsilon=spherize_epsilon,
        )
        if isinstance(result, str):
            return result
        raise PycytominerCLIError(
            "normalize() returned a DataFrame when a file path was expected."
        )

    def feature_select(
        self,
        profiles: str,
        output_file: str,
        features: str | Sequence[str] = "infer",
        image_features: bool = False,
        samples: str = "all",
        operation: str | Sequence[str] = "variance_threshold",
        output_type: Literal["csv", "parquet", "anndata_h5ad", "anndata_zarr"]
        | None = "csv",
        na_cutoff: float = 0.05,
        corr_threshold: float = 0.9,
        corr_method: str = "pearson",
        freq_cut: float = 0.05,
        unique_cut: float = 0.01,
        compression_options: str | dict[str, Any] | None = None,
        float_format: str | None = None,
        blocklist_file: str | None = None,
        outlier_cutoff: float = 500.0,
        noise_removal_perturb_groups: str | None = None,
        noise_removal_stdev_cutoff: float | None = None,
    ) -> str:
        """Select features from profiles and write the results to disk.

        Args:
            profiles: Path to the input profiles file.
            output_file: Path to the output file to write.
            features: Feature list or "infer" to infer CellProfiler features.
            image_features: Whether profiles include image features.
            samples: Query string to choose selection samples.
            operation: Operation(s) to apply, comma-delimited for multiple.
            output_type: Output type to write.
            na_cutoff: Missing value cutoff for dropping columns.
            corr_threshold: Correlation threshold for dropping columns.
            corr_method: Correlation method.
            freq_cut: Frequency cutoff for variance thresholding.
            unique_cut: Unique value cutoff for variance thresholding.
            compression_options: Compression options for writing output.
            float_format: Decimal precision for output formatting.
            blocklist_file: Optional blocklist file path.
            outlier_cutoff: Outlier cutoff for feature removal.
            noise_removal_perturb_groups: Metadata column or list for noise removal.
            noise_removal_stdev_cutoff: Standard deviation cutoff for noise removal.

        Returns:
            The output file path.
        """
        if isinstance(features, str) and features == "infer":
            features_value: str | list[str] = "infer"
        else:
            features_value = _split_csv_arg(features)

        if isinstance(operation, str) and "," in operation:
            operation_value: str | list[str] = _split_csv_arg(operation)
        elif isinstance(operation, str):
            operation_value = operation
        else:
            operation_value = list(operation)

        noise_removal_groups_value = _parse_list_or_str(noise_removal_perturb_groups)

        result = feature_select(
            profiles=profiles,
            features=features_value,
            image_features=image_features,
            samples=samples,
            operation=operation_value,
            output_file=output_file,
            output_type=output_type,
            na_cutoff=na_cutoff,
            corr_threshold=corr_threshold,
            corr_method=corr_method,
            freq_cut=freq_cut,
            unique_cut=unique_cut,
            compression_options=compression_options,
            float_format=float_format,
            blocklist_file=blocklist_file,
            outlier_cutoff=outlier_cutoff,
            noise_removal_perturb_groups=noise_removal_groups_value,
            noise_removal_stdev_cutoff=noise_removal_stdev_cutoff,
        )
        if isinstance(result, str):
            return result
        raise PycytominerCLIError(
            "feature_select() returned a DataFrame when a file path was expected."
        )

    def consensus(
        self,
        profiles: str,
        output_file: str,
        replicate_columns: str | Sequence[str] = "Metadata_Plate,Metadata_Well",
        operation: str = "median",
        features: str | Sequence[str] = "infer",
        output_type: Literal["csv", "parquet", "anndata_h5ad", "anndata_zarr"]
        | None = "csv",
        compression_options: str | dict[str, Any] | None = None,
        float_format: str | None = None,
        modz_method: str = "spearman",
        modz_min_weight: float = 0.01,
        modz_precision: int = 4,
    ) -> str:
        """Create consensus profiles from a file and write output.

        Args:
            profiles: Path to the input profiles file.
            output_file: Path to the output file to write.
            replicate_columns: Metadata columns to aggregate by.
            operation: Consensus operation ("median", "mean", or "modz").
            features: Feature list or "infer" to infer CellProfiler features.
            output_type: Output type to write.
            compression_options: Compression options for writing output.
            float_format: Decimal precision for output formatting.
            modz_method: MODZ correlation method.
            modz_min_weight: MODZ minimum weight.
            modz_precision: MODZ precision.

        Returns:
            The output file path.
        """
        replicate_columns_list = _split_csv_arg(replicate_columns)
        if isinstance(features, str) and features == "infer":
            features_value: str | list[str] = "infer"
        else:
            features_value = _split_csv_arg(features)

        modz_args: dict[str, int | float | str] | None = None
        if operation == "modz":
            modz_args = {
                "method": modz_method,
                "min_weight": modz_min_weight,
                "precision": modz_precision,
            }

        result = consensus(
            profiles=profiles,
            replicate_columns=replicate_columns_list,
            operation=operation,
            features=features_value,
            output_file=output_file,
            output_type=output_type,
            compression_options=compression_options,
            float_format=float_format,
            modz_args=modz_args,
        )
        if isinstance(result, str):
            return result
        raise PycytominerCLIError(
            "consensus() returned a DataFrame when a file path was expected."
        )


def main() -> None:
    """Run the Pycytominer CLI."""
    fire.Fire(PycytominerCLI)


if __name__ == "__main__":
    main()
