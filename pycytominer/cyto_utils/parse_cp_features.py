def parse_cp_features(
    feature: str, channels: list = ["DNA", "RNA", "AGP", "Mito", "ER", "mito_tubeness"]
):
    """Parses a CellProfiler feature string into its semantic components.

    This function will take a feature string and return a dictionary containing its semantic components,
    specifically: the compartment, feature group, feature type, and channel.
    If the feature string is not in a recognized format, the function will assign 'Unknown' to the non-comprehensible components.
    Channel information will be returned as 'None' where it's not applicable.

    Parameters
    ----------
    feature : str
        The CellProfiler feature string to parse.

    channels : list, optional
        A list of channel names to use when parsing the feature string. The default is ['DNA', 'RNA', 'AGP', 'Mito', 'ER'].

    Returns
    -------
    dict
        A dictionary with the following keys: 'feature', 'compartment', 'feature_group', 'feature_type', 'channel'.
        Each key maps to the respective component of the feature string.

    Raises
    ------
    ValueError
        Raised if the input is not a string.
    """

    if not isinstance(feature, str):
        raise ValueError(f"Expected a string, got {type(feature).__name__}")

    if not isinstance(channels, list):
        raise ValueError(f"Expected a list, got {type(channels).__name__}")

    def channel_standardizer(channel):
        channel = channel.replace("Orig", "")
        return channel

    unique_token = "XUNIQUEX"
    tokenized_feature = feature
    for channel in channels:
        tokenized_channel = channel.replace("_", unique_token)
        tokenized_feature = tokenized_feature.replace(channel, tokenized_channel)

    parts = tokenized_feature.split("_")

    feature_group = parts[1]
    if parts[0] not in ["Cells", "Cytoplasm", "Nuclei", "Image"]:
        compartment = "XUNKNOWN"
        feature_group = "XUNKNOWN"
        feature_type = "XUNKNOWN"
        channel = "XUNKNOWN"
    else:
        compartment = parts[0]
        feature_group = parts[1]
        feature_type = "XNONE"  # default value
        channel = "XNONE"  # default value

        if feature_group in [
            "AreaShape",
            "Neighbors",
            "Children",
            "Parent",
            "Number",
            "Threshold",
            "ObjectSkeleton",
        ]:
            # Examples:
            # Cells,AreaShape,Zernike_2_0
            # Cells,AreaShape,BoundingBoxArea
            # Cells,Neighbors,AngleBetweenNeighbors_Adjacent
            # Nuclei,Children,Cytoplasm_Count
            # Nuclei,Parent,NucleiIncludingEdges
            # Nuclei,Number,ObjectNumber
            # Image,Threshold,SumOfEntropies_NucleiIncludingEdges
            # Nuclei,ObjectSkeleton,NumberTrunks_mito_skel

            feature_type = parts[2]

        elif feature_group == "Location":
            # Examples:
            # Cells,Location_CenterMassIntensity_X_DNA
            # Cells,Location_Center_X

            feature_type = parts[2]
            if feature_type != "Center":
                channel = parts[4]

        elif feature_group == "Count":
            # Examples:
            # Cells,Count,Cells
            pass

        elif feature_group == "Granularity":
            # Examples:
            # Cells,Granularity,15_ER
            channel = parts[3]

        elif feature_group in ["Intensity", "ImageQuality"]:
            # Examples:
            # Cells,Intensity,MeanIntensity_DNA
            # Image,ImageQuality,MaxIntensity_OrigAGP
            feature_type = parts[2]
            channel = parts[3]

        elif feature_group == "Correlation":
            # Examples:
            # Cells,Correlation,Correlation_DNA_ER
            feature_type = parts[2]
            channel = [parts[3], parts[4]]
            channel.sort()
            channel = "_".join(channel)

        elif feature_group in ["Texture", "RadialDistribution"]:
            # Examples:
            # Cells,Texture,SumEntropy_ER_3_01_256
            # Cells,RadialDistribution,FracAtD_mito_tubeness_2of16
            feature_type = parts[2]
            channel = parts[3]

        else:
            feature_type = "XUNKNOWN"
            channel = "XUNKNOWN"

    channel = "_".join(list(map(channel_standardizer, channel.split("_"))))

    channel = channel.replace(unique_token, "_")

    return {
        "feature": feature,
        "compartment": compartment,
        "feature_group": feature_group,
        "feature_type": feature_type,
        "channel": channel,
    }
