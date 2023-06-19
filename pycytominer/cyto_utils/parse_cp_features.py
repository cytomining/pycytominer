def parse_cp_features(feature):
    """
    Parse a feature string into a dictionary with the compartment, feature group, feature type, and channel.

    Args:
        feature (str): The CellProfiler feature string to parse.

    Returns:
        dict: A dictionary with the compartment, feature group, feature type, and channel.

    Raises:
        ValueError: If the input is not a string.
    """

    if not isinstance(feature, str):
        raise ValueError(f"Expected a string, got {type(feature).__name__}")

    parts = feature.split("_")
    compartment = parts[0]
    feature_group = parts[1]

    # Default values for unrecognized types
    feature_type = "Unknown"
    channel = "Unknown"

    if compartment not in ["Cells", "Cytoplasm", "Nuclei", "Image"]:
        compartment = "Unknown"
        feature_group = "Unknown"
    else:
        if feature_group in ["AreaShape", "Neighbors", "Children", "Parent", "Number"]:
            # Examples:
            # Cells,AreaShape,Zernike_2_0
            # Cells,AreaShape,BoundingBoxArea
            # Cells,Neighbors,AngleBetweenNeighbors_Adjacent
            # Nuclei,Children,Cytoplasm_Count
            # Nuclei,Parent,NucleiIncludingEdges
            # Nuclei,Number,ObjectNumber

            feature_type = parts[2]
            channel = "None"

        elif feature_group == "Location":
            # Examples:
            # Cells,Location_CenterMassIntensity_X_DNA
            # Cells,Location_Center_X

            feature_type = parts[2]
            if feature_type == "Center":
                channel = "None"
            else:
                channel = parts[4]

        elif feature_group == "Count":
            # Examples:
            # Cells,Count,Cells
            feature_type = "None"
            channel = "None"

        elif feature_group == "Granularity":
            # Examples:
            # Cells,Granularity,15_ER
            feature_type = "None"
            channel = parts[3]

        elif feature_group == "Intensity":
            # Examples:
            # Cells,Intensity,MeanIntensity_DNA
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

    channel = channel.upper()

    return {
        "feature": feature,
        "compartment": compartment,
        "feature_group": feature_group,
        "feature_type": feature_type,
        "channel": channel,
    }
