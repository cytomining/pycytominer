def parse_feature(feature):
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
        if feature_group in [
            "AreaShape",
            "Neighbors",
            "Children",
            "Parent",
            "Number",
            "Location",
        ]:
            feature_type = parts[2]
            channel = (
                "None"
                if feature_group == "Location" and feature_type == "Center"
                else parts[4]
            )

        elif feature_group == "Count":
            feature_type = "None"
            channel = "None"

        elif feature_group == "Granularity":
            feature_type = "None"
            channel = parts[3]

        elif feature_group == "Intensity":
            feature_type = parts[2]
            channel = parts[3]

        elif feature_group == "Correlation":
            feature_type = parts[2]
            channel = [parts[3], parts[4]]
            channel.sort()
            channel = "_".join(channel)

        elif feature_group in ["Texture", "RadialDistribution"]:
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
