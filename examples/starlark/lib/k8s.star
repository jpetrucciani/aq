def kind_names(resources, kind):
    return sorted(
        [resource["metadata"]["name"] for resource in resources if resource["kind"] == kind],
    )
