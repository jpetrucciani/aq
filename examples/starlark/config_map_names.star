load("lib/k8s.star", "kind_names")

def main(data):
    return kind_names(data, "ConfigMap")
