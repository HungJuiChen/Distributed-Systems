import kagglehub

# Download latest version
path = kagglehub.dataset_download("jensenbaxter/10dataset-text-document-classification")

print("Path to dataset files:", path)