from setuptools import setup, find_packages

setup(
    name="OCFO-ETL",
    version="0.1",
    packages=find_packages(),  # Automatically detects your package folders
    install_requires=[
        "numpy",
        "pandas", 
        "scikit-learn", 
        "spacy", 
        "google-api-python-client", 
        "google-auth-httplib2", 
        "google-auth-oauthlib"
    ],
)