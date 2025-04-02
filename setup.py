from setuptools import setup, find_packages

setup(
    name="mlops-etl-pipeline",
    version="0.1.0",
    description="ETL Pipeline for Sales Data Processing",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        "apache-airflow",
        "pandas",
        "psycopg2-binary",
        "pymysql",
        "sqlalchemy",
        "python-dotenv",
        "numpy",
    ],
    python_requires=">=3.8",
) 