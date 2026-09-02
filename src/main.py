from src.analysis.analyse import run_analysis
from src.ingestion.run_ingestion import run_ingestion


def main():
    run_ingestion()
    run_analysis()


if __name__ == "__main__":
    main()
